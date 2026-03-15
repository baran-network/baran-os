package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/discovery"
	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/health"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/router"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Runtime is the top-level orchestrator that owns the lifecycle of all
// subsystems. It starts an embedded NATS server, wires all components,
// and handles graceful shutdown.
type Runtime struct {
	config Config
	nodeID string
	logger *slog.Logger

	natsServer *natsserver.Server
	natsConn   *nats.Conn

	bus            eventbus.EventBus
	registry       registry.AgentRegistry
	router         *router.DefaultRouter
	healthMonitor  *health.Monitor
	workflowEngine *workflow.WorkflowEngine
	announcer      *discovery.CapabilityAnnouncer
	discoveryH     *discovery.DiscoveryHandler
	registryH      *registry.Handler

	subscriptions   []eventbus.Subscription
	httpServer      *http.Server
	healthAddr      string
	startedAt       time.Time
	subsystemStatus map[string]string
	mu              sync.RWMutex
}

// New creates a new Runtime with the given configuration. It generates a
// UUID v7 node ID and configures structured logging.
func New(cfg Config) *Runtime {
	level := parseSlogLevel(cfg.LogLevel)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	return &Runtime{
		config:          cfg,
		nodeID:          uuid.Must(uuid.NewV7()).String(),
		logger:          logger,
		subsystemStatus: make(map[string]string),
	}
}

// Run starts all subsystems and blocks until the context is cancelled.
// On context cancellation it performs graceful shutdown.
func (r *Runtime) Run(ctx context.Context) error {
	log := r.logger.With("component", "runtime")

	r.startHealthHTTP()

	if err := r.startNATS(ctx); err != nil {
		return fmt.Errorf("start NATS: %w", err)
	}

	if err := r.startSubsystems(ctx); err != nil {
		return fmt.Errorf("start subsystems: %w", err)
	}

	r.startedAt = time.Now()

	natsURL := r.natsServer.ClientURL()
	log.Info("runtime ready", "node_id", r.nodeID, "nats_url", natsURL)

	<-ctx.Done()
	log.Info("shutdown signal received", "signal", "context cancelled")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), r.config.ShutdownGrace)
	defer cancel()

	r.shutdown(shutdownCtx)
	return nil
}

// NodeID returns the runtime's unique node identifier.
func (r *Runtime) NodeID() string { return r.nodeID }

// NATSURL returns the client connection URL of the embedded NATS server.
func (r *Runtime) NATSURL() string {
	if r.natsServer == nil {
		return ""
	}
	return r.natsServer.ClientURL()
}

func (r *Runtime) startHealthHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", r.healthHandler)

	r.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", r.config.HealthPort),
		Handler: mux,
	}

	ln, err := net.Listen("tcp", r.httpServer.Addr)
	if err != nil {
		r.logger.With("component", "http").Error("failed to listen", "error", err)
		return
	}
	r.healthAddr = ln.Addr().String()
	r.logger.With("component", "http").Info("health endpoint started", "addr", r.healthAddr)

	go func() {
		if err := r.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			r.logger.With("component", "http").Error("http server error", "error", err)
		}
	}()
}

// HealthAddr returns the address the health HTTP server is listening on.
func (r *Runtime) HealthAddr() string { return r.healthAddr }

func (r *Runtime) startNATS(ctx context.Context) error {
	log := r.logger.With("component", "nats")

	opts := &natsserver.Options{
		Host:         "127.0.0.1",
		Port:         r.config.NATSPort,
		JetStream:    true,
		StoreDir:     r.config.NATSStoreDir,
		NoSigs: true,
		NoLog:  true, // we set a custom logger below
	}

	s, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	debugFlag := parseSlogLevel(r.config.LogLevel) <= slog.LevelDebug
	s.SetLogger(newNATSLogger(r.logger), debugFlag, false)
	s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		s.Shutdown()
		return fmt.Errorf("server not ready within 5s")
	}
	r.natsServer = s
	r.setSubsystemStatus("nats", "up")
	log.Info("subsystem started", "port", opts.Port)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		return fmt.Errorf("connect client: %w", err)
	}
	r.natsConn = nc

	return nil
}

func (r *Runtime) startSubsystems(ctx context.Context) error {
	log := r.logger.With("component", "runtime")

	// EventBus
	bus, err := natseventbus.NewFromConn(ctx, r.natsConn)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("eventbus: %w", err)
	}
	r.bus = bus
	r.setSubsystemStatus("eventbus", "up")
	log.Info("subsystem started", "component", "eventbus")

	// Agent Registry
	reg, err := registry.NewKVRegistry(ctx, r.natsConn, r.config.UnhealthyThreshold, r.config.DeadThreshold)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("registry: %w", err)
	}
	r.registry = reg
	r.setSubsystemStatus("registry", "up")
	log.Info("subsystem started", "component", "registry")

	// Event Router
	streams := router.DefaultStreamRegistry()
	rtr := router.NewDefaultRouter(r.bus, r.registry, streams)
	r.router = rtr
	r.setSubsystemStatus("router", "up")
	log.Info("subsystem started", "component", "router")

	// Registry Handler
	regHandler := registry.NewHandler(r.bus, r.registry, r.nodeID)
	regSubs, err := regHandler.Start(ctx)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("registry handler: %w", err)
	}
	r.registryH = regHandler
	r.subscriptions = append(r.subscriptions, regSubs...)

	// Workflow State Store + Engine
	store, err := workflow.NewKVWorkflowStateStore(ctx, r.natsConn)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("workflow state store: %w", err)
	}
	engine := workflow.NewWorkflowEngine(r.bus, store, r.registry, r.nodeID, r.config.WorkflowTimeout)
	wfSubs, err := engine.Start(ctx)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("workflow engine: %w", err)
	}
	r.workflowEngine = engine
	r.subscriptions = append(r.subscriptions, wfSubs...)
	r.setSubsystemStatus("workflow_engine", "up")
	log.Info("subsystem started", "component", "workflow")

	// Health Monitor
	healthCfg := health.Config{
		HeartbeatInterval:  r.config.HeartbeatInterval,
		UnhealthyThreshold: r.config.UnhealthyThreshold,
		DeadThreshold:      r.config.DeadThreshold,
	}
	mon := health.New(r.bus, r.registry, r.nodeID, healthCfg)
	monSub, err := mon.Start(ctx)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("health monitor: %w", err)
	}
	r.healthMonitor = mon
	r.subscriptions = append(r.subscriptions, monSub)
	r.setSubsystemStatus("health_monitor", "up")
	log.Info("subsystem started", "component", "health")

	// Capability Announcer
	ann := discovery.NewCapabilityAnnouncer(r.bus, r.registry, r.nodeID)
	annSubs, err := ann.Start(ctx)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("capability announcer: %w", err)
	}
	r.announcer = ann
	r.subscriptions = append(r.subscriptions, annSubs...)

	// Discovery Handler
	dh := discovery.NewDiscoveryHandler(r.bus, r.registry, r.nodeID)
	dhSubs, err := dh.Start(ctx)
	if err != nil {
		r.shutdown(ctx)
		return fmt.Errorf("discovery handler: %w", err)
	}
	r.discoveryH = dh
	r.subscriptions = append(r.subscriptions, dhSubs...)
	r.setSubsystemStatus("discovery", "up")
	log.Info("subsystem started", "component", "discovery")

	return nil
}

func (r *Runtime) shutdown(ctx context.Context) {
	log := r.logger.With("component", "runtime")

	// HTTP server first (stop accepting new health checks)
	if r.httpServer != nil {
		log.Info("stopping subsystem", "component", "http")
		if err := r.httpServer.Shutdown(ctx); err != nil {
			log.Error("http server shutdown error", "component", "http", "error", err)
		}
	}

	// Unsubscribe all event subscriptions
	for _, sub := range r.subscriptions {
		_ = sub.Unsubscribe()
	}
	r.subscriptions = nil

	// Health monitor
	if r.healthMonitor != nil {
		log.Info("stopping subsystem", "component", "health")
		r.healthMonitor.Stop()
	}

	// Router
	if r.router != nil {
		log.Info("stopping subsystem", "component", "router")
		_ = r.router.Close()
	}

	// EventBus (drains NATS connection)
	if r.bus != nil {
		log.Info("stopping subsystem", "component", "eventbus")
		_ = r.bus.Close()
	}

	// NATS client connection
	if r.natsConn != nil && !r.natsConn.IsClosed() {
		r.natsConn.Close()
	}

	// Embedded NATS server
	if r.natsServer != nil {
		log.Info("stopping subsystem", "component", "nats")
		r.natsServer.Shutdown()
		r.natsServer.WaitForShutdown()
	}

	log.Info("runtime stopped")
}

func (r *Runtime) setSubsystemStatus(name, status string) {
	r.mu.Lock()
	r.subsystemStatus[name] = status
	r.mu.Unlock()
}

func parseSlogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
