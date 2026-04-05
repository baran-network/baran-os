package a2a

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/baran-network/baran-os/core/eventbus"
	natsBus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/taxonomy"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/nats-io/nats.go"
)

// Server is the A2A gateway HTTP server.
type Server struct {
	cfg       *GatewayConfig
	logger    *slog.Logger
	mux       *http.ServeMux
	discovery *DiscoveryHandler
	jsonrpc   *JSONRPCHandler
	extMgr    *ExternalAgentManager
}

// NewServer creates and wires the A2A gateway. It connects to NATS,
// initialises the registry, workflow state store, and task manager.
func NewServer(cfg *GatewayConfig, logger *slog.Logger) (*Server, error) {
	ctx := context.Background()

	bus, err := natsBus.New(ctx, cfg.NATSUrl)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS at %s: %w", cfg.NATSUrl, err)
	}

	nc, err := nats.Connect(cfg.NATSUrl)
	if err != nil {
		return nil, fmt.Errorf("raw NATS connect: %w", err)
	}

	cat := taxonomy.NewStandardCatalog()

	reg, err := registry.NewKVRegistryWithCatalog(ctx, nc, 3, 6, cat)
	if err != nil {
		return nil, fmt.Errorf("create registry: %w", err)
	}

	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		return nil, fmt.Errorf("create workflow state store: %w", err)
	}

	return NewServerWithDeps(cfg, bus, reg, store, cat, logger), nil
}

// NewServerWithDeps constructs a Server with injected dependencies (for testing).
// cat may be nil; if nil, the ExternalAgentManager will skip catalog validation.
func NewServerWithDeps(cfg *GatewayConfig, bus eventbus.EventBus, reg registry.AgentRegistry, store workflow.WorkflowStateStore, cat taxonomy.Catalog, logger *slog.Logger) *Server {
	tasks := NewTaskManager(bus, reg, store, logger)
	discovery := NewDiscoveryHandler(reg, cfg, logger)
	jsonrpc := NewJSONRPCHandler(tasks, logger)
	extMgr := NewExternalAgentManager(reg, cat, logger)

	s := &Server{
		cfg:       cfg,
		logger:    logger,
		mux:       http.NewServeMux(),
		discovery: discovery,
		jsonrpc:   jsonrpc,
		extMgr:    extMgr,
	}
	s.registerRoutes()
	return s
}

// ExternalAgentManager returns the manager used to onboard and monitor external
// A2A agents. The caller (e.g. main) uses it to call OnboardExternalAgent and
// StartHealthPolling for each entry in GatewayConfig.ExternalAgents.
func (s *Server) ExternalAgentManager() *ExternalAgentManager {
	return s.extMgr
}

// Handler returns the HTTP handler for use in tests.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) registerRoutes() {
	authed := func(h http.HandlerFunc) http.Handler {
		return authMiddleware(s.cfg.PSK, h)
	}

	// Discovery endpoint (no auth required per A2A spec).
	s.mux.HandleFunc("GET /.well-known/agent-card.json", s.discovery.HandleDiscovery)

	// JSON-RPC endpoint (requires auth).
	s.mux.Handle("POST /", authed(s.jsonrpc.HandleJSONRPC))
}

// Run starts the HTTP server and blocks until SIGINT/SIGTERM or ctx cancellation.
func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.A2APort)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: s.mux,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("a2a gateway listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case sig := <-sigCh:
		s.logger.Info("signal received, shutting down", "signal", sig)
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGrace)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	s.logger.Info("a2a gateway stopped")
	return nil
}

// authMiddleware validates PSK authentication.
// If PSK is empty, all requests are allowed (development mode).
func authMiddleware(psk string, next http.Handler) http.Handler {
	if psk == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractToken(r)
		if token != psk {
			writeJSONRPC(w, nil, &JSONRPCError{Code: -32600, Message: "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return r.URL.Query().Get("token")
}
