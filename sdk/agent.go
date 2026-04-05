// Package sdk provides a high-level API for building Baran OS agents.
// It wraps the EventBus interface to handle connection, registration,
// capability announcement, health ping responses, workflow step handling,
// result publishing, idempotency deduplication, and graceful shutdown.
//
// A minimal agent can be built in under 20 lines:
//
//	agent, _ := sdk.New("my-agent", "example", "1.0.0")
//	agent.Handle(sdk.Capability{Name: "greet", Version: "1.0.0"}, handler)
//	agent.Run(context.Background())
//
// See specs/006-agent-sdk/quickstart.md for a complete example.
package sdk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natsBus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/google/uuid"
)

// AgentState represents the lifecycle state of an agent.
type AgentState int

const (
	AgentStateCreated  AgentState = iota // constructed, not yet connected
	AgentStateStarting                   // connecting and registering
	AgentStateRunning                    // registered, handling steps and health pings
	AgentStateStopping                   // shutting down, waiting for in-flight handlers
	AgentStateStopped                    // disconnected, terminal
)

// Capability describes a skill the agent offers.
type Capability struct {
	Name        string
	Version     string
	Description string
	Parameters  map[string]string
	// Taxonomy fields (Phase 9). Auto-populated by the runtime for standard capabilities.
	Category    string
	Action      string
	InputTypes  []string
	OutputTypes []string
}

// capabilityEntry pairs a Capability with its registered handler.
type capabilityEntry struct {
	cap     Capability
	handler StepHandler
}

// Agent is the top-level developer-facing object for building a Baran OS agent.
// Typical usage: one Agent per process.
type Agent struct {
	id          string
	name        string
	agentType   string
	version     string
	opts        options
	state       AgentState
	mu          sync.RWMutex
	capabilities map[string]capabilityEntry
	bus         eventbus.EventBus
	ownsBus     bool
	subs        []eventbus.Subscription
	inFlight    sync.WaitGroup
	busCtx      context.Context    // independent context for in-flight publish calls
	busCancel   context.CancelFunc // cancelled after in-flight handlers drain
	logger      *slog.Logger
	cache       *idempotencyCache
}

// New creates a new Agent with the given name, type, version, and options.
// It generates a UUID v7 agent ID automatically.
func New(name, agentType, version string, opts ...Option) (*Agent, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate agent ID: %w", err)
	}
	return NewWithID(id.String(), name, agentType, version, opts...)
}

// NewWithID creates a new Agent with an explicit agent ID.
// Use this when the caller needs to control the agent's identity (e.g. sidecar gateway).
func NewWithID(agentID, name, agentType, version string, opts ...Option) (*Agent, error) {
	if name == "" {
		return nil, errors.New("agent name must not be empty")
	}
	if agentID == "" {
		return nil, errors.New("agent ID must not be empty")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	logger := o.logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Agent{
		id:           agentID,
		name:         name,
		agentType:    agentType,
		version:      version,
		opts:         o,
		state:        AgentStateCreated,
		capabilities: make(map[string]capabilityEntry),
		logger:       logger.With("agent_id", agentID, "agent_name", name),
		cache:        newIdempotencyCache(o.idempotencyCacheSize),
	}, nil
}

// Handle registers a step handler for the given capability.
// Must be called before Start. Logs a warning if called after Start.
// Returns the agent for chaining.
func (a *Agent) Handle(cap Capability, handler StepHandler) *Agent {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.state >= AgentStateStarting {
		a.logger.Warn("Handle called after Start — capability will not be registered", "capability", cap.Name)
	}
	if _, exists := a.capabilities[cap.Name]; exists {
		a.logger.Warn("duplicate capability registered", "capability", cap.Name)
	}
	a.capabilities[cap.Name] = capabilityEntry{cap: cap, handler: handler}
	return a
}

// ID returns the agent's unique identifier (UUID v7).
func (a *Agent) ID() string {
	return a.id
}

// Start connects to the EventBus, registers the agent, and begins handling events.
// It is non-blocking — use Run for a blocking variant.
func (a *Agent) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.state != AgentStateCreated {
		a.mu.Unlock()
		return fmt.Errorf("agent already started (state=%d)", a.state)
	}
	if len(a.capabilities) == 0 {
		a.logger.Warn("WARNING: no capabilities registered")
	}
	a.state = AgentStateStarting
	a.mu.Unlock()

	// Create or use provided EventBus.
	var bus eventbus.EventBus
	if a.opts.eventBus != nil {
		bus = a.opts.eventBus
		a.ownsBus = false
	} else {
		b, err := natsBus.New(ctx, a.opts.natsURL)
		if err != nil {
			a.mu.Lock()
			a.state = AgentStateCreated
			a.mu.Unlock()
			return fmt.Errorf("connect to NATS at %s: %w", a.opts.natsURL, err)
		}
		bus = b
		a.ownsBus = true
	}

	a.mu.Lock()
	a.bus = bus
	a.mu.Unlock()

	// Register the agent via lifecycle event.
	if err := a.register(ctx); err != nil {
		if a.ownsBus {
			_ = bus.Close()
		}
		a.mu.Lock()
		a.state = AgentStateCreated
		a.mu.Unlock()
		return fmt.Errorf("register agent: %w", err)
	}

	// Set up subscriptions.
	if err := a.subscribeHealth(ctx); err != nil {
		if a.ownsBus {
			_ = bus.Close()
		}
		a.mu.Lock()
		a.state = AgentStateCreated
		a.mu.Unlock()
		return fmt.Errorf("subscribe health: %w", err)
	}

	if err := a.subscribeDirect(ctx); err != nil {
		if a.ownsBus {
			_ = bus.Close()
		}
		a.mu.Lock()
		a.state = AgentStateCreated
		a.mu.Unlock()
		return fmt.Errorf("subscribe direct: %w", err)
	}

	busCtx, busCancel := context.WithCancel(context.Background())

	a.mu.Lock()
	a.busCtx = busCtx
	a.busCancel = busCancel
	a.state = AgentStateRunning
	a.mu.Unlock()

	a.logger.Info("agent started", "type", a.agentType, "version", a.version)
	return nil
}

// Run calls Start, then blocks until ctx is cancelled or SIGINT/SIGTERM is received.
// It performs graceful shutdown before returning.
func (a *Agent) Run(ctx context.Context) error {
	if err := a.Start(ctx); err != nil {
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case <-ctx.Done():
	case sig := <-sigCh:
		a.logger.Info("received signal, shutting down", "signal", sig)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), a.opts.shutdownTimeout+5*time.Second)
	defer cancel()
	return a.Stop(stopCtx)
}

// Stop performs graceful shutdown: waits for in-flight handlers, unregisters,
// cancels subscriptions, and closes the EventBus.
func (a *Agent) Stop(ctx context.Context) error {
	a.mu.Lock()
	if a.state != AgentStateRunning {
		a.mu.Unlock()
		return nil
	}
	a.state = AgentStateStopping
	bus := a.bus
	subs := a.subs
	a.subs = nil
	a.mu.Unlock()

	a.logger.Info("stopping agent")

	// Cancel subscriptions first so NATS stops delivering new messages.
	// Combined with the state check in dispatchStep (which guards
	// inFlight.Add under RLock), this guarantees no new Add(1) calls
	// can race with Wait() below.
	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}

	// Wait for in-flight step handlers up to ShutdownTimeout.
	done := make(chan struct{})
	go func() {
		a.inFlight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(a.opts.shutdownTimeout):
		a.logger.Warn("shutdown timeout reached, forcing stop")
	}

	// In-flight handlers have drained (or timed out); cancel busCtx.
	if a.busCancel != nil {
		a.busCancel()
	}

	// Unregister from the runtime.
	if bus != nil {
		if err := a.unregister(ctx); err != nil {
			a.logger.Warn("unregister failed", "error", err)
		}

		if a.ownsBus {
			_ = bus.Close()
		}
	}

	a.mu.Lock()
	a.state = AgentStateStopped
	a.mu.Unlock()

	a.logger.Info("agent stopped")
	return nil
}
