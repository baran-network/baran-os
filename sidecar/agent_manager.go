package sidecar

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/sdk"
)

// ManagedAgentState is the lifecycle state of a managed external agent.
type ManagedAgentState int

const (
	ManagedAgentRegistering  ManagedAgentState = iota // sdk.Agent.Start() in progress
	ManagedAgentActive                                // registered, healthy
	ManagedAgentUnreachable                           // all connections lost, no liveness
	ManagedAgentDeregistered                          // explicitly removed or declared dead
)

// SSEClient tracks an active SSE connection for one agent.
type SSEClient struct {
	ClientID    string
	AgentID     string
	Channel     chan sseEvent
	ConnectedAt time.Time
	LastEventID string
}

// sseEvent is a single event delivered over SSE.
type sseEvent struct {
	ID        string
	EventType string
	Data      []byte // JSON-encoded payload
}

// WSClient tracks an active WebSocket connection for one agent.
type WSClient struct {
	ClientID    string
	AgentID     string
	ConnectedAt time.Time
	LastEventID string
	send        chan wsMessage
	done        chan struct{}
	closeOnce   sync.Once
}

// wsMessage is a message sent over WebSocket.
type wsMessage struct {
	msgType string
	data    []byte
}

// ManagedAgent is the sidecar's internal representation of a proxied external agent.
type ManagedAgent struct {
	AgentID      string
	Name         string
	AgentType    string
	Version      string
	Capabilities []sdk.Capability
	Labels       map[string]string
	State        ManagedAgentState
	SDKAgent     *sdk.Agent
	CallbackURL  string
	RegisteredAt time.Time
	LastActivity time.Time

	mu         sync.RWMutex
	sseClients map[string]*SSEClient
	wsClients  map[string]*WSClient
	cancelFn   context.CancelFunc
}

// AddSSEClient registers a new SSE connection for this agent.
// Restores ACTIVE state if the agent was UNREACHABLE.
func (a *ManagedAgent) AddSSEClient(c *SSEClient) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sseClients[c.ClientID] = c
	if a.State == ManagedAgentUnreachable {
		a.State = ManagedAgentActive
	}
}

// RemoveSSEClient removes an SSE connection (called on disconnect).
func (a *ManagedAgent) RemoveSSEClient(clientID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.sseClients, clientID)
	a.checkLiveness()
}

// AddWSClient registers a new WebSocket connection for this agent.
// Restores ACTIVE state if the agent was UNREACHABLE.
func (a *ManagedAgent) AddWSClient(c *WSClient) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.wsClients[c.ClientID] = c
	if a.State == ManagedAgentUnreachable {
		a.State = ManagedAgentActive
	}
}

// RemoveWSClient removes a WebSocket connection.
func (a *ManagedAgent) RemoveWSClient(clientID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.wsClients, clientID)
	a.checkLiveness()
}

// FanOutSSE delivers an event to all active SSE clients for this agent.
// Events are dropped for slow clients (non-blocking send).
func (a *ManagedAgent) FanOutSSE(evt sseEvent) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, c := range a.sseClients {
		select {
		case c.Channel <- evt:
		default:
			// slow client — drop
		}
	}
}

// FanOutWS delivers an event to all active WebSocket clients for this agent.
func (a *ManagedAgent) FanOutWS(msg wsMessage) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, c := range a.wsClients {
		select {
		case c.send <- msg:
		default:
		}
	}
}

// HasConnections reports whether the agent has any active SSE or WS connections.
// Must be called with a.mu held (read or write).
func (a *ManagedAgent) HasConnections() bool {
	return len(a.sseClients) > 0 || len(a.wsClients) > 0
}

// checkLiveness transitions to UNREACHABLE if no connections remain.
// Must be called with a.mu held (write).
func (a *ManagedAgent) checkLiveness() {
	if a.State == ManagedAgentActive && !a.HasConnections() && a.CallbackURL == "" {
		a.State = ManagedAgentUnreachable
	}
}

// AgentManager manages all ManagedAgent instances for the sidecar.
type AgentManager struct {
	cfg      *SidecarConfig
	bus      eventbus.EventBus
	registry *PayloadRegistry
	logger   *slog.Logger

	mu     sync.RWMutex
	agents map[string]*ManagedAgent
}

// NewAgentManager creates an AgentManager.
func NewAgentManager(cfg *SidecarConfig, bus eventbus.EventBus, registry *PayloadRegistry, logger *slog.Logger) *AgentManager {
	return &AgentManager{
		cfg:      cfg,
		bus:      bus,
		registry: registry,
		logger:   logger,
		agents:   make(map[string]*ManagedAgent),
	}
}

// Register creates a new ManagedAgent, starts its SDK Agent, and registers it.
func (m *AgentManager) Register(ctx context.Context, req RegisterRequest) (*ManagedAgent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.agents[req.AgentID]; exists {
		return nil, ErrAgentAlreadyExists
	}
	if len(m.agents) >= m.cfg.MaxAgents {
		return nil, ErrMaxAgentsReached
	}

	caps := make([]sdk.Capability, len(req.Capabilities))
	for i, c := range req.Capabilities {
		caps[i] = sdk.Capability{
			Name:        c.Name,
			Version:     c.Version,
			Description: c.Description,
			Parameters:  c.Parameters,
		}
	}

	sdkOpts := []sdk.Option{
		sdk.WithEventBus(m.bus),
		sdk.WithLogger(m.logger.With("managed_agent", req.AgentID)),
	}
	if len(req.Labels) > 0 {
		sdkOpts = append(sdkOpts, sdk.WithLabels(req.Labels))
	}

	sdkAgent, err := sdk.NewWithID(req.AgentID, req.Name, req.AgentType, req.Version, sdkOpts...)
	if err != nil {
		return nil, err
	}

	// Register a no-op step handler for each capability — the actual handler
	// is the external agent via SSE/WS. We intercept the step delivery in the
	// event bridge (handler_subscribe.go).
	for _, cap := range caps {
		capCopy := cap
		sdkAgent.Handle(capCopy, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
			// Handled by external agent; bridge delivers via SSE/WS.
			return nil, nil
		})
	}

	agentCtx, cancelFn := context.WithCancel(ctx)
	if err := sdkAgent.Start(agentCtx); err != nil {
		cancelFn()
		return nil, err
	}

	ma := &ManagedAgent{
		AgentID:      req.AgentID,
		Name:         req.Name,
		AgentType:    req.AgentType,
		Version:      req.Version,
		Capabilities: caps,
		Labels:       req.Labels,
		State:        ManagedAgentActive,
		SDKAgent:     sdkAgent,
		CallbackURL:  req.CallbackURL,
		RegisteredAt: now(),
		LastActivity: now(),
		sseClients:   make(map[string]*SSEClient),
		wsClients:    make(map[string]*WSClient),
		cancelFn:     cancelFn,
	}

	m.agents[req.AgentID] = ma
	m.logger.Info("agent registered", "agent_id", req.AgentID, "name", req.Name)
	return ma, nil
}

// Deregister stops and removes a ManagedAgent.
func (m *AgentManager) Deregister(ctx context.Context, agentID string) error {
	m.mu.Lock()
	ma, exists := m.agents[agentID]
	if !exists {
		m.mu.Unlock()
		return ErrAgentNotFound
	}
	delete(m.agents, agentID)
	m.mu.Unlock()

	ma.mu.Lock()
	ma.State = ManagedAgentDeregistered
	// Close all SSE client channels to unblock SSE write loops.
	for _, c := range ma.sseClients {
		close(c.Channel)
	}
	ma.sseClients = make(map[string]*SSEClient)
	// Signal all WS clients to disconnect.
	for _, c := range ma.wsClients {
		c.closeOnce.Do(func() { close(c.done) })
	}
	ma.wsClients = make(map[string]*WSClient)
	ma.mu.Unlock()

	ma.cancelFn()
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = ma.SDKAgent.Stop(stopCtx)

	m.logger.Info("agent deregistered", "agent_id", agentID)
	return nil
}

// Get returns a ManagedAgent by ID, or nil if not found.
func (m *AgentManager) Get(agentID string) (*ManagedAgent, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ma, ok := m.agents[agentID]
	return ma, ok
}

// Count returns the number of managed agents.
func (m *AgentManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.agents)
}

// StopAll stops all managed agents (called during sidecar shutdown).
func (m *AgentManager) StopAll(ctx context.Context) {
	m.mu.Lock()
	agentIDs := make([]string, 0, len(m.agents))
	for id := range m.agents {
		agentIDs = append(agentIDs, id)
	}
	m.mu.Unlock()

	for _, id := range agentIDs {
		_ = m.Deregister(ctx, id)
	}
}
