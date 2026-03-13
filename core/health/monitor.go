package health

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/carlosmolina/agent-os/core/eventbus"
	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// Config holds the health monitor configuration.
type Config struct {
	HeartbeatInterval  time.Duration
	UnhealthyThreshold int32
	DeadThreshold      int32
}

// DefaultConfig returns the default health monitor configuration.
func DefaultConfig() Config {
	return Config{
		HeartbeatInterval:  10 * time.Second,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
}

// Monitor periodically pings registered agents and drives state machine
// transitions based on missed heartbeats.
type Monitor struct {
	bus      eventbus.EventBus
	registry registry.AgentRegistry
	config   Config
	nodeID   string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	sequence int64
}

// New creates a new health monitor.
func New(bus eventbus.EventBus, reg registry.AgentRegistry, nodeID string, cfg Config) *Monitor {
	return &Monitor{
		bus:      bus,
		registry: reg,
		config:   cfg,
		nodeID:   nodeID,
	}
}

// Start begins the ping loop and subscribes to pong responses.
func (m *Monitor) Start(ctx context.Context) (eventbus.Subscription, error) {
	mCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	sub, err := m.bus.Subscribe(mCtx, "agent.health.pong", m.handlePong)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("subscribe pong: %w", err)
	}

	m.wg.Add(1)
	go m.pingLoop(mCtx)

	return sub, nil
}

// Stop cancels the ping loop and waits for it to finish.
func (m *Monitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *Monitor) pingLoop(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pingRound(ctx)
		}
	}
}

func (m *Monitor) pingRound(ctx context.Context) {
	agents, err := m.registry.List(ctx)
	if err != nil {
		return
	}

	for _, agent := range agents {
		if agent.Status != registry.StatusActive && agent.Status != registry.StatusUnhealthy {
			continue
		}

		m.mu.Lock()
		m.sequence++
		seq := m.sequence
		m.mu.Unlock()

		// Send ping.
		pingPayload := &protocolv1.HealthPingPayload{
			AgentId:  agent.AgentID,
			Sequence: seq,
		}
		data, err := proto.Marshal(pingPayload)
		if err != nil {
			continue
		}

		_ = m.bus.Publish(ctx, &eventbus.Event{
			ID:          uuid.Must(uuid.NewV7()).String(),
			Type:        "agent.health.ping",
			SourceNode:  m.nodeID,
			SourceAgent: "runtime",
			TargetAgent: agent.AgentID,
			Timestamp:   time.Now().UnixNano(),
			Payload:     data,
		})

		// Increment missed heartbeats — the counter is incremented at ping time.
		status, rev, err := m.registry.IncrementMissedHeartbeats(ctx, agent.AgentID, agent.Revision)
		if err != nil {
			continue
		}

		if status == registry.StatusDead {
			m.handleDeadAgent(ctx, agent.AgentID, rev)
		}
	}
}

func (m *Monitor) handlePong(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.HealthPongPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal pong: %w", err)
	}

	// Retry loop for CAS conflicts (ping loop may update revision concurrently).
	const maxRetries = 5
	for i := range maxRetries {
		_, rev, err := m.registry.Get(ctx, payload.AgentId)
		if err != nil {
			return fmt.Errorf("get agent for pong: %w", err)
		}

		_, err = m.registry.RecordHeartbeat(ctx, payload.AgentId, rev)
		if err == nil {
			return nil
		}

		// CAS conflict — retry with backoff.
		if i < maxRetries-1 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return fmt.Errorf("failed to record heartbeat for %s after %d retries", payload.AgentId, maxRetries)
}

func (m *Monitor) handleDeadAgent(ctx context.Context, agentID string, rev uint64) {
	// Publish agent.error indicating death.
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:   agentID,
		ErrorCode: "AGENT_DEAD",
		Message:   fmt.Sprintf("agent %s missed %d heartbeats and is declared dead", agentID, m.config.DeadThreshold),
	}
	data, _ := proto.Marshal(errPayload)

	_ = m.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  m.nodeID,
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})

	// Deregister the dead agent.
	_ = m.registry.Deregister(ctx, agentID)
}
