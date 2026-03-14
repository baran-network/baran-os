package discovery_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/carlosmolina/agent-os/core/discovery"
	"github.com/carlosmolina/agent-os/core/eventbus"
	natseventbus "github.com/carlosmolina/agent-os/core/eventbus/nats"
	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/carlosmolina/agent-os/core/testutil"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

func setupHandlerTest(t *testing.T) (eventbus.EventBus, *registry.KVRegistry, *nats.Conn) {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	handler := discovery.NewDiscoveryHandler(bus, reg, "test-node")
	subs, err := handler.Start(ctx)
	if err != nil {
		t.Fatalf("start handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})

	time.Sleep(200 * time.Millisecond)

	return bus, reg, nc
}

// responseCollector subscribes to agent.discovery.response via core NATS.
type responseCollector struct {
	mu        sync.Mutex
	responses []*protocolv1.DiscoveryResponsePayload
	eventIDs  []string // correlation IDs
}

func newResponseCollector(t *testing.T, nc *nats.Conn) *responseCollector {
	t.Helper()
	c := &responseCollector{}
	sub, err := nc.Subscribe("agent.discovery.response", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		var payload protocolv1.DiscoveryResponsePayload
		if err := proto.Unmarshal(pbEvent.Payload, &payload); err != nil {
			return
		}
		c.mu.Lock()
		c.responses = append(c.responses, &payload)
		c.eventIDs = append(c.eventIDs, pbEvent.CorrelationId)
		c.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe response: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return c
}

func (c *responseCollector) waitFor(count int, timeout time.Duration) []*protocolv1.DiscoveryResponsePayload {
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		n := len(c.responses)
		c.mu.Unlock()
		if n >= count {
			break
		}
		select {
		case <-deadline:
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.responses
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.responses
}

func (c *responseCollector) correlationIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string{}, c.eventIDs...)
}

// errorCollector subscribes to agent.error via core NATS.
type errorCollector struct {
	mu     sync.Mutex
	errors []*protocolv1.AgentErrorPayload
}

func newErrorCollector(t *testing.T, nc *nats.Conn) *errorCollector {
	t.Helper()
	c := &errorCollector{}
	sub, err := nc.Subscribe("agent.error", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		var payload protocolv1.AgentErrorPayload
		if err := proto.Unmarshal(pbEvent.Payload, &payload); err != nil {
			return
		}
		c.mu.Lock()
		c.errors = append(c.errors, &payload)
		c.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return c
}

func (c *errorCollector) waitFor(count int, timeout time.Duration) []*protocolv1.AgentErrorPayload {
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		n := len(c.errors)
		c.mu.Unlock()
		if n >= count {
			break
		}
		select {
		case <-deadline:
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.errors
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors
}

func publishDiscoveryRequest(t *testing.T, bus eventbus.EventBus, capName, versionConstraint, correlationID string) {
	t.Helper()
	payload := &protocolv1.DiscoveryRequestPayload{
		CapabilityName:    capName,
		VersionConstraint: versionConstraint,
	}
	data, _ := proto.Marshal(payload)

	err := bus.Publish(context.Background(), &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "agent.discovery.request",
		SourceNode:    "test-node",
		SourceAgent:   "requesting-agent",
		CorrelationID: correlationID,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
	if err != nil {
		t.Fatalf("publish discovery request: %v", err)
	}
}

func registerAgentDirect(t *testing.T, reg *registry.KVRegistry, agentID, agentType string, caps []registry.Capability) {
	t.Helper()
	_, err := reg.Register(context.Background(), registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    agentType,
		Version:      "1.0.0",
		Capabilities: caps,
		NodeID:       "test-node",
	})
	if err != nil {
		t.Fatalf("register %s: %v", agentID, err)
	}
}

func TestDiscoveryHandler_TwoMatchingAgents(t *testing.T) {
	bus, reg, nc := setupHandlerTest(t)
	collector := newResponseCollector(t, nc)

	registerAgentDirect(t, reg, "agent-A", "estimator", []registry.Capability{
		{Name: "risk-estimation", Version: "1.0.0"},
	})
	registerAgentDirect(t, reg, "agent-B", "estimator", []registry.Capability{
		{Name: "risk-estimation", Version: "1.2.0"},
	})
	registerAgentDirect(t, reg, "agent-C", "planner", []registry.Capability{
		{Name: "evacuation", Version: "1.0.0"},
	})

	publishDiscoveryRequest(t, bus, "risk-estimation", "", "corr-123")

	responses := collector.waitFor(1, 3*time.Second)
	if len(responses) == 0 {
		t.Fatal("expected discovery response, got none")
	}

	resp := responses[0]
	if len(resp.Matches) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(resp.Matches))
	}

	ids := map[string]bool{}
	for _, m := range resp.Matches {
		ids[m.AgentId] = true
	}
	if !ids["agent-A"] || !ids["agent-B"] {
		t.Errorf("expected agents A and B in response, got %v", ids)
	}
}

func TestDiscoveryHandler_NoMatchingAgents(t *testing.T) {
	bus, reg, nc := setupHandlerTest(t)
	collector := newResponseCollector(t, nc)

	registerAgentDirect(t, reg, "agent-only", "test", []registry.Capability{
		{Name: "unrelated", Version: "1.0.0"},
	})

	publishDiscoveryRequest(t, bus, "nonexistent-cap", "", "corr-empty")

	responses := collector.waitFor(1, 3*time.Second)
	if len(responses) == 0 {
		t.Fatal("expected discovery response (empty matches), got none")
	}

	if len(responses[0].Matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(responses[0].Matches))
	}
}

func TestDiscoveryHandler_VersionConstraint(t *testing.T) {
	bus, reg, nc := setupHandlerTest(t)
	collector := newResponseCollector(t, nc)

	registerAgentDirect(t, reg, "agent-v1", "test", []registry.Capability{
		{Name: "risk-estimation", Version: "1.0.0"},
	})
	registerAgentDirect(t, reg, "agent-v2", "test", []registry.Capability{
		{Name: "risk-estimation", Version: "2.0.0"},
	})

	publishDiscoveryRequest(t, bus, "risk-estimation", "1.x", "corr-version")

	responses := collector.waitFor(1, 3*time.Second)
	if len(responses) == 0 {
		t.Fatal("expected discovery response, got none")
	}

	if len(responses[0].Matches) != 1 {
		t.Fatalf("expected 1 match with version constraint, got %d", len(responses[0].Matches))
	}
	if responses[0].Matches[0].AgentId != "agent-v1" {
		t.Errorf("expected agent-v1, got %s", responses[0].Matches[0].AgentId)
	}
}

func TestDiscoveryHandler_EmptyCapabilityName(t *testing.T) {
	bus, _, nc := setupHandlerTest(t)
	errCollector := newErrorCollector(t, nc)

	publishDiscoveryRequest(t, bus, "", "", "corr-invalid")

	errors := errCollector.waitFor(1, 3*time.Second)
	if len(errors) == 0 {
		t.Fatal("expected agent.error for empty capability_name, got none")
	}

	found := false
	for _, e := range errors {
		if e.ErrorCode == "INVALID_DISCOVERY_REQUEST" {
			found = true
		}
	}
	if !found {
		t.Error("expected error with code INVALID_DISCOVERY_REQUEST")
	}
}

func TestDiscoveryHandler_CorrelationIDPropagated(t *testing.T) {
	bus, reg, nc := setupHandlerTest(t)
	collector := newResponseCollector(t, nc)

	registerAgentDirect(t, reg, "agent-corr", "test", []registry.Capability{
		{Name: "detect", Version: "1.0.0"},
	})

	corrID := uuid.Must(uuid.NewV7()).String()
	publishDiscoveryRequest(t, bus, "detect", "", corrID)

	collector.waitFor(1, 3*time.Second)

	corrIDs := collector.correlationIDs()
	if len(corrIDs) == 0 {
		t.Fatal("expected response with correlation ID")
	}

	found := false
	for _, id := range corrIDs {
		if id == corrID {
			found = true
		}
	}
	if !found {
		t.Errorf("correlation_id not propagated, got %v, want %s", corrIDs, corrID)
	}
}
