package discovery_test

import (
	"context"
	"fmt"
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
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// announceCollector subscribes directly to NATS (not via EventBus) to avoid
// durable consumer name conflicts with the announcer's own subscription.
type announceCollector struct {
	mu        sync.Mutex
	announces []*protocolv1.CapabilityAnnouncePayload
	sub       *nats.Subscription
}

func newAnnounceCollector(t *testing.T, nc *nats.Conn) *announceCollector {
	t.Helper()
	c := &announceCollector{}
	sub, err := nc.Subscribe("agent.capability.announce", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		var payload protocolv1.CapabilityAnnouncePayload
		if err := proto.Unmarshal(pbEvent.Payload, &payload); err != nil {
			return
		}
		c.mu.Lock()
		c.announces = append(c.announces, &payload)
		c.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe announce: %v", err)
	}
	c.sub = sub
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return c
}

func (c *announceCollector) waitFor(count int, timeout time.Duration) []*protocolv1.CapabilityAnnouncePayload {
	deadline := time.After(timeout)
	for {
		c.mu.Lock()
		n := len(c.announces)
		c.mu.Unlock()
		if n >= count {
			break
		}
		select {
		case <-deadline:
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.announces
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.announces
}

func (c *announceCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.announces)
}

func setupAnnouncerTest(t *testing.T) (eventbus.EventBus, *registry.KVRegistry, *nats.Conn) {
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

	handler := registry.NewHandler(bus, reg, "test-node")
	subs, err := handler.Start(ctx)
	if err != nil {
		t.Fatalf("start handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})

	announcer := discovery.NewCapabilityAnnouncer(bus, reg, "test-node")
	announcerSubs, err := announcer.Start(ctx)
	if err != nil {
		t.Fatalf("start announcer: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range announcerSubs {
			_ = s.Unsubscribe()
		}
	})

	time.Sleep(200 * time.Millisecond)

	return bus, reg, nc
}

func publishRegister(t *testing.T, bus eventbus.EventBus, agentID, agentType string, caps []*protocolv1.Capability) {
	t.Helper()
	payload := &protocolv1.AgentRegisterPayload{
		AgentId:      agentID,
		AgentType:    agentType,
		Version:      "1.0.0",
		Capabilities: caps,
	}
	data, _ := proto.Marshal(payload)

	err := bus.Publish(context.Background(), &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.register",
		SourceNode:  "test-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish register: %v", err)
	}
}

func publishUnregister(t *testing.T, bus eventbus.EventBus, agentID string) {
	t.Helper()
	payload := &protocolv1.AgentUnregisterPayload{
		AgentId: agentID,
		Reason:  "test",
	}
	data, _ := proto.Marshal(payload)

	err := bus.Publish(context.Background(), &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.unregister",
		SourceNode:  "test-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish unregister: %v", err)
	}
}

func TestAnnouncer_AgentWithCapabilities(t *testing.T) {
	bus, _, nc := setupAnnouncerTest(t)
	collector := newAnnounceCollector(t, nc)

	publishRegister(t, bus, "announce-agent-1", "test", []*protocolv1.Capability{
		{Name: "risk-estimation", Version: "1.0.0"},
	})

	announces := collector.waitFor(1, 3*time.Second)
	if len(announces) == 0 {
		t.Fatal("expected at least 1 announce event, got 0")
	}

	found := false
	for _, a := range announces {
		if a.AgentId == "announce-agent-1" && len(a.Capabilities) > 0 {
			found = true
			if a.Capabilities[0].Name != "risk-estimation" {
				t.Errorf("got capability %q, want risk-estimation", a.Capabilities[0].Name)
			}
		}
	}
	if !found {
		t.Error("announce for announce-agent-1 not found")
	}
}

func TestAnnouncer_AgentWithNoCapabilities(t *testing.T) {
	_, _, nc := setupAnnouncerTest(t)
	collector := newAnnounceCollector(t, nc)

	// No registrations — no announces expected.
	time.Sleep(500 * time.Millisecond)

	if collector.count() != 0 {
		t.Errorf("expected 0 announce events, got %d", collector.count())
	}
}

func TestAnnouncer_ReRegistrationWithUpdatedCapabilities(t *testing.T) {
	bus, _, nc := setupAnnouncerTest(t)
	collector := newAnnounceCollector(t, nc)

	publishRegister(t, bus, "re-reg-agent", "test", []*protocolv1.Capability{
		{Name: "cap-v1", Version: "1.0.0"},
	})

	announces := collector.waitFor(1, 3*time.Second)
	if len(announces) == 0 {
		t.Fatal("expected announce after first registration")
	}

	// Re-register with updated capabilities.
	publishRegister(t, bus, "re-reg-agent", "test", []*protocolv1.Capability{
		{Name: "cap-v2", Version: "2.0.0"},
	})

	announces = collector.waitFor(2, 3*time.Second)

	var foundV2 bool
	for _, a := range announces {
		if a.AgentId == "re-reg-agent" {
			for _, c := range a.Capabilities {
				if c.Name == "cap-v2" {
					foundV2 = true
				}
			}
		}
	}
	if !foundV2 {
		t.Error("expected announce with updated capability cap-v2")
	}
}

// TestAnnouncer_UnregisterDeannounce tests that unregistering an agent publishes
// an announce with empty capabilities (Phase 6 / US4, but tested here since
// the announcer handles both).
func TestAnnouncer_UnregisterDeannounce(t *testing.T) {
	bus, _, nc := setupAnnouncerTest(t)
	collector := newAnnounceCollector(t, nc)

	publishRegister(t, bus, "unreg-agent", "test", []*protocolv1.Capability{
		{Name: "detect", Version: "1.0.0"},
	})

	collector.waitFor(1, 3*time.Second)

	publishUnregister(t, bus, "unreg-agent")

	announces := collector.waitFor(2, 3*time.Second)

	var foundDeannounce bool
	for _, a := range announces {
		if a.AgentId == "unreg-agent" && len(a.Capabilities) == 0 {
			foundDeannounce = true
		}
	}
	if !foundDeannounce {
		t.Error("expected deannounce (empty capabilities) after unregister")
	}
}

// TestAnnouncer_DeadAgentDeannounce tests that an AGENT_DEAD error triggers
// a deannounce event.
func TestAnnouncer_DeadAgentDeannounce(t *testing.T) {
	bus, _, nc := setupAnnouncerTest(t)
	collector := newAnnounceCollector(t, nc)

	publishRegister(t, bus, "dead-agent", "test", []*protocolv1.Capability{
		{Name: "risk", Version: "1.0.0"},
	})

	collector.waitFor(1, 3*time.Second)

	// Simulate AGENT_DEAD error.
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:   "dead-agent",
		ErrorCode: "AGENT_DEAD",
		Message:   fmt.Sprintf("agent dead-agent declared dead"),
	}
	data, _ := proto.Marshal(errPayload)

	err := bus.Publish(context.Background(), &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  "test-node",
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish agent.error: %v", err)
	}

	announces := collector.waitFor(2, 3*time.Second)

	var foundDeannounce bool
	for _, a := range announces {
		if a.AgentId == "dead-agent" && len(a.Capabilities) == 0 {
			foundDeannounce = true
		}
	}
	if !foundDeannounce {
		t.Error("expected deannounce after AGENT_DEAD error")
	}
}

// TestAnnouncer_SubsequentDiscoveryExcludesDeannounced tests that after deannounce,
// the agent is not returned by FindByCapability.
func TestAnnouncer_SubsequentDiscoveryExcludesDeannounced(t *testing.T) {
	bus, reg, _ := setupAnnouncerTest(t)
	ctx := context.Background()

	publishRegister(t, bus, "disc-agent", "test", []*protocolv1.Capability{
		{Name: "plan", Version: "1.0.0"},
	})
	time.Sleep(500 * time.Millisecond)

	// Verify agent is discoverable.
	matches, err := reg.FindByCapability(ctx, "plan", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match before unregister, got %d", len(matches))
	}

	publishUnregister(t, bus, "disc-agent")
	time.Sleep(500 * time.Millisecond)

	// Verify agent is no longer discoverable.
	matches, err = reg.FindByCapability(ctx, "plan", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("expected 0 matches after unregister, got %d", len(matches))
	}
}

// Ensure nats.Conn and jetstream are used (avoid unused import errors).
var _ *nats.Conn
var _ jetstream.JetStream
