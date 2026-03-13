package core_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/carlosmolina/agent-os/core/eventbus"
	natseventbus "github.com/carlosmolina/agent-os/core/eventbus/nats"
	"github.com/carlosmolina/agent-os/core/health"
	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/carlosmolina/agent-os/core/testutil"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// TestFullAgentLifecycle exercises the complete lifecycle:
// register → heartbeat pings → pong responses → stop responding →
// UNHEALTHY → DEAD → deregistered → re-register → ACTIVE.
func TestFullAgentLifecycle(t *testing.T) {
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	reg, err := registry.NewKVRegistry(ctx, nc, 2, 4)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	handler := registry.NewHandler(bus, reg, "e2e-node")
	subs, err := handler.Start(ctx)
	if err != nil {
		t.Fatalf("start handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})

	cfg := health.Config{
		HeartbeatInterval:  100 * time.Millisecond,
		UnhealthyThreshold: 2,
		DeadThreshold:      4,
	}
	monitor := health.New(bus, reg, "e2e-node", cfg)

	time.Sleep(200 * time.Millisecond)

	// --- Step 1: Register agent via event ---
	agentID := "e2e-agent-" + uuid.Must(uuid.NewV7()).String()[:8]
	regPayload := &protocolv1.AgentRegisterPayload{
		AgentId:   agentID,
		AgentType: "e2e-test",
		Version:   "1.0.0",
		Capabilities: []*protocolv1.Capability{
			{Name: "test-cap", Version: "1.0.0"},
		},
	}
	data, _ := proto.Marshal(regPayload)

	err = bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.register",
		SourceNode:  "e2e-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish register: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	agent, _, err := reg.Get(ctx, agentID)
	if err != nil {
		t.Fatalf("agent not registered: %v", err)
	}
	if agent.Status != registry.StatusActive {
		t.Fatalf("expected ACTIVE after register, got %v", agent.Status)
	}
	t.Log("Step 1 PASS: Agent registered and ACTIVE")

	// --- Step 2: Start monitor, agent responds to pings ---
	var respondToPings sync.Mutex
	responding := true

	_, err = bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		var ping protocolv1.HealthPingPayload
		if err := proto.Unmarshal(evt.Payload, &ping); err != nil {
			return nil
		}
		if ping.AgentId != agentID {
			return nil
		}

		respondToPings.Lock()
		shouldRespond := responding
		respondToPings.Unlock()

		if !shouldRespond {
			return nil
		}

		pong := &protocolv1.HealthPongPayload{
			AgentId:  agentID,
			Sequence: ping.Sequence,
			Status:   protocolv1.AgentStatus_AGENT_STATUS_HEALTHY,
		}
		pongData, _ := proto.Marshal(pong)

		return bus.Publish(ctx, &eventbus.Event{
			ID:          uuid.Must(uuid.NewV7()).String(),
			Type:        "agent.health.pong",
			SourceNode:  "e2e-node",
			SourceAgent: agentID,
			Timestamp:   time.Now().UnixNano(),
			Payload:     pongData,
		})
	})
	if err != nil {
		t.Fatalf("subscribe pings: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	monSub, err := monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}
	t.Cleanup(func() {
		monitor.Stop()
		_ = monSub.Unsubscribe()
	})

	// Let pings/pongs cycle for a bit.
	time.Sleep(500 * time.Millisecond)

	agent, _, err = reg.Get(ctx, agentID)
	if err != nil {
		t.Fatalf("get after pings: %v", err)
	}
	if agent.Status != registry.StatusActive {
		t.Fatalf("expected ACTIVE during heartbeats, got %v", agent.Status)
	}
	t.Log("Step 2 PASS: Agent responding to pings, stays ACTIVE")

	// --- Step 3: Stop responding → UNHEALTHY → DEAD → deregistered ---
	respondToPings.Lock()
	responding = false
	respondToPings.Unlock()

	// Wait long enough for dead threshold (4 missed at 100ms interval = ~400ms + buffer).
	time.Sleep(800 * time.Millisecond)

	_, _, err = reg.Get(ctx, agentID)
	if err != registry.ErrNotFound {
		t.Fatalf("expected agent deregistered after death, got err=%v", err)
	}
	t.Log("Step 3 PASS: Agent went DEAD and was deregistered")

	// --- Step 4: Re-register → ACTIVE again ---
	monitor.Stop()

	reRegPayload := &protocolv1.AgentRegisterPayload{
		AgentId:   agentID,
		AgentType: "e2e-test",
		Version:   "1.0.0",
		Capabilities: []*protocolv1.Capability{
			{Name: "test-cap", Version: "1.0.0"},
		},
	}
	data, _ = proto.Marshal(reRegPayload)

	err = bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.register",
		SourceNode:  "e2e-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	agent, _, err = reg.Get(ctx, agentID)
	if err != nil {
		t.Fatalf("agent not re-registered: %v", err)
	}
	if agent.Status != registry.StatusActive {
		t.Fatalf("expected ACTIVE after re-register, got %v", agent.Status)
	}
	t.Log("Step 4 PASS: Agent re-registered and ACTIVE")
}
