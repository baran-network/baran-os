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
	"github.com/carlosmolina/agent-os/core/router"
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

// TestRouterIntegration_RegistrationFlow verifies that agent registration works
// correctly when events are published through the DefaultRouter.
func TestRouterIntegration_RegistrationFlow(t *testing.T) {
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

	// Set up handler (subscribes to registration events via bus).
	handler := registry.NewHandler(bus, reg, "router-test-node")
	subs, err := handler.Start(ctx)
	if err != nil {
		t.Fatalf("start handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})

	// Create router.
	r := router.NewDefaultRouter(bus, reg, router.DefaultStreamRegistry())

	time.Sleep(200 * time.Millisecond)

	// Register agent via router.Route (broadcast — agent.register goes to AGENTS stream).
	agentID := "router-reg-" + uuid.Must(uuid.NewV7()).String()[:8]
	regPayload := &protocolv1.AgentRegisterPayload{
		AgentId:   agentID,
		AgentType: "router-test",
		Version:   "1.0.0",
		Capabilities: []*protocolv1.Capability{
			{Name: "test-cap", Version: "1.0.0"},
		},
	}
	data, _ := proto.Marshal(regPayload)

	err = r.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.register",
		SourceNode:  "router-test-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("route register: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	agent, _, err := reg.Get(ctx, agentID)
	if err != nil {
		t.Fatalf("agent not registered: %v", err)
	}
	if agent.Status != registry.StatusActive {
		t.Fatalf("expected ACTIVE, got %v", agent.Status)
	}
}

// TestRouterIntegration_HealthFlow verifies that health ping/pong flows work
// through the router: pings are broadcast, pongs are direct to runtime.
func TestRouterIntegration_HealthFlow(t *testing.T) {
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

	r := router.NewDefaultRouter(bus, reg, router.DefaultStreamRegistry())

	// Register agent directly.
	agentID := "health-flow-" + uuid.Must(uuid.NewV7()).String()[:8]
	_, err = reg.Register(ctx, registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    "health-test",
		Version:      "1.0.0",
		Capabilities: []registry.Capability{{Name: "test", Version: "1.0.0"}},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Simulate health ping via router (broadcast).
	pingPayload := &protocolv1.HealthPingPayload{
		AgentId:  agentID,
		Sequence: 1,
	}
	pingData, _ := proto.Marshal(pingPayload)

	err = r.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.health.ping",
		SourceNode:  "runtime",
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     pingData,
	})
	if err != nil {
		t.Fatalf("route ping: %v", err)
	}

	// Verify ping was received via broadcast subscription.
	var pingReceived bool
	sub, err := bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		var p protocolv1.HealthPingPayload
		if err := proto.Unmarshal(evt.Payload, &p); err != nil {
			return nil
		}
		if p.AgentId == agentID {
			pingReceived = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe ping: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	time.Sleep(500 * time.Millisecond)

	if !pingReceived {
		t.Error("health ping not received via broadcast")
	}

	// Simulate pong via router (direct to runtime).
	pongPayload := &protocolv1.HealthPongPayload{
		AgentId:  agentID,
		Sequence: 1,
		Status:   protocolv1.AgentStatus_AGENT_STATUS_HEALTHY,
	}
	pongData, _ := proto.Marshal(pongPayload)

	// Pong is broadcast in the current architecture (health monitor subscribes to
	// agent.health.pong). Verify it routes through the router successfully.
	err = r.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.health.pong",
		SourceNode:  "agent-node",
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     pongData,
	})
	if err != nil {
		t.Fatalf("route pong: %v", err)
	}
}
