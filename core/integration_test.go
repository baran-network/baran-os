package core_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ad-hok/agent-os/core/discovery"
	"github.com/ad-hok/agent-os/core/eventbus"
	natseventbus "github.com/ad-hok/agent-os/core/eventbus/nats"
	"github.com/ad-hok/agent-os/core/health"
	"github.com/ad-hok/agent-os/core/registry"
	"github.com/ad-hok/agent-os/core/router"
	"github.com/ad-hok/agent-os/core/testutil"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/ad-hok/agent-os/protocol/gen/go/agentosprotocol/v1"
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
	var pingReceived atomic.Bool
	sub, err := bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		var p protocolv1.HealthPingPayload
		if err := proto.Unmarshal(evt.Payload, &p); err != nil {
			return nil
		}
		if p.AgentId == agentID {
			pingReceived.Store(true)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe ping: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	time.Sleep(500 * time.Millisecond)

	if !pingReceived.Load() {
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

// announceWatcher subscribes to agent.capability.announce via core NATS to avoid
// consumer name conflicts with the announcer's JetStream consumer.
type announceWatcher struct {
	mu        sync.Mutex
	announces []*protocolv1.CapabilityAnnouncePayload
}

func newAnnounceWatcher(t *testing.T, nc *nats.Conn) *announceWatcher {
	t.Helper()
	w := &announceWatcher{}
	sub, err := nc.Subscribe("agent.capability.announce", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		var payload protocolv1.CapabilityAnnouncePayload
		if err := proto.Unmarshal(pbEvent.Payload, &payload); err != nil {
			return
		}
		w.mu.Lock()
		w.announces = append(w.announces, &payload)
		w.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe announce: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return w
}

func (w *announceWatcher) waitFor(count int, timeout time.Duration) []*protocolv1.CapabilityAnnouncePayload {
	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		n := len(w.announces)
		w.mu.Unlock()
		if n >= count {
			break
		}
		select {
		case <-deadline:
			w.mu.Lock()
			defer w.mu.Unlock()
			return w.announces
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.announces
}

// responseWatcher subscribes to agent.discovery.response via core NATS.
type responseWatcher struct {
	mu        sync.Mutex
	responses []*protocolv1.DiscoveryResponsePayload
	corrIDs   []string
}

func newResponseWatcher(t *testing.T, nc *nats.Conn) *responseWatcher {
	t.Helper()
	w := &responseWatcher{}
	sub, err := nc.Subscribe("agent.discovery.response", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		var payload protocolv1.DiscoveryResponsePayload
		if err := proto.Unmarshal(pbEvent.Payload, &payload); err != nil {
			return
		}
		w.mu.Lock()
		w.responses = append(w.responses, &payload)
		w.corrIDs = append(w.corrIDs, pbEvent.CorrelationId)
		w.mu.Unlock()
	})
	if err != nil {
		t.Fatalf("subscribe response: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	return w
}

func (w *responseWatcher) waitFor(count int, timeout time.Duration) []*protocolv1.DiscoveryResponsePayload {
	deadline := time.After(timeout)
	for {
		w.mu.Lock()
		n := len(w.responses)
		w.mu.Unlock()
		if n >= count {
			break
		}
		select {
		case <-deadline:
			w.mu.Lock()
			defer w.mu.Unlock()
			return w.responses
		case <-time.After(50 * time.Millisecond):
		}
	}
	time.Sleep(100 * time.Millisecond)
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.responses
}

// TestDiscoveryFlow_EndToEnd exercises the full discovery cycle:
// start EventBus + registry handler + discovery handler + capability announcer →
// register agents A, B (risk-estimation), C (evacuation) → verify announce events →
// send discovery request for risk-estimation → verify response →
// unregister A → verify deannounce → send discovery request again → verify only B returned.
func TestDiscoveryFlow_EndToEnd(t *testing.T) {
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

	// Start registry handler.
	regHandler := registry.NewHandler(bus, reg, "e2e-node")
	regSubs, err := regHandler.Start(ctx)
	if err != nil {
		t.Fatalf("start reg handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range regSubs {
			_ = s.Unsubscribe()
		}
	})

	// Start discovery handler.
	discHandler := discovery.NewDiscoveryHandler(bus, reg, "e2e-node")
	discSubs, err := discHandler.Start(ctx)
	if err != nil {
		t.Fatalf("start disc handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range discSubs {
			_ = s.Unsubscribe()
		}
	})

	// Start capability announcer.
	announcer := discovery.NewCapabilityAnnouncer(bus, reg, "e2e-node")
	annSubs, err := announcer.Start(ctx)
	if err != nil {
		t.Fatalf("start announcer: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range annSubs {
			_ = s.Unsubscribe()
		}
	})

	// Set up watchers via core NATS.
	annWatcher := newAnnounceWatcher(t, nc)
	respWatcher := newResponseWatcher(t, nc)

	time.Sleep(300 * time.Millisecond)

	// --- Step 1: Register agents A, B (risk-estimation), C (evacuation) ---
	agents := []struct {
		id, agentType, capName, capVersion string
	}{
		{"e2e-agent-A", "estimator", "risk-estimation", "1.0.0"},
		{"e2e-agent-B", "estimator", "risk-estimation", "1.2.0"},
		{"e2e-agent-C", "planner", "evacuation", "1.0.0"},
	}

	for _, a := range agents {
		regPayload := &protocolv1.AgentRegisterPayload{
			AgentId:   a.id,
			AgentType: a.agentType,
			Version:   "1.0.0",
			Capabilities: []*protocolv1.Capability{
				{Name: a.capName, Version: a.capVersion},
			},
		}
		data, _ := proto.Marshal(regPayload)

		err := bus.Publish(ctx, &eventbus.Event{
			ID:          uuid.Must(uuid.NewV7()).String(),
			Type:        "agent.register",
			SourceNode:  "e2e-node",
			SourceAgent: a.id,
			Timestamp:   time.Now().UnixNano(),
			Payload:     data,
		})
		if err != nil {
			t.Fatalf("register %s: %v", a.id, err)
		}
	}

	// --- Step 2: Verify announce events for all 3 agents ---
	announces := annWatcher.waitFor(3, 5*time.Second)
	if len(announces) < 3 {
		t.Fatalf("expected 3 announce events, got %d", len(announces))
	}

	announcedIDs := map[string]bool{}
	for _, a := range announces {
		announcedIDs[a.AgentId] = true
	}
	for _, a := range agents {
		if !announcedIDs[a.id] {
			t.Errorf("missing announce for %s", a.id)
		}
	}
	t.Log("Step 1-2 PASS: All agents registered and announced")

	// --- Step 3: Send discovery request for risk-estimation ---
	corrID1 := uuid.Must(uuid.NewV7()).String()
	discReq := &protocolv1.DiscoveryRequestPayload{
		CapabilityName: "risk-estimation",
	}
	data, _ := proto.Marshal(discReq)

	err = bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "agent.discovery.request",
		SourceNode:    "e2e-node",
		SourceAgent:   "requesting-agent",
		CorrelationID: corrID1,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
	if err != nil {
		t.Fatalf("publish discovery request: %v", err)
	}

	responses := respWatcher.waitFor(1, 3*time.Second)
	if len(responses) == 0 {
		t.Fatal("expected discovery response, got none")
	}

	resp := responses[0]
	if len(resp.Matches) != 2 {
		t.Fatalf("expected 2 matches for risk-estimation, got %d", len(resp.Matches))
	}

	matchIDs := map[string]bool{}
	for _, m := range resp.Matches {
		matchIDs[m.AgentId] = true
	}
	if !matchIDs["e2e-agent-A"] || !matchIDs["e2e-agent-B"] {
		t.Errorf("expected agents A and B, got %v", matchIDs)
	}
	t.Log("Step 3 PASS: Discovery response contains agents A and B")

	// --- Step 4: Unregister agent A ---
	unregPayload := &protocolv1.AgentUnregisterPayload{
		AgentId: "e2e-agent-A",
		Reason:  "e2e test unregister",
	}
	data, _ = proto.Marshal(unregPayload)

	err = bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.unregister",
		SourceNode:  "e2e-node",
		SourceAgent: "e2e-agent-A",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("unregister A: %v", err)
	}

	// --- Step 5: Verify deannounce event ---
	allAnnounces := annWatcher.waitFor(4, 3*time.Second)
	var foundDeannounce bool
	for _, a := range allAnnounces {
		if a.AgentId == "e2e-agent-A" && len(a.Capabilities) == 0 {
			foundDeannounce = true
		}
	}
	if !foundDeannounce {
		t.Error("expected deannounce for agent A (empty capabilities)")
	}
	t.Log("Step 4-5 PASS: Agent A unregistered and deannounced")

	// --- Step 6: Send discovery request again → only B returned ---
	corrID2 := uuid.Must(uuid.NewV7()).String()
	discReq2 := &protocolv1.DiscoveryRequestPayload{
		CapabilityName: "risk-estimation",
	}
	data, _ = proto.Marshal(discReq2)

	err = bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "agent.discovery.request",
		SourceNode:    "e2e-node",
		SourceAgent:   "requesting-agent",
		CorrelationID: corrID2,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
	if err != nil {
		t.Fatalf("publish second discovery request: %v", err)
	}

	allResponses := respWatcher.waitFor(2, 3*time.Second)
	if len(allResponses) < 2 {
		t.Fatalf("expected 2 total responses, got %d", len(allResponses))
	}

	// The second response should only have agent B.
	resp2 := allResponses[1]
	if len(resp2.Matches) != 1 {
		t.Fatalf("expected 1 match after unregister, got %d", len(resp2.Matches))
	}
	if resp2.Matches[0].AgentId != "e2e-agent-B" {
		t.Errorf("expected agent B, got %s", resp2.Matches[0].AgentId)
	}
	t.Log("Step 6 PASS: Discovery after unregister returns only agent B")
}
