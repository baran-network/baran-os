package health_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/health"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/testutil"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

type testEnv struct {
	bus      *natseventbus.Bus
	registry *registry.KVRegistry
	monitor  *health.Monitor
}

func setup(t *testing.T, cfg health.Config) *testEnv {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}

	reg, err := registry.NewKVRegistry(ctx, nc, cfg.UnhealthyThreshold, cfg.DeadThreshold)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	mon := health.New(bus, reg, "test-node", cfg)

	t.Cleanup(func() {
		mon.Stop()
		_ = bus.Close()
	})

	return &testEnv{bus: bus, registry: reg, monitor: mon}
}

func registerAgent(t *testing.T, ctx context.Context, reg *registry.KVRegistry, agentID string) {
	t.Helper()
	_, err := reg.Register(ctx, registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    "test",
		Version:      "1.0.0",
		Capabilities: []registry.Capability{{Name: "test", Version: "1.0.0"}},
		NodeID:       "test-node",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func TestPingSentToRegisteredAgent(t *testing.T) {
	cfg := health.Config{
		HeartbeatInterval:  200 * time.Millisecond,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
	env := setup(t, cfg)
	ctx := context.Background()

	registerAgent(t, ctx, env.registry, "agent-ping")

	var received bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := env.bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.HealthPingPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil && payload.AgentId == "agent-ping" {
			mu.Lock()
			if !received {
				received = true
				wg.Done()
			}
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_, err = env.monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}

	waitDone(t, &wg, 5*time.Second)

	mu.Lock()
	if !received {
		t.Error("expected ping for registered agent")
	}
	mu.Unlock()
}

func TestPongResetsCounter(t *testing.T) {
	cfg := health.Config{
		HeartbeatInterval:  200 * time.Millisecond,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
	env := setup(t, cfg)
	ctx := context.Background()

	registerAgent(t, ctx, env.registry, "agent-pong")

	// Subscribe to pings and auto-respond with pongs.
	_, err := env.bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		var ping protocolv1.HealthPingPayload
		if err := proto.Unmarshal(evt.Payload, &ping); err != nil {
			return nil
		}
		if ping.AgentId != "agent-pong" {
			return nil
		}

		pong := &protocolv1.HealthPongPayload{
			AgentId:  ping.AgentId,
			Sequence: ping.Sequence,
			Status:   protocolv1.AgentStatus_AGENT_STATUS_HEALTHY,
		}
		data, _ := proto.Marshal(pong)

		return env.bus.Publish(ctx, &eventbus.Event{
			ID:          uuid.Must(uuid.NewV7()).String(),
			Type:        "agent.health.pong",
			SourceNode:  "test-node",
			SourceAgent: ping.AgentId,
			Timestamp:   time.Now().UnixNano(),
			Payload:     data,
		})
	})
	if err != nil {
		t.Fatalf("subscribe ping: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	_, err = env.monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}

	// Wait for a few ping/pong cycles.
	time.Sleep(800 * time.Millisecond)

	agent, _, err := env.registry.Get(ctx, "agent-pong")
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}

	if agent.Status != registry.StatusActive {
		t.Errorf("expected ACTIVE, got %v", agent.Status)
	}
}

func TestUnhealthyAfterMissedHeartbeats(t *testing.T) {
	cfg := health.Config{
		HeartbeatInterval:  150 * time.Millisecond,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
	env := setup(t, cfg)
	ctx := context.Background()

	registerAgent(t, ctx, env.registry, "agent-unhealthy")

	_, err := env.monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}

	// Wait for 3 ping rounds (~450ms + buffer).
	time.Sleep(700 * time.Millisecond)

	agent, _, err := env.registry.Get(ctx, "agent-unhealthy")
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}

	if agent.Status != registry.StatusUnhealthy {
		t.Errorf("after 3 missed: expected UNHEALTHY, got %v (missed=%d)", agent.Status, agent.MissedHeartbeats)
	}
}

func TestRecoveryFromUnhealthy(t *testing.T) {
	cfg := health.Config{
		HeartbeatInterval:  150 * time.Millisecond,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
	env := setup(t, cfg)
	ctx := context.Background()

	registerAgent(t, ctx, env.registry, "agent-recover")

	_, err := env.monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}

	// Let it go UNHEALTHY.
	time.Sleep(700 * time.Millisecond)

	agent, _, _ := env.registry.Get(ctx, "agent-recover")
	if agent.Status != registry.StatusUnhealthy {
		t.Fatalf("expected UNHEALTHY before recovery, got %v", agent.Status)
	}

	// Stop the monitor so pings don't race with the pong we're about to send.
	env.monitor.Stop()

	// Send a pong to recover. The monitor's pong subscription is still active
	// until bus.Close, so use RecordHeartbeat directly to test the registry behavior.
	_, rev, _ := env.registry.Get(ctx, "agent-recover")
	_, err = env.registry.RecordHeartbeat(ctx, "agent-recover", rev)
	if err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	agent, _, _ = env.registry.Get(ctx, "agent-recover")
	if agent.Status != registry.StatusActive {
		t.Errorf("expected ACTIVE after recovery heartbeat, got %v", agent.Status)
	}
}

func TestDeadAgentAutoDeregistered(t *testing.T) {
	cfg := health.Config{
		HeartbeatInterval:  100 * time.Millisecond,
		UnhealthyThreshold: 2,
		DeadThreshold:      4,
	}
	env := setup(t, cfg)
	ctx := context.Background()

	registerAgent(t, ctx, env.registry, "agent-dead")

	_, err := env.monitor.Start(ctx)
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}

	// Wait for enough rounds to reach dead threshold and deregister.
	time.Sleep(800 * time.Millisecond)

	_, _, err = env.registry.Get(ctx, "agent-dead")
	if err != registry.ErrNotFound {
		t.Errorf("expected dead agent to be deregistered, got err=%v", err)
	}
}

func waitDone(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("timed out waiting")
	}
}
