package registry_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/carlosmolina/agent-os/core/eventbus"
	natseventbus "github.com/carlosmolina/agent-os/core/eventbus/nats"
	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/carlosmolina/agent-os/core/testutil"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

type handlerEnv struct {
	bus      *natseventbus.Bus
	registry *registry.KVRegistry
	handler  *registry.Handler
}

func setupHandler(t *testing.T) *handlerEnv {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}

	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	h := registry.NewHandler(bus, reg, "test-node")
	subs, err := h.Start(ctx)
	if err != nil {
		t.Fatalf("start handler: %v", err)
	}

	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
		_ = bus.Close()
	})

	// Give consumers time to initialize.
	time.Sleep(300 * time.Millisecond)

	return &handlerEnv{bus: bus, registry: reg, handler: h}
}

func publishRegisterEvent(t *testing.T, ctx context.Context, bus *natseventbus.Bus, agentID, agentType string, caps []*protocolv1.Capability) {
	t.Helper()
	payload := &protocolv1.AgentRegisterPayload{
		AgentId:      agentID,
		AgentType:    agentType,
		Version:      "1.0.0",
		Capabilities: caps,
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	err = bus.Publish(ctx, &eventbus.Event{
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

func TestHandlerRegisterEvent(t *testing.T) {
	env := setupHandler(t)
	ctx := context.Background()

	publishRegisterEvent(t, ctx, env.bus, "handler-agent-1", "test-type",
		[]*protocolv1.Capability{{Name: "cap1", Version: "1.0.0"}})

	// Wait for event processing.
	time.Sleep(500 * time.Millisecond)

	agent, _, err := env.registry.Get(ctx, "handler-agent-1")
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}
	if agent.AgentType != "test-type" {
		t.Errorf("got type %q, want %q", agent.AgentType, "test-type")
	}
	if agent.Status != registry.StatusActive {
		t.Errorf("got status %v, want ACTIVE", agent.Status)
	}
}

func TestHandlerRegisterInvalidPublishesError(t *testing.T) {
	env := setupHandler(t)
	ctx := context.Background()

	var errorReceived bool
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := env.bus.Subscribe(ctx, "agent.error", func(_ context.Context, evt *eventbus.Event) error {
		mu.Lock()
		defer mu.Unlock()
		if !errorReceived {
			errorReceived = true
			wg.Done()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Register with no capabilities → should produce an error event.
	publishRegisterEvent(t, ctx, env.bus, "bad-agent", "test-type", nil)

	waitDone(t, &wg, 5*time.Second)

	mu.Lock()
	if !errorReceived {
		t.Error("expected agent.error event for invalid registration")
	}
	mu.Unlock()
}

func TestHandlerUnregisterEvent(t *testing.T) {
	env := setupHandler(t)
	ctx := context.Background()

	// First register.
	publishRegisterEvent(t, ctx, env.bus, "unreg-agent", "test-type",
		[]*protocolv1.Capability{{Name: "cap", Version: "1.0.0"}})
	time.Sleep(500 * time.Millisecond)

	// Verify registered.
	_, _, err := env.registry.Get(ctx, "unreg-agent")
	if err != nil {
		t.Fatalf("expected agent to be registered: %v", err)
	}

	// Unregister.
	payload := &protocolv1.AgentUnregisterPayload{
		AgentId: "unreg-agent",
		Reason:  "graceful shutdown",
	}
	data, _ := proto.Marshal(payload)

	err = env.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.unregister",
		SourceNode:  "test-node",
		SourceAgent: "unreg-agent",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish unregister: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	_, _, err = env.registry.Get(ctx, "unreg-agent")
	if err != registry.ErrNotFound {
		t.Errorf("expected agent to be deregistered, got %v", err)
	}
}

func TestHandlerUnregisterNonexistentNoError(t *testing.T) {
	env := setupHandler(t)
	ctx := context.Background()

	payload := &protocolv1.AgentUnregisterPayload{
		AgentId: "ghost-agent",
		Reason:  "never existed",
	}
	data, _ := proto.Marshal(payload)

	err := env.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.unregister",
		SourceNode:  "test-node",
		SourceAgent: "ghost-agent",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// No panic, no error — idempotent.
	time.Sleep(500 * time.Millisecond)
}

func TestErrorEventPersistedToStream(t *testing.T) {
	env := setupHandler(t)
	ctx := context.Background()

	var received *eventbus.Event
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := env.bus.Subscribe(ctx, "agent.error", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.AgentErrorPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			mu.Lock()
			if received == nil {
				received = evt
				wg.Done()
			}
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish an error event directly.
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:    "err-agent",
		ErrorCode:  "TEST_ERROR",
		Message:    "something went wrong",
		StackTrace: "at line 42",
		WorkflowId: "wf-001",
	}
	data, _ := proto.Marshal(errPayload)

	err = env.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  "test-node",
		SourceAgent: "err-agent",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
	if err != nil {
		t.Fatalf("publish error: %v", err)
	}

	waitDone(t, &wg, 5*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if received == nil {
		t.Fatal("expected error event to be persisted and delivered")
	}

	var got protocolv1.AgentErrorPayload
	if err := proto.Unmarshal(received.Payload, &got); err != nil {
		t.Fatalf("unmarshal error payload: %v", err)
	}
	if got.ErrorCode != "TEST_ERROR" {
		t.Errorf("got error code %q, want %q", got.ErrorCode, "TEST_ERROR")
	}
	if got.WorkflowId != "wf-001" {
		t.Errorf("got workflow_id %q, want %q", got.WorkflowId, "wf-001")
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
