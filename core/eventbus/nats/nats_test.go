package nats_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ad-hok/agent-os/core/eventbus"
	natseventbus "github.com/ad-hok/agent-os/core/eventbus/nats"
	"github.com/ad-hok/agent-os/core/testutil"
)

func newTestBus(t *testing.T) *natseventbus.Bus {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()
	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("failed to create bus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	bus := newTestBus(t)
	ctx := context.Background()

	var received *eventbus.Event
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe(ctx, "agent.register", func(_ context.Context, evt *eventbus.Event) error {
		received = evt
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Give consumer time to initialize.
	time.Sleep(200 * time.Millisecond)

	evt := &eventbus.Event{
		ID:          "evt-001",
		Type:        "agent.register",
		SourceNode:  "node-1",
		SourceAgent: "agent-1",
		Timestamp:   time.Now().UnixNano(),
		Payload:     []byte("test-payload"),
	}

	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitDone(t, &wg, 5*time.Second)

	if received == nil {
		t.Fatal("expected to receive event")
	}
	if received.ID != evt.ID {
		t.Errorf("got ID %q, want %q", received.ID, evt.ID)
	}
	if received.SourceAgent != evt.SourceAgent {
		t.Errorf("got SourceAgent %q, want %q", received.SourceAgent, evt.SourceAgent)
	}
	if string(received.Payload) != string(evt.Payload) {
		t.Errorf("got Payload %q, want %q", received.Payload, evt.Payload)
	}
}

func TestDeduplication(t *testing.T) {
	bus := newTestBus(t)
	ctx := context.Background()

	var mu sync.Mutex
	count := 0

	_, err := bus.Subscribe(ctx, "agent.register", func(_ context.Context, evt *eventbus.Event) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	evt := &eventbus.Event{
		ID:          "dedup-001",
		Type:        "agent.register",
		SourceNode:  "node-1",
		SourceAgent: "agent-1",
		Timestamp:   time.Now().UnixNano(),
	}

	// Publish the same event twice.
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Wait for potential delivery.
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 delivery (dedup), got %d", count)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	bus := newTestBus(t)
	ctx := context.Background()

	var mu sync.Mutex
	count := 0

	sub, err := bus.Subscribe(ctx, "agent.register", func(_ context.Context, evt *eventbus.Event) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish one event before unsubscribe.
	if err := bus.Publish(ctx, &eventbus.Event{
		ID: "unsub-001", Type: "agent.register", SourceNode: "n", SourceAgent: "a", Timestamp: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	if err := sub.Unsubscribe(); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish another event after unsubscribe — should not be delivered.
	if err := bus.Publish(ctx, &eventbus.Event{
		ID: "unsub-002", Type: "agent.register", SourceNode: "n", SourceAgent: "a", Timestamp: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 delivery after unsubscribe, got %d", count)
	}
}

func TestHealthStreamRouting(t *testing.T) {
	bus := newTestBus(t)
	ctx := context.Background()

	var received *eventbus.Event
	var wg sync.WaitGroup
	wg.Add(1)

	_, err := bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, evt *eventbus.Event) error {
		received = evt
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if err := bus.Publish(ctx, &eventbus.Event{
		ID: "health-001", Type: "agent.health.ping", SourceNode: "n", SourceAgent: "a", Timestamp: time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitDone(t, &wg, 5*time.Second)

	if received == nil {
		t.Fatal("expected health ping event")
	}
	if received.ID != "health-001" {
		t.Errorf("got ID %q, want %q", received.ID, "health-001")
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
		t.Fatal("timed out waiting for event delivery")
	}
}
