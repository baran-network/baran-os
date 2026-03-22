package simulation_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/router"
	"github.com/baran-network/baran-os/core/simulation"
	"github.com/baran-network/baran-os/core/testutil"
)

type testSetup struct {
	bus    *natseventbus.Bus
	store  *simulation.JetStreamEventStore
	engine *simulation.ReplayEngine
	streams *router.StreamRegistry
}

func newTestSetup(t *testing.T) *testSetup {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	streams := router.DefaultStreamRegistry()
	bus, err := natseventbus.NewFromConn(ctx, nc, streams)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	store := simulation.NewJetStreamEventStore(bus.JetStream(), streams)
	engine := simulation.NewReplayEngine(store, bus, bus.JetStream(), "test-node")
	return &testSetup{bus: bus, store: store, engine: engine, streams: streams}
}

func publishEvent(t *testing.T, bus *natseventbus.Bus, id, eventType, sourceAgent, workflowID string, ts time.Time) {
	t.Helper()
	ctx := context.Background()
	evt := &eventbus.Event{
		ID:          id,
		Type:        eventType,
		SourceNode:  "node-1",
		SourceAgent: sourceAgent,
		WorkflowID:  workflowID,
		Timestamp:   ts.UnixNano(),
		Payload:     []byte("test"),
	}
	if err := bus.Publish(ctx, evt); err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

func TestQueryByTimeRange(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	now := time.Now()
	publishEvent(t, ts.bus, "evt-1", "agent.register", "agent-a", "", now.Add(-2*time.Hour))
	publishEvent(t, ts.bus, "evt-2", "agent.register", "agent-b", "", now.Add(-1*time.Hour))
	publishEvent(t, ts.bus, "evt-3", "agent.register", "agent-c", "", now.Add(-30*time.Minute))

	// Allow JetStream to persist.
	time.Sleep(300 * time.Millisecond)

	// Query last 90 minutes: should get evt-2 and evt-3.
	events, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime: now.Add(-90 * time.Minute),
		EndTime:   now,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(events) < 2 {
		t.Fatalf("expected at least 2 events, got %d", len(events))
	}

	// Verify chronological order.
	for i := 1; i < len(events); i++ {
		if events[i].Event.Timestamp < events[i-1].Event.Timestamp {
			t.Errorf("events not in chronological order at index %d", i)
		}
	}
}

func TestQueryByEventType(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	now := time.Now()
	publishEvent(t, ts.bus, "evt-t1", "agent.register", "agent-a", "", now)
	publishEvent(t, ts.bus, "evt-t2", "agent.health.ping", "agent-a", "", now)

	time.Sleep(300 * time.Millisecond)

	events, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime:  now.Add(-1 * time.Minute),
		EventTypes: []string{"agent.health.ping"},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Event.Type != "agent.health.ping" {
		t.Errorf("got type %q, want agent.health.ping", events[0].Event.Type)
	}
}

func TestQueryBySourceAgent(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	now := time.Now()
	publishEvent(t, ts.bus, "evt-s1", "agent.register", "agent-alpha", "", now)
	publishEvent(t, ts.bus, "evt-s2", "agent.register", "agent-beta", "", now.Add(time.Millisecond))

	time.Sleep(300 * time.Millisecond)

	events, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime:   now.Add(-1 * time.Minute),
		SourceAgent: "agent-alpha",
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Event.SourceAgent != "agent-alpha" {
		t.Errorf("got source %q, want agent-alpha", events[0].Event.SourceAgent)
	}
}

func TestQueryPagination(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		publishEvent(t, ts.bus, fmt.Sprintf("evt-p%d", i), "agent.register", "agent-a", "", now.Add(time.Duration(i)*time.Millisecond))
	}

	time.Sleep(300 * time.Millisecond)

	// First page: limit 2, offset 0.
	page1, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime: now.Add(-1 * time.Minute),
		Limit:     2,
		Offset:    0,
	})
	if err != nil {
		t.Fatalf("query page 1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page 1: expected 2 events, got %d", len(page1))
	}

	// Second page: limit 2, offset 2.
	page2, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime: now.Add(-1 * time.Minute),
		Limit:     2,
		Offset:    2,
	})
	if err != nil {
		t.Fatalf("query page 2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page 2: expected 2 events, got %d", len(page2))
	}

	// Pages should not overlap.
	if page1[0].Event.ID == page2[0].Event.ID {
		t.Error("page 1 and page 2 overlap")
	}
}

func TestGetWorkflowEvents(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	workflowID := "wf-test-001"
	streamName := "WF-" + workflowID

	// Create the workflow stream manually (normally done by WorkflowStreamManager).
	if err := ts.bus.EnsureStream(ctx, streamName, []string{"workflow." + workflowID + ".>"}); err != nil {
		t.Fatalf("ensure stream: %v", err)
	}

	now := time.Now()
	// Publish events to the workflow stream using workflow-scoped subjects.
	for i := 0; i < 3; i++ {
		evt := &eventbus.Event{
			ID:          fmt.Sprintf("wf-evt-%d", i),
			Type:        fmt.Sprintf("workflow.%s.workflow.step.result", workflowID),
			SourceNode:  "node-1",
			SourceAgent: "agent-a",
			WorkflowID:  workflowID,
			Timestamp:   now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Payload:     []byte("step-result"),
		}
		if err := ts.bus.Publish(ctx, evt); err != nil {
			t.Fatalf("publish workflow event %d: %v", i, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	events, err := ts.store.GetWorkflowEvents(ctx, workflowID)
	if err != nil {
		t.Fatalf("get workflow events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}

	// All events should be from the WF stream.
	for _, e := range events {
		if e.Stream != streamName {
			t.Errorf("got stream %q, want %q", e.Stream, streamName)
		}
		if e.Event.WorkflowID != workflowID {
			t.Errorf("got workflow %q, want %q", e.Event.WorkflowID, workflowID)
		}
	}

	// Verify chronological order.
	for i := 1; i < len(events); i++ {
		if events[i].Event.Timestamp < events[i-1].Event.Timestamp {
			t.Errorf("events not in chronological order at index %d", i)
		}
	}
}

func TestGetWorkflowEventsNotFound(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	events, err := ts.store.GetWorkflowEvents(ctx, "nonexistent-wf")
	if err != nil {
		t.Fatalf("expected nil error for missing stream, got: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

// TestReplaySession verifies that:
// 1. A replay session can be created from a workflow's events.
// 2. All replayed events appear in the SIMULATION stream with correct metadata.
// 3. No events are published to live streams (AGENTS, WF-{id}).
func TestReplaySession(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	workflowID := "wf-replay-test-001"
	streamName := "WF-" + workflowID

	// Create the workflow stream and publish 3 events.
	if err := ts.bus.EnsureStream(ctx, streamName, []string{"workflow." + workflowID + ".>"}); err != nil {
		t.Fatalf("ensure workflow stream: %v", err)
	}

	now := time.Now()
	originalIDs := []string{"orig-evt-1", "orig-evt-2", "orig-evt-3"}
	for i, id := range originalIDs {
		evt := &eventbus.Event{
			ID:          id,
			Type:        fmt.Sprintf("workflow.%s.workflow.step.result", workflowID),
			SourceNode:  "node-1",
			SourceAgent: "agent-a",
			WorkflowID:  workflowID,
			Timestamp:   now.Add(time.Duration(i) * time.Millisecond).UnixNano(),
			Payload:     []byte("step-data"),
		}
		if err := ts.bus.Publish(ctx, evt); err != nil {
			t.Fatalf("publish original event %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// Subscribe to SIMULATION stream to collect replayed events.
	var mu sync.Mutex
	var simulationEvents []*eventbus.Event
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		mu.Lock()
		simulationEvents = append(simulationEvents, e)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe simulation: %v", err)
	}
	defer sub.Unsubscribe()

	// Subscribe to AGENTS stream to verify no live events arrive.
	var agentsReceived int
	agentsSub, err := ts.bus.Subscribe(ctx, "agent.register", func(_ context.Context, _ *eventbus.Event) error {
		mu.Lock()
		agentsReceived++
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agents: %v", err)
	}
	defer agentsSub.Unsubscribe()

	// Create and start replay session (max speed = 0).
	session, err := ts.engine.CreateSession(ctx, workflowID, 0)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if session.TotalEvents != 3 {
		t.Fatalf("expected 3 total events, got %d", session.TotalEvents)
	}

	// Wait for session to complete (max 5s).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := ts.engine.GetSession(session.ID)
		if s != nil && (s.State == simulation.SessionStateCompleted || s.State == simulation.SessionStateError) {
			session = s
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if session.State != simulation.SessionStateCompleted {
		t.Fatalf("expected session completed, got %s (error: %s)", session.State, session.ErrorMessage)
	}
	if session.ReplayedEvents != 3 {
		t.Fatalf("expected 3 replayed events, got %d", session.ReplayedEvents)
	}

	// Allow NATS to deliver messages to subscribers.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Expect: 3 replayed data events + 1 replay.start + 1 replay.complete = at least 5 events.
	if len(simulationEvents) < 5 {
		t.Fatalf("expected at least 5 simulation events, got %d", len(simulationEvents))
	}

	// Verify replayed data events have correct metadata.
	var dataEvents []*eventbus.Event
	for _, e := range simulationEvents {
		if e.Metadata["simulation.replay"] == "true" {
			dataEvents = append(dataEvents, e)
		}
	}
	if len(dataEvents) != 3 {
		t.Fatalf("expected 3 replayed data events, got %d", len(dataEvents))
	}

	// Verify replay metadata on each data event.
	for _, e := range dataEvents {
		if e.Metadata["simulation.session_id"] != session.ID {
			t.Errorf("wrong session_id: got %q, want %q", e.Metadata["simulation.session_id"], session.ID)
		}
		if e.Metadata["simulation.original_id"] == "" {
			t.Error("missing simulation.original_id metadata")
		}
		if e.Metadata["simulation.original_timestamp"] == "" {
			t.Error("missing simulation.original_timestamp metadata")
		}
		// Replayed events must have new IDs (not the original ones).
		for _, origID := range originalIDs {
			if e.ID == origID {
				t.Errorf("replayed event reused original ID %s", origID)
			}
		}
		// WorkflowID must be preserved.
		if e.WorkflowID != workflowID {
			t.Errorf("workflow_id not preserved: got %q, want %q", e.WorkflowID, workflowID)
		}
	}

	// Verify no events landed in the AGENTS live stream from replay.
	if agentsReceived > 0 {
		t.Errorf("replayed events leaked into AGENTS stream: %d events", agentsReceived)
	}
}

// TestReplaySessionStop verifies that StopSession transitions the session to STOPPED.
func TestReplaySessionStop(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	workflowID := "wf-stop-test-001"
	streamName := "WF-" + workflowID

	if err := ts.bus.EnsureStream(ctx, streamName, []string{"workflow." + workflowID + ".>"}); err != nil {
		t.Fatalf("ensure workflow stream: %v", err)
	}

	now := time.Now()
	// Publish 10 events with real-time speed so the session runs long enough to stop.
	for i := 0; i < 10; i++ {
		evt := &eventbus.Event{
			ID:         fmt.Sprintf("stop-evt-%d", i),
			Type:       fmt.Sprintf("workflow.%s.workflow.step.result", workflowID),
			SourceNode: "node-1",
			WorkflowID: workflowID,
			Timestamp:  now.Add(time.Duration(i) * time.Second).UnixNano(), // 1s apart
			Payload:    []byte("data"),
		}
		if err := ts.bus.Publish(ctx, evt); err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// Use speed=1.0 so there's a delay between events.
	session, err := ts.engine.CreateSession(ctx, workflowID, 1.0)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	// Wait briefly for the session to start.
	time.Sleep(100 * time.Millisecond)

	// Stop the session.
	if err := ts.engine.StopSession(session.ID); err != nil {
		t.Fatalf("stop session: %v", err)
	}

	// Wait for the stop to propagate.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := ts.engine.GetSession(session.ID)
		if s != nil && s.State != simulation.SessionStateRunning {
			session = s
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if session.State != simulation.SessionStateStopped {
		t.Fatalf("expected session stopped, got %s", session.State)
	}

	// StopSession on an already stopped session returns an error.
	if err := ts.engine.StopSession(session.ID); err == nil {
		t.Error("expected error when stopping a non-running session")
	}
}

func TestQueryStreamMetadata(t *testing.T) {
	ts := newTestSetup(t)
	ctx := context.Background()

	now := time.Now()
	publishEvent(t, ts.bus, "evt-meta-1", "agent.register", "agent-a", "", now)

	time.Sleep(300 * time.Millisecond)

	events, err := ts.store.Query(ctx, simulation.EventQuery{
		StartTime:  now.Add(-1 * time.Minute),
		EventTypes: []string{"agent.register"},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(events) == 0 {
		t.Fatal("expected at least 1 event")
	}

	// Verify stream metadata is populated.
	if events[0].Stream != "AGENTS" {
		t.Errorf("got stream %q, want AGENTS", events[0].Stream)
	}
	if events[0].Sequence == 0 {
		t.Error("expected non-zero sequence number")
	}
}
