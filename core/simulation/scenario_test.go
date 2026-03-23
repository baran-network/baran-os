package simulation_test

import (
	"context"
	"embed"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/simulation"
)

//go:embed testdata/wildfire-simulation.json
var scenarioFS embed.FS

// scenarioSetup provides EventInjector and ScenarioEngine for integration tests.
type scenarioSetup struct {
	testSetup
	injector *simulation.EventInjector
	engine   *simulation.ScenarioEngine
}

func newScenarioSetup(t *testing.T) *scenarioSetup {
	t.Helper()
	base := newTestSetup(t)
	injector := simulation.NewEventInjector(base.bus.JetStream(), base.bus, "test-node")
	engine := simulation.NewScenarioEngine(injector, base.bus.JetStream())
	return &scenarioSetup{testSetup: *base, injector: injector, engine: engine}
}

// TestInjectSyntheticEvent verifies that injecting a single synthetic event via EventInjector
// results in the event appearing on the SIMULATION stream with correct synthetic metadata markers.
func TestInjectSyntheticEvent(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Subscribe to SIMULATION stream before injecting.
	var mu sync.Mutex
	var received []map[string]string
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		mu.Lock()
		received = append(received, e.Metadata)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe simulation: %v", err)
	}
	defer sub.Unsubscribe()

	// Inject a synthetic event.
	req := simulation.InjectRequest{
		EventType:   "workflow.start",
		SourceAgent: "sensor-001",
		WorkflowID:  "wf-sim-001",
		Metadata: map[string]string{
			"zone": "north-ridge",
		},
	}
	result, err := ts.injector.Inject(ctx, req, "", "")
	if err != nil {
		t.Fatalf("inject: %v", err)
	}

	// Verify the result contains the event ID and stream.
	if result.EventID == "" {
		t.Error("expected non-empty event_id")
	}
	if result.Stream == "" {
		t.Error("expected non-empty stream name")
	}
	if result.Sequence == 0 {
		t.Error("expected non-zero sequence")
	}

	// Allow NATS to deliver the message.
	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Find our injected event by checking the synthetic marker.
	var found bool
	for _, meta := range received {
		if meta["simulation.synthetic"] == "true" {
			found = true
			// Custom metadata should be preserved.
			if meta["zone"] != "north-ridge" {
				t.Errorf("expected zone=north-ridge, got %q", meta["zone"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected at least one event with simulation.synthetic=true, received %d events", len(received))
	}
}

// TestInjectSyntheticEventWithSession verifies that session and scenario metadata are set
// when injecting as part of a named scenario session.
func TestInjectSyntheticEventWithSession(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	var mu sync.Mutex
	var syntheticMeta map[string]string
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		if e.Metadata["simulation.synthetic"] == "true" {
			mu.Lock()
			syntheticMeta = e.Metadata
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe simulation: %v", err)
	}
	defer sub.Unsubscribe()

	req := simulation.InjectRequest{
		EventType: "workflow.step",
	}
	_, err = ts.injector.Inject(ctx, req, "session-abc", "wildfire-test")
	if err != nil {
		t.Fatalf("inject: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if syntheticMeta == nil {
		t.Fatal("no synthetic event received")
	}
	if syntheticMeta["simulation.synthetic"] != "true" {
		t.Error("simulation.synthetic marker missing")
	}
	if syntheticMeta["simulation.session_id"] != "session-abc" {
		t.Errorf("session_id: got %q, want session-abc", syntheticMeta["simulation.session_id"])
	}
	if syntheticMeta["simulation.scenario_name"] != "wildfire-test" {
		t.Errorf("scenario_name: got %q, want wildfire-test", syntheticMeta["simulation.scenario_name"])
	}
}

// TestInjectRequiresEventType verifies that injecting without event_type returns an error.
func TestInjectRequiresEventType(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	_, err := ts.injector.Inject(ctx, simulation.InjectRequest{}, "", "")
	if err == nil {
		t.Fatal("expected error for missing event_type")
	}
}

// TestScenarioExecution registers a 3-step scenario with delays, starts it,
// and verifies all synthetic events arrive in order on the SIMULATION stream with session metadata.
func TestScenarioExecution(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Collect synthetic events from the SIMULATION stream.
	var mu sync.Mutex
	var syntheticEvents []string // event types in order
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		if e.Metadata["simulation.synthetic"] == "true" {
			mu.Lock()
			syntheticEvents = append(syntheticEvents, e.Type)
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Register a 3-step scenario with small delays.
	def := &simulation.ScenarioDefinition{
		Name:        "test-execution",
		Description: "Three-step scenario for testing sequential execution",
		Steps: []simulation.ScenarioStep{
			{EventType: "workflow.start", DelayMs: 0, SourceAgent: "sensor-001"},
			{EventType: "workflow.step", DelayMs: 50, SourceAgent: "risk-agent"},
			{EventType: "workflow.complete", DelayMs: 50, SourceAgent: "coordinator"},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Start the scenario.
	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if session.State != simulation.ScenarioStateRunning {
		t.Fatalf("expected running, got %s", session.State)
	}

	// Wait for scenario to complete (3 steps + 100ms delays + buffer).
	time.Sleep(800 * time.Millisecond)

	// Verify session reached completed state.
	final := ts.engine.GetSession(session.ID)
	if final == nil {
		t.Fatal("session not found after execution")
	}
	if final.State != simulation.ScenarioStateCompleted {
		t.Errorf("expected completed, got %s (error: %s)", final.State, final.ErrorMessage)
	}
	if final.InjectedEvents != 3 {
		t.Errorf("expected 3 injected events, got %d", final.InjectedEvents)
	}
	if final.DurationMs <= 0 {
		t.Error("expected positive duration")
	}

	// Verify synthetic events arrived in correct order.
	mu.Lock()
	defer mu.Unlock()
	expected := []string{"workflow.start", "workflow.step", "workflow.complete"}
	if len(syntheticEvents) < len(expected) {
		t.Fatalf("expected at least %d synthetic events, got %d", len(expected), len(syntheticEvents))
	}
	for i, want := range expected {
		if syntheticEvents[i] != want {
			t.Errorf("event[%d]: got %q, want %q", i, syntheticEvents[i], want)
		}
	}
}

// TestScenarioStop starts a scenario and stops it mid-execution, verifying that
// the session transitions to STOPPED with the correct reason and no further events are injected.
func TestScenarioStop(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Register a scenario with enough delay to allow stopping mid-execution.
	def := &simulation.ScenarioDefinition{
		Name: "test-stop",
		Steps: []simulation.ScenarioStep{
			{EventType: "workflow.start", DelayMs: 0},
			{EventType: "workflow.step", DelayMs: 2000}, // long delay to ensure we can stop
			{EventType: "workflow.complete", DelayMs: 0},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for first event to be injected, then stop.
	time.Sleep(300 * time.Millisecond)
	if err := ts.engine.StopSession(session.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}

	// Wait for termination to propagate.
	time.Sleep(300 * time.Millisecond)

	final := ts.engine.GetSession(session.ID)
	if final == nil {
		t.Fatal("session not found")
	}
	if final.State != simulation.ScenarioStateStopped {
		t.Errorf("expected stopped, got %s", final.State)
	}
	// Should have injected only the first event (second was delayed).
	if final.InjectedEvents > 1 {
		t.Errorf("expected at most 1 injected event, got %d", final.InjectedEvents)
	}

	// Trying to stop again should return a conflict error.
	err = ts.engine.StopSession(session.ID)
	if err == nil {
		t.Error("expected error when stopping already-stopped session")
	}
}

// TestScenarioConditionTimeout verifies that a scenario with a condition step that
// times out transitions to FAILED with a descriptive error message.
func TestScenarioConditionTimeout(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Register a scenario where step 0 waits for an event that will never arrive.
	def := &simulation.ScenarioDefinition{
		Name: "test-condition-timeout",
		Steps: []simulation.ScenarioStep{
			{
				EventType: "workflow.start",
				DelayMs:   0,
				Condition: &simulation.StepCondition{
					ExpectEventType: "never.arriving.event",
					TimeoutMs:       500, // short timeout
				},
			},
			{EventType: "workflow.complete", DelayMs: 0},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for condition timeout + buffer.
	time.Sleep(1500 * time.Millisecond)

	final := ts.engine.GetSession(session.ID)
	if final == nil {
		t.Fatal("session not found")
	}
	if final.State != simulation.ScenarioStateFailed {
		t.Errorf("expected failed, got %s", final.State)
	}
	if final.ErrorMessage == "" {
		t.Error("expected non-empty error message")
	}
	// Only the first event should have been injected (condition failed before step 2).
	if final.InjectedEvents != 1 {
		t.Errorf("expected 1 injected event, got %d", final.InjectedEvents)
	}
}

// TestScenarioValidation verifies that invalid scenario definitions are rejected.
func TestScenarioValidation(t *testing.T) {
	ts := newScenarioSetup(t)

	tests := []struct {
		name    string
		def     simulation.ScenarioDefinition
		wantErr string
	}{
		{
			name:    "empty name",
			def:     simulation.ScenarioDefinition{Steps: []simulation.ScenarioStep{{EventType: "test"}}},
			wantErr: "name is required",
		},
		{
			name:    "empty steps",
			def:     simulation.ScenarioDefinition{Name: "no-steps"},
			wantErr: "steps must not be empty",
		},
		{
			name: "missing event type in step",
			def: simulation.ScenarioDefinition{
				Name:  "bad-step",
				Steps: []simulation.ScenarioStep{{EventType: ""}},
			},
			wantErr: "event_type is required",
		},
		{
			name: "negative delay",
			def: simulation.ScenarioDefinition{
				Name:  "negative-delay",
				Steps: []simulation.ScenarioStep{{EventType: "test", DelayMs: -100}},
			},
			wantErr: "delay_ms must not be negative",
		},
		{
			name: "condition missing expect_event_type",
			def: simulation.ScenarioDefinition{
				Name: "bad-condition",
				Steps: []simulation.ScenarioStep{{
					EventType: "test",
					Condition: &simulation.StepCondition{TimeoutMs: 1000},
				}},
			},
			wantErr: "expect_event_type is required",
		},
		{
			name: "condition zero timeout",
			def: simulation.ScenarioDefinition{
				Name: "zero-timeout",
				Steps: []simulation.ScenarioStep{{
					EventType: "test",
					Condition: &simulation.StepCondition{ExpectEventType: "foo", TimeoutMs: 0},
				}},
			},
			wantErr: "timeout_ms must be > 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ts.engine.RegisterScenario(&tt.def)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestScenarioStepPayloadJSON verifies that payload_json is correctly passed through
// to the synthetic event on the SIMULATION stream.
func TestScenarioStepPayloadJSON(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	var mu sync.Mutex
	var receivedPayload json.RawMessage
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		if e.Metadata["simulation.synthetic"] == "true" {
			mu.Lock()
			receivedPayload = e.Payload
			mu.Unlock()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	payload := json.RawMessage(`{"workflow_name":"wildfire","zone":"north"}`)
	def := &simulation.ScenarioDefinition{
		Name: "test-payload",
		Steps: []simulation.ScenarioStep{
			{EventType: "workflow.start", PayloadJSON: payload},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	_, err = ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if receivedPayload == nil {
		t.Fatal("no synthetic event received")
	}

	var got map[string]interface{}
	if err := json.Unmarshal(receivedPayload, &got); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if got["workflow_name"] != "wildfire" {
		t.Errorf("expected workflow_name=wildfire, got %v", got["workflow_name"])
	}
}

// TestScenarioDuplicateName verifies that registering a scenario with a duplicate name is rejected.
func TestScenarioDuplicateName(t *testing.T) {
	ts := newScenarioSetup(t)

	def := &simulation.ScenarioDefinition{
		Name:  "unique-name",
		Steps: []simulation.ScenarioStep{{EventType: "test"}},
	}
	_, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("first register: %v", err)
	}

	dup := &simulation.ScenarioDefinition{
		Name:  "unique-name",
		Steps: []simulation.ScenarioStep{{EventType: "test"}},
	}
	_, err = ts.engine.RegisterScenario(dup)
	if err == nil {
		t.Fatal("expected error for duplicate name")
	}
	if !contains(err.Error(), "already exists") {
		t.Errorf("error should mention 'already exists': %v", err)
	}
}

// TestScenarioAPILifecycle verifies the full lifecycle of a scenario via the ScenarioEngine API:
// register, start, list sessions, query status, stop, and verify terminal state.
func TestScenarioAPILifecycle(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Step 1: Register a scenario.
	def := &simulation.ScenarioDefinition{
		Name:        "lifecycle-test",
		Description: "Full lifecycle integration test",
		Steps: []simulation.ScenarioStep{
			{EventType: "workflow.start", DelayMs: 0, SourceAgent: "sensor-001"},
			{EventType: "workflow.step", DelayMs: 500, SourceAgent: "risk-agent"},
			{EventType: "workflow.complete", DelayMs: 500, SourceAgent: "coordinator"},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register scenario: %v", err)
	}
	if registered.ID == "" {
		t.Fatal("registered scenario has no ID")
	}

	// Step 2: List scenarios and verify it appears.
	scenarios := ts.engine.ListScenarios()
	if len(scenarios) == 0 {
		t.Fatal("expected at least one scenario after registration")
	}
	var found bool
	for _, s := range scenarios {
		if s.ID == registered.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("registered scenario %s not found in list", registered.ID)
	}

	// Step 3: Get scenario by ID.
	got := ts.engine.GetScenario(registered.ID)
	if got == nil {
		t.Fatalf("GetScenario(%s) returned nil", registered.ID)
	}
	if got.Name != def.Name {
		t.Errorf("scenario name: got %q, want %q", got.Name, def.Name)
	}

	// Step 4: Start the scenario.
	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start scenario: %v", err)
	}
	if session.State != simulation.ScenarioStateRunning {
		t.Fatalf("expected running, got %s", session.State)
	}
	if session.TotalSteps != 3 {
		t.Errorf("total_steps: got %d, want 3", session.TotalSteps)
	}

	// Step 5: List sessions — should include the new session.
	sessions := ts.engine.ListSessions("")
	var sessionFound bool
	for _, s := range sessions {
		if s.ID == session.ID {
			sessionFound = true
			break
		}
	}
	if !sessionFound {
		t.Fatalf("session %s not found in list", session.ID)
	}

	// Step 6: Filter sessions by state — running.
	runningSessions := ts.engine.ListSessions("running")
	var runningFound bool
	for _, s := range runningSessions {
		if s.ID == session.ID {
			runningFound = true
			break
		}
	}
	if !runningFound {
		t.Fatalf("session %s not found when filtering by 'running'", session.ID)
	}

	// Step 7: Query session status.
	status := ts.engine.GetSession(session.ID)
	if status == nil {
		t.Fatalf("GetSession(%s) returned nil", session.ID)
	}
	if status.ScenarioID != registered.ID {
		t.Errorf("scenario_id: got %q, want %q", status.ScenarioID, registered.ID)
	}

	// Step 8: Stop the session before it completes.
	time.Sleep(200 * time.Millisecond) // let first step execute
	if err := ts.engine.StopSession(session.ID); err != nil {
		t.Fatalf("stop session: %v", err)
	}

	// Step 9: Wait for termination and verify terminal state.
	time.Sleep(400 * time.Millisecond)
	final := ts.engine.GetSession(session.ID)
	if final == nil {
		t.Fatalf("final session %s not found", session.ID)
	}
	if final.State != simulation.ScenarioStateStopped {
		t.Errorf("expected stopped, got %s (error: %s)", final.State, final.ErrorMessage)
	}
	if final.CompletedAt == nil {
		t.Error("expected completed_at to be set on stopped session")
	}
	if final.DurationMs <= 0 {
		t.Error("expected positive duration_ms")
	}

	// Step 10: Verify stopped session appears in state-filtered list.
	stoppedSessions := ts.engine.ListSessions("stopped")
	var stoppedFound bool
	for _, s := range stoppedSessions {
		if s.ID == session.ID {
			stoppedFound = true
			break
		}
	}
	if !stoppedFound {
		t.Fatalf("session %s not found when filtering by 'stopped'", session.ID)
	}

	// Step 11: Attempting to stop again must return a conflict error.
	err = ts.engine.StopSession(session.ID)
	if err == nil {
		t.Error("expected error when stopping already-stopped session")
	}
}

// TestScenarioSSEWatcher verifies that SSE watchers receive events and the terminal notification
// when a scenario session completes, and that late subscribers get the terminal event immediately.
func TestScenarioSSEWatcher(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	def := &simulation.ScenarioDefinition{
		Name: "sse-lifecycle-test",
		Steps: []simulation.ScenarioStep{
			{EventType: "workflow.start", DelayMs: 0},
			{EventType: "workflow.complete", DelayMs: 50},
		},
	}
	registered, err := ts.engine.RegisterScenario(def)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Subscribe before completion.
	ch, cleanup, err := ts.engine.WatchSession(session.ID)
	if err != nil {
		t.Fatalf("WatchSession: %v", err)
	}
	defer cleanup()

	// Collect SSE events.
	var events []simulation.ScenarioEventNotification
	done := make(chan struct{})
	go func() {
		defer close(done)
		for evt := range ch {
			events = append(events, evt)
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SSE watcher channel not closed within 3s")
	}

	// Should have at least one scenario.event and one terminal event (scenario.complete).
	if len(events) < 2 {
		t.Fatalf("expected at least 2 SSE events, got %d", len(events))
	}
	last := events[len(events)-1]
	if last.Name != "scenario.complete" {
		t.Errorf("last SSE event: got %q, want scenario.complete", last.Name)
	}

	// Late subscriber should immediately receive the terminal event.
	lateCh, lateCleanup, err := ts.engine.WatchSession(session.ID)
	if err != nil {
		t.Fatalf("late WatchSession: %v", err)
	}
	defer lateCleanup()

	select {
	case evt, ok := <-lateCh:
		if !ok {
			t.Error("late subscriber channel closed without terminal event")
			break
		}
		if evt.Name != "scenario.complete" {
			t.Errorf("late subscriber: got %q, want scenario.complete", evt.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late subscriber did not receive terminal event within 2s")
	}
}

// TestWildfireScenario loads the bundled wildfire scenario JSON, registers it,
// executes it, and verifies all events are published with correct types and ordering.
func TestWildfireScenario(t *testing.T) {
	ts := newScenarioSetup(t)
	ctx := context.Background()

	// Load the bundled wildfire scenario JSON.
	scenarioJSON, err := scenarioFS.ReadFile("testdata/wildfire-simulation.json")
	if err != nil {
		t.Fatalf("read wildfire scenario: %v", err)
	}

	var def simulation.ScenarioDefinition
	if err := json.Unmarshal(scenarioJSON, &def); err != nil {
		t.Fatalf("unmarshal wildfire scenario: %v", err)
	}

	// Validate the scenario meets minimum requirements.
	if err := def.Validate(); err != nil {
		t.Fatalf("wildfire scenario validation: %v", err)
	}
	if len(def.Steps) < 5 {
		t.Fatalf("wildfire scenario must have at least 5 steps, got %d", len(def.Steps))
	}

	// Register the scenario.
	registered, err := ts.engine.RegisterScenario(&def)
	if err != nil {
		t.Fatalf("register wildfire scenario: %v", err)
	}
	if registered.Name != "wildfire-sierra-nevada" {
		t.Errorf("scenario name: got %q, want %q", registered.Name, "wildfire-sierra-nevada")
	}

	// Collect events from SIMULATION stream.
	var mu sync.Mutex
	var receivedTypes []string
	sub, err := ts.bus.Subscribe(ctx, "simulation.>", func(_ context.Context, e *eventbus.Event) error {
		mu.Lock()
		receivedTypes = append(receivedTypes, e.Type)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe simulation: %v", err)
	}
	defer sub.Unsubscribe()

	// Start the scenario.
	session, err := ts.engine.StartScenario(ctx, registered.ID)
	if err != nil {
		t.Fatalf("start wildfire scenario: %v", err)
	}
	if session.ScenarioName != "wildfire-sierra-nevada" {
		t.Errorf("session scenario_name: got %q, want %q", session.ScenarioName, "wildfire-sierra-nevada")
	}

	// Wait for scenario to complete (7 steps with ~12s total delay, but test uses short delays).
	// The actual delays in the JSON sum to 12s, so we give it enough time.
	deadline := time.After(30 * time.Second)
	for {
		s := ts.engine.GetSession(session.ID)
		if s == nil {
			t.Fatal("get session returned nil")
		}
		if s.State == simulation.ScenarioStateCompleted {
			break
		}
		if s.State == simulation.ScenarioStateFailed {
			t.Fatalf("wildfire scenario failed: %s", s.ErrorMessage)
		}
		select {
		case <-deadline:
			t.Fatal("wildfire scenario did not complete within 30s")
		case <-time.After(200 * time.Millisecond):
		}
	}

	// Allow NATS to deliver remaining messages.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// Verify we received the expected event types in order.
	// The engine publishes: simulation.start, then for each step simulation.inject_event + the synthetic event,
	// then simulation.stop at the end. We check that the synthetic event types appear in order.
	expectedSynthetic := []string{
		"workflow.start",
		"workflow.step",
		"workflow.step",
		"workflow.step",
		"human.decision.request",
		"human.decision.response",
		"workflow.complete",
	}

	// Filter to only synthetic event types (not coordination events).
	var syntheticTypes []string
	for _, et := range receivedTypes {
		switch et {
		case "simulation.start", "simulation.stop", "simulation.inject_event":
			// Coordination event, skip.
		default:
			syntheticTypes = append(syntheticTypes, et)
		}
	}

	if len(syntheticTypes) != len(expectedSynthetic) {
		t.Fatalf("synthetic event count: got %d, want %d\ngot types: %v",
			len(syntheticTypes), len(expectedSynthetic), syntheticTypes)
	}

	for i, want := range expectedSynthetic {
		if syntheticTypes[i] != want {
			t.Errorf("synthetic event[%d]: got %q, want %q", i, syntheticTypes[i], want)
		}
	}

	// Verify distinct event types (at least 5 required by spec).
	distinctTypes := make(map[string]bool)
	for _, et := range syntheticTypes {
		distinctTypes[et] = true
	}
	if len(distinctTypes) < 5 {
		t.Errorf("distinct event types: got %d, want >= 5", len(distinctTypes))
	}

	// Verify final session state.
	final := ts.engine.GetSession(session.ID)
	if final.InjectedEvents != len(expectedSynthetic) {
		t.Errorf("injected_events: got %d, want %d", final.InjectedEvents, len(expectedSynthetic))
	}
	if final.DurationMs <= 0 {
		t.Error("expected positive duration_ms")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
