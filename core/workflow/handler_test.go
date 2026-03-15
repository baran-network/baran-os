package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ad-hok/agent-os/core/eventbus"
	natseventbus "github.com/ad-hok/agent-os/core/eventbus/nats"
	"github.com/ad-hok/agent-os/core/registry"
	"github.com/ad-hok/agent-os/core/testutil"
	"github.com/ad-hok/agent-os/core/workflow"
	protocolv1 "github.com/ad-hok/agent-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// testSetup creates all components wired to a single real embedded NATS server.
type testSetup struct {
	bus    *natseventbus.Bus
	store  workflow.WorkflowStateStore
	reg    registry.AgentRegistry
	engine *workflow.WorkflowEngine
}

func newTestSetup(t *testing.T, defaultTimeout time.Duration) *testSetup {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("NewFromConn: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		t.Fatalf("NewKVWorkflowStateStore: %v", err)
	}

	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("NewKVRegistry: %v", err)
	}

	engine := workflow.NewWorkflowEngine(bus, store, reg, "test-node", defaultTimeout)
	return &testSetup{bus: bus, store: store, reg: reg, engine: engine}
}

func (ts *testSetup) startEngine(t *testing.T) []eventbus.Subscription {
	t.Helper()
	subs, err := ts.engine.Start(context.Background())
	if err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})
	time.Sleep(200 * time.Millisecond) // let subscriptions initialize
	return subs
}

func (ts *testSetup) registerAgent(t *testing.T, agentID, capability string) {
	t.Helper()
	_, err := ts.reg.Register(context.Background(), registry.AgentRegistration{
		AgentID:   agentID,
		AgentType: "test-agent",
		Version:   "1.0.0",
		Capabilities: []registry.Capability{
			{Name: capability, Version: "1.0.0"},
		},
	})
	if err != nil {
		t.Fatalf("register agent %s: %v", agentID, err)
	}
}

func waitDoneWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for %s", msg)
	}
}

func publishWorkflowStart(t *testing.T, bus *natseventbus.Bus, def *protocolv1.WorkflowDefinition) {
	t.Helper()
	startPayload := &protocolv1.WorkflowStartPayload{Definition: def}
	data, _ := proto.Marshal(startPayload)
	if err := bus.Publish(context.Background(), &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish workflow.start: %v", err)
	}
}

// TestUS1_WorkflowStart verifies that workflow.start creates KV state (RUNNING)
// and dispatches workflow.step to the resolved agent.
func TestUS1_WorkflowStart(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agentID = "agent-risk-001"
	ts.registerAgent(t, agentID, "risk-estimation")
	ts.startEngine(t)

	// Subscribe to the agent's direct subject to capture workflow.step dispatch.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var capturedStep protocolv1.WorkflowStepPayload
	var capturedOnce sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			capturedOnce.Do(func() {
				capturedStep = step
				stepWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent direct: %v", err)
	}

	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "risk-response",
		Initiator: "test",
		Steps:     []*protocolv1.StepDefinition{{Name: "step-0", Capability: "risk-estimation"}},
	})

	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "workflow.step dispatch")

	// Validate dispatched step.
	if capturedStep.WorkflowId == "" {
		t.Error("expected non-empty WorkflowId in step payload")
	}
	if capturedStep.StepIndex != 0 {
		t.Errorf("StepIndex = %d, want 0", capturedStep.StepIndex)
	}
	if capturedStep.Step == nil || capturedStep.Step.Capability != "risk-estimation" {
		t.Errorf("expected capability 'risk-estimation', got: %v", capturedStep.Step)
	}

	// Verify KV state is RUNNING.
	wfState, _, err := ts.store.Get(ctx, capturedStep.WorkflowId)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if wfState.Status != workflow.StatusRunning {
		t.Errorf("workflow status = %v, want Running", wfState.Status)
	}
	if wfState.CurrentStep != 0 {
		t.Errorf("current_step = %d, want 0", wfState.CurrentStep)
	}
}

// TestUS1_WorkflowStart_InvalidDefinition verifies that invalid definitions produce agent.error.
func TestUS1_WorkflowStart_InvalidDefinition(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()
	ts.startEngine(t)

	var errorWg sync.WaitGroup
	errorWg.Add(1)
	var capturedError protocolv1.AgentErrorPayload
	var capturedOnce sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.error", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.AgentErrorPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			capturedOnce.Do(func() {
				capturedError = payload
				errorWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent.error: %v", err)
	}

	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "bad-workflow",
		Initiator: "test",
		Steps:     []*protocolv1.StepDefinition{}, // empty — invalid
	})

	waitDoneWithTimeout(t, &errorWg, 5*time.Second, "agent.error for invalid definition")

	if capturedError.ErrorCode != "INVALID_DEFINITION" {
		t.Errorf("error code = %q, want INVALID_DEFINITION", capturedError.ErrorCode)
	}
}

// TestUS5_WorkflowStateRequest verifies state query for a running workflow.
func TestUS5_WorkflowStateRequest(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agentID = "agent-evacuation-001"
	ts.registerAgent(t, agentID, "evacuation-planning")
	ts.startEngine(t)

	// Subscribe to agent direct to capture the workflow ID.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var wfID string
	var once sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			once.Do(func() { wfID = step.WorkflowId; stepWg.Done() })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:  "evacuation",
		Steps: []*protocolv1.StepDefinition{{Capability: "evacuation-planning"}},
	})
	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "workflow.step dispatch")

	// Now query the state.
	var respWg sync.WaitGroup
	respWg.Add(1)
	var capturedResp protocolv1.WorkflowStateResponsePayload
	var respOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "workflow.state.response", func(_ context.Context, evt *eventbus.Event) error {
		var resp protocolv1.WorkflowStateResponsePayload
		if err := proto.Unmarshal(evt.Payload, &resp); err == nil {
			respOnce.Do(func() { capturedResp = resp; respWg.Done() })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe state.response: %v", err)
	}

	time.Sleep(100 * time.Millisecond) // let subscription init

	reqPayload := &protocolv1.WorkflowStateRequestPayload{WorkflowId: wfID}
	data, _ := proto.Marshal(reqPayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "workflow.state.request",
		CorrelationID: "corr-001",
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	}); err != nil {
		t.Fatalf("publish state.request: %v", err)
	}

	waitDoneWithTimeout(t, &respWg, 5*time.Second, "workflow.state.response")

	if capturedResp.WorkflowId != wfID {
		t.Errorf("workflow_id = %q, want %q", capturedResp.WorkflowId, wfID)
	}
	if capturedResp.Status != protocolv1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
		t.Errorf("status = %v, want RUNNING", capturedResp.Status)
	}
	if capturedResp.CurrentStep != 0 {
		t.Errorf("current_step = %d, want 0", capturedResp.CurrentStep)
	}
}

// TestUS5_WorkflowStateRequest_NotFound verifies that a bad workflow ID returns agent.error.
func TestUS5_WorkflowStateRequest_NotFound(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()
	ts.startEngine(t)

	var errWg sync.WaitGroup
	errWg.Add(1)
	var capturedErr protocolv1.AgentErrorPayload
	var once sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.error", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.AgentErrorPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			once.Do(func() { capturedErr = payload; errWg.Done() })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	reqPayload := &protocolv1.WorkflowStateRequestPayload{WorkflowId: "nonexistent-id"}
	data, _ := proto.Marshal(reqPayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.state.request",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitDoneWithTimeout(t, &errWg, 5*time.Second, "agent.error for not-found")

	if capturedErr.ErrorCode != "WORKFLOW_NOT_FOUND" {
		t.Errorf("error code = %q, want WORKFLOW_NOT_FOUND", capturedErr.ErrorCode)
	}
}
