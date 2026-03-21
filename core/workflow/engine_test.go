package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/workflow"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// helper: start a workflow and wait for the first step dispatch, returning the workflow ID.
func startWorkflowAndCaptureID(t *testing.T, ts *testSetup, agentID string, def *protocolv1.WorkflowDefinition) string {
	t.Helper()
	ctx := context.Background()

	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var wfID string
	var once sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			once.Do(func() {
				wfID = step.WorkflowId
				stepWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent direct: %v", err)
	}

	publishWorkflowStart(t, ts.bus, def)
	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "workflow.step dispatch for "+agentID)
	return wfID
}

// helper: publish a step result for a given workflow.
func publishStepResult(t *testing.T, ts *testSetup, workflowID string, stepIndex uint32, status protocolv1.StepStatus, result []byte, stepErr *protocolv1.WorkflowError) {
	t.Helper()
	payload := &protocolv1.WorkflowStepResultPayload{
		StepIndex: stepIndex,
		Status:    status,
		Result:    result,
		Error:     stepErr,
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal step result: %v", err)
	}

	subject := "workflow." + workflowID + ".workflow.step.result"
	if err := ts.bus.Publish(context.Background(), &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        subject,
		SourceAgent: "test-agent",
		WorkflowID:  workflowID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}); err != nil {
		t.Fatalf("publish step result: %v", err)
	}
}

// TestUS2_WorkflowStepCompletion verifies that a 2-step workflow completes
// when both steps return SUCCESS results.
func TestUS2_WorkflowStepCompletion(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agent1 = "agent-risk-001"
	const agent2 = "agent-resource-001"
	ts.registerAgent(t, agent1, "risk-estimation")
	ts.registerAgent(t, agent2, "resource-allocation")
	ts.startEngine(t)

	// Subscribe to workflow.complete (via the per-workflow stream — use wildcard on AGENTS).
	var completeWg sync.WaitGroup
	completeWg.Add(1)
	var capturedComplete *protocolv1.WorkflowCompletePayload
	var completeOnce sync.Once

	// Also capture step 1 dispatch to agent2.
	var step1Wg sync.WaitGroup
	step1Wg.Add(1)
	var capturedStep1 *protocolv1.WorkflowStepPayload
	var step1Once sync.Once

	_, err := ts.bus.Subscribe(ctx, "agent.direct."+agent2+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step1Once.Do(func() {
				capturedStep1 = proto.Clone(&step).(*protocolv1.WorkflowStepPayload)
				step1Wg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent2 direct: %v", err)
	}

	// Start a 2-step workflow.
	wfID := startWorkflowAndCaptureID(t, ts, agent1, &protocolv1.WorkflowDefinition{
		Name:      "risk-response",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "risk", Capability: "risk-estimation"},
			{Name: "resource", Capability: "resource-allocation"},
		},
	})

	// Subscribe to workflow complete events on the per-workflow stream.
	completeSubject := "workflow." + wfID + ".workflow.complete"
	_, err = ts.bus.Subscribe(ctx, completeSubject, func(_ context.Context, evt *eventbus.Event) error {
		var cp protocolv1.WorkflowCompletePayload
		if err := proto.Unmarshal(evt.Payload, &cp); err == nil {
			completeOnce.Do(func() {
				capturedComplete = proto.Clone(&cp).(*protocolv1.WorkflowCompletePayload)
				completeWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.complete: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let subscriptions init

	// Emit step 0 SUCCESS.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("risk-ok"), nil)

	// Wait for step 1 to be dispatched to agent2.
	waitDoneWithTimeout(t, &step1Wg, 5*time.Second, "step 1 dispatch to agent2")

	if capturedStep1.StepIndex != 1 {
		t.Errorf("step1 StepIndex = %d, want 1", capturedStep1.StepIndex)
	}
	if capturedStep1.Step.Capability != "resource-allocation" {
		t.Errorf("step1 capability = %q, want resource-allocation", capturedStep1.Step.Capability)
	}

	// Emit step 1 SUCCESS.
	publishStepResult(t, ts, wfID, 1, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("resource-ok"), nil)

	// Wait for workflow.complete.
	waitDoneWithTimeout(t, &completeWg, 5*time.Second, "workflow.complete")

	if capturedComplete.WorkflowId != wfID {
		t.Errorf("complete workflow_id = %q, want %q", capturedComplete.WorkflowId, wfID)
	}
	if len(capturedComplete.Results) != 2 {
		t.Fatalf("complete results count = %d, want 2", len(capturedComplete.Results))
	}
	if capturedComplete.Results[0].StepIndex != 0 {
		t.Errorf("result[0] step = %d, want 0", capturedComplete.Results[0].StepIndex)
	}
	if capturedComplete.Results[1].StepIndex != 1 {
		t.Errorf("result[1] step = %d, want 1", capturedComplete.Results[1].StepIndex)
	}

	// Verify KV state is COMPLETED.
	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusCompleted {
		t.Errorf("workflow status = %v, want COMPLETED", state.Status)
	}
}

// TestUS3_WorkflowStepFailure verifies that a step FAILURE result transitions
// the workflow to FAILED and publishes workflow.failed.
func TestUS3_WorkflowStepFailure(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agentID = "agent-risk-002"
	ts.registerAgent(t, agentID, "risk-estimation")
	ts.startEngine(t)

	// Subscribe to workflow.failed via wildcard.
	var failedWg sync.WaitGroup
	failedWg.Add(1)
	var capturedFailed *protocolv1.WorkflowFailedPayload
	var failedOnce sync.Once

	// Start a 2-step workflow (second step should NOT be dispatched).
	wfID := startWorkflowAndCaptureID(t, ts, agentID, &protocolv1.WorkflowDefinition{
		Name:      "failing-workflow",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "risk", Capability: "risk-estimation"},
			{Name: "risk2", Capability: "risk-estimation"},
		},
	})

	// Subscribe to workflow.failed on per-workflow stream.
	failedSubject := "workflow." + wfID + ".workflow.failed"
	_, err := ts.bus.Subscribe(ctx, failedSubject, func(_ context.Context, evt *eventbus.Event) error {
		var fp protocolv1.WorkflowFailedPayload
		if err := proto.Unmarshal(evt.Payload, &fp); err == nil {
			failedOnce.Do(func() {
				capturedFailed = proto.Clone(&fp).(*protocolv1.WorkflowFailedPayload)
				failedWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.failed: %v", err)
	}

	// Track if a second step is dispatched (it shouldn't be).
	var extraStepReceived bool
	var extraMu sync.Mutex
	_, _ = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil && step.WorkflowId == wfID && step.StepIndex == 1 {
			extraMu.Lock()
			extraStepReceived = true
			extraMu.Unlock()
		}
		return nil
	})
	time.Sleep(100 * time.Millisecond) // let subscriptions init

	// Emit step 0 FAILURE.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_FAILURE, nil, &protocolv1.WorkflowError{
		Code:      "STEP_FAILED",
		Message:   "risk estimation failed",
		StepIndex: 0,
		AgentId:   agentID,
	})

	// Wait for workflow.failed.
	waitDoneWithTimeout(t, &failedWg, 5*time.Second, "workflow.failed")

	if capturedFailed.WorkflowId != wfID {
		t.Errorf("failed workflow_id = %q, want %q", capturedFailed.WorkflowId, wfID)
	}
	if capturedFailed.FailedStep != 0 {
		t.Errorf("failed_step = %d, want 0", capturedFailed.FailedStep)
	}
	if capturedFailed.Error == nil {
		t.Fatal("expected error in WorkflowFailedPayload")
	}
	if capturedFailed.Error.Code != "STEP_FAILED" {
		t.Errorf("error code = %q, want STEP_FAILED", capturedFailed.Error.Code)
	}

	// Verify KV state is FAILED.
	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusFailed {
		t.Errorf("workflow status = %v, want FAILED", state.Status)
	}

	// Verify no additional step was dispatched.
	time.Sleep(300 * time.Millisecond)
	extraMu.Lock()
	if extraStepReceived {
		t.Error("step 1 was dispatched after step 0 failure — should not happen")
	}
	extraMu.Unlock()
}

// TestUS6_PreviousResultsForwarding verifies that each step dispatch includes
// accumulated previous_results from completed steps.
func TestUS6_PreviousResultsForwarding(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agent1 = "agent-risk-003"
	const agent2 = "agent-resource-003"
	const agent3 = "agent-evac-003"
	ts.registerAgent(t, agent1, "risk-estimation")
	ts.registerAgent(t, agent2, "resource-allocation")
	ts.registerAgent(t, agent3, "evacuation-planning")
	ts.startEngine(t)

	// Capture step dispatches to agent2 and agent3.
	var step1Wg, step2Wg sync.WaitGroup
	step1Wg.Add(1)
	step2Wg.Add(1)
	var capturedStep1, capturedStep2 *protocolv1.WorkflowStepPayload
	var step1Once, step2Once sync.Once

	_, _ = ts.bus.Subscribe(ctx, "agent.direct."+agent2+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step1Once.Do(func() {
				capturedStep1 = proto.Clone(&step).(*protocolv1.WorkflowStepPayload)
				step1Wg.Done()
			})
		}
		return nil
	})
	_, _ = ts.bus.Subscribe(ctx, "agent.direct."+agent3+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step2Once.Do(func() {
				capturedStep2 = proto.Clone(&step).(*protocolv1.WorkflowStepPayload)
				step2Wg.Done()
			})
		}
		return nil
	})

	// Start a 3-step workflow.
	wfID := startWorkflowAndCaptureID(t, ts, agent1, &protocolv1.WorkflowDefinition{
		Name:      "wildfire-response",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "risk", Capability: "risk-estimation", Input: []byte("zone-a")},
			{Name: "resource", Capability: "resource-allocation"},
			{Name: "evacuation", Capability: "evacuation-planning"},
		},
	})

	// Step 0 SUCCESS with result data.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("risk-result-0"), nil)
	waitDoneWithTimeout(t, &step1Wg, 5*time.Second, "step 1 dispatch")

	// Verify step 1 has 1 previous result.
	if len(capturedStep1.PreviousResults) != 1 {
		t.Fatalf("step1 previous_results count = %d, want 1", len(capturedStep1.PreviousResults))
	}
	if capturedStep1.PreviousResults[0].StepIndex != 0 {
		t.Errorf("step1 prev[0] step_index = %d, want 0", capturedStep1.PreviousResults[0].StepIndex)
	}
	if string(capturedStep1.PreviousResults[0].Result) != "risk-result-0" {
		t.Errorf("step1 prev[0] result = %q, want 'risk-result-0'", capturedStep1.PreviousResults[0].Result)
	}

	// Step 1 SUCCESS with result data.
	publishStepResult(t, ts, wfID, 1, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("resource-result-1"), nil)
	waitDoneWithTimeout(t, &step2Wg, 5*time.Second, "step 2 dispatch")

	// Verify step 2 has 2 previous results.
	if len(capturedStep2.PreviousResults) != 2 {
		t.Fatalf("step2 previous_results count = %d, want 2", len(capturedStep2.PreviousResults))
	}
	if capturedStep2.PreviousResults[0].StepIndex != 0 {
		t.Errorf("step2 prev[0] step_index = %d, want 0", capturedStep2.PreviousResults[0].StepIndex)
	}
	if capturedStep2.PreviousResults[1].StepIndex != 1 {
		t.Errorf("step2 prev[1] step_index = %d, want 1", capturedStep2.PreviousResults[1].StepIndex)
	}
	if string(capturedStep2.PreviousResults[1].Result) != "resource-result-1" {
		t.Errorf("step2 prev[1] result = %q, want 'resource-result-1'", capturedStep2.PreviousResults[1].Result)
	}
}

// TestEdgeCase_LateResultOnCompletedWorkflow verifies that a step result
// received after workflow completion is a no-op.
func TestEdgeCase_LateResultOnCompletedWorkflow(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agentID = "agent-edge-001"
	ts.registerAgent(t, agentID, "risk-estimation")
	ts.startEngine(t)

	// Start a 1-step workflow.
	wfID := startWorkflowAndCaptureID(t, ts, agentID, &protocolv1.WorkflowDefinition{
		Name:  "edge-complete",
		Steps: []*protocolv1.StepDefinition{{Capability: "risk-estimation"}},
	})

	// Complete it.
	var completeWg sync.WaitGroup
	completeWg.Add(1)
	var completeOnce sync.Once
	_, _ = ts.bus.Subscribe(ctx, "workflow."+wfID+".workflow.complete", func(_ context.Context, evt *eventbus.Event) error {
		completeOnce.Do(func() { completeWg.Done() })
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("done"), nil)
	waitDoneWithTimeout(t, &completeWg, 5*time.Second, "workflow.complete")

	// Now send a late result — should be a no-op.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("late"), nil)
	time.Sleep(300 * time.Millisecond)

	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusCompleted {
		t.Errorf("status = %v, want COMPLETED after late result", state.Status)
	}
}

// TestEdgeCase_LateResultOnFailedWorkflow verifies that a step result
// received after workflow failure is a no-op.
func TestEdgeCase_LateResultOnFailedWorkflow(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agentID = "agent-edge-002"
	ts.registerAgent(t, agentID, "risk-estimation")
	ts.startEngine(t)

	wfID := startWorkflowAndCaptureID(t, ts, agentID, &protocolv1.WorkflowDefinition{
		Name:  "edge-fail",
		Steps: []*protocolv1.StepDefinition{{Capability: "risk-estimation"}, {Capability: "risk-estimation"}},
	})

	// Fail it.
	var failedWg sync.WaitGroup
	failedWg.Add(1)
	var failedOnce sync.Once
	_, _ = ts.bus.Subscribe(ctx, "workflow."+wfID+".workflow.failed", func(_ context.Context, evt *eventbus.Event) error {
		failedOnce.Do(func() { failedWg.Done() })
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_FAILURE, nil, &protocolv1.WorkflowError{
		Code: "STEP_FAILED", Message: "fail",
	})
	waitDoneWithTimeout(t, &failedWg, 5*time.Second, "workflow.failed")

	// Late result on failed workflow — no-op.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("late"), nil)
	time.Sleep(300 * time.Millisecond)

	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusFailed {
		t.Errorf("status = %v, want FAILED after late result", state.Status)
	}
}

// TestEdgeCase_DuplicateStepResult verifies that duplicate step result delivery
// (CAS conflict) is handled as a no-op.
func TestEdgeCase_DuplicateStepResult(t *testing.T) {
	ts := newTestSetup(t, 5*time.Second)
	ctx := context.Background()

	const agent1 = "agent-edge-003"
	const agent2 = "agent-edge-003b"
	ts.registerAgent(t, agent1, "risk-estimation")
	ts.registerAgent(t, agent2, "resource-allocation")
	ts.startEngine(t)

	// 2-step workflow.
	var step1Wg sync.WaitGroup
	step1Wg.Add(1)
	var step1Once sync.Once

	_, _ = ts.bus.Subscribe(ctx, "agent.direct."+agent2+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step1Once.Do(func() { step1Wg.Done() })
		}
		return nil
	})

	wfID := startWorkflowAndCaptureID(t, ts, agent1, &protocolv1.WorkflowDefinition{
		Name: "edge-dup",
		Steps: []*protocolv1.StepDefinition{
			{Capability: "risk-estimation"},
			{Capability: "resource-allocation"},
		},
	})

	// Send step 0 SUCCESS twice rapidly.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("ok"), nil)
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("ok-dup"), nil)

	// Should still advance to step 1 exactly once.
	waitDoneWithTimeout(t, &step1Wg, 5*time.Second, "step 1 dispatch")

	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.CurrentStep != 1 {
		t.Errorf("current_step = %d, want 1", state.CurrentStep)
	}
	if state.Status != workflow.StatusRunning {
		t.Errorf("status = %v, want RUNNING", state.Status)
	}
}

// TestConcurrency_MultipleWorkflows verifies that 10 concurrent workflows
// complete independently without state cross-contamination.
func TestConcurrency_MultipleWorkflows(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	const numWorkflows = 10

	// Register agents for each capability.
	for i := 0; i < numWorkflows; i++ {
		agentID := uuid.Must(uuid.NewV7()).String()
		ts.registerAgent(t, agentID, "cap-concurrent")
	}
	ts.startEngine(t)

	// Capture all step dispatches.
	type stepDispatch struct {
		workflowID string
		stepIndex  uint32
	}
	dispatches := make(chan stepDispatch, numWorkflows*2)

	_, _ = ts.bus.Subscribe(ctx, "agent.direct.>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			dispatches <- stepDispatch{workflowID: step.WorkflowId, stepIndex: step.StepIndex}
		}
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	// Start all workflows concurrently.
	var startWg sync.WaitGroup
	for i := 0; i < numWorkflows; i++ {
		startWg.Add(1)
		go func() {
			defer startWg.Done()
			publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
				Name:  "concurrent-wf",
				Steps: []*protocolv1.StepDefinition{{Capability: "cap-concurrent"}},
			})
		}()
	}
	startWg.Wait()

	// Collect workflow IDs from step dispatches.
	wfIDs := make(map[string]bool)
	timeout := time.After(10 * time.Second)
	for len(wfIDs) < numWorkflows {
		select {
		case d := <-dispatches:
			wfIDs[d.workflowID] = true
		case <-timeout:
			t.Fatalf("timeout: only received %d/%d workflow step dispatches", len(wfIDs), numWorkflows)
		}
	}

	// Complete all workflows.
	for wfID := range wfIDs {
		publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("ok"), nil)
	}

	// Wait and verify all completed.
	time.Sleep(2 * time.Second)
	for wfID := range wfIDs {
		state, _, err := ts.store.Get(ctx, wfID)
		if err != nil {
			t.Errorf("store.Get(%s): %v", wfID, err)
			continue
		}
		if state.Status != workflow.StatusCompleted {
			t.Errorf("workflow %s status = %v, want COMPLETED", wfID, state.Status)
		}
	}
}
