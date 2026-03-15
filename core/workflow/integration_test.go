package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/workflow"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"google.golang.org/protobuf/proto"
)

// TestE2E_WildfireScenario is a full end-to-end integration test simulating
// the wildfire response scenario: 3 agents (risk-estimation, resource-allocation,
// evacuation-planning) execute a 3-step workflow sequentially.
func TestE2E_WildfireScenario(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	// Register 3 agents with different capabilities.
	agents := map[string]string{
		"agent-risk-e2e":     "risk-estimation",
		"agent-resource-e2e": "resource-allocation",
		"agent-evac-e2e":     "evacuation-planning",
	}
	for id, cap := range agents {
		ts.registerAgent(t, id, cap)
	}
	ts.startEngine(t)

	// Track per-step dispatches.
	type stepCapture struct {
		payload protocolv1.WorkflowStepPayload
		event   *eventbus.Event
	}
	stepCh := make(chan stepCapture, 10)

	for agentID := range agents {
		aid := agentID
		_, _ = ts.bus.Subscribe(ctx, "agent.direct."+aid+".>", func(_ context.Context, evt *eventbus.Event) error {
			var step protocolv1.WorkflowStepPayload
			if err := proto.Unmarshal(evt.Payload, &step); err == nil {
				stepCh <- stepCapture{payload: step, event: evt}
			}
			return nil
		})
	}

	// Subscribe to workflow.complete via a broad pattern.
	var completeWg sync.WaitGroup
	completeWg.Add(1)
	var capturedComplete protocolv1.WorkflowCompletePayload
	var completeOnce sync.Once
	var completedWfID string

	// We'll subscribe to the specific per-workflow complete later, after getting the wfID.

	// Start the workflow.
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "wildfire-response",
		Initiator: "incident-commander",
		Steps: []*protocolv1.StepDefinition{
			{Name: "assess-risk", Capability: "risk-estimation", Input: []byte(`{"zone":"A"}`)},
			{Name: "allocate-resources", Capability: "resource-allocation"},
			{Name: "plan-evacuation", Capability: "evacuation-planning"},
		},
	})

	// Step 0: risk-estimation
	var step0 stepCapture
	select {
	case step0 = <-stepCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for step 0 dispatch")
	}

	wfID := step0.payload.WorkflowId
	if step0.payload.StepIndex != 0 {
		t.Errorf("step 0 index = %d, want 0", step0.payload.StepIndex)
	}
	if step0.payload.Step.Capability != "risk-estimation" {
		t.Errorf("step 0 capability = %q, want risk-estimation", step0.payload.Step.Capability)
	}
	if string(step0.payload.Input) != `{"zone":"A"}` {
		t.Errorf("step 0 input = %q, want zone A", step0.payload.Input)
	}
	if len(step0.payload.PreviousResults) != 0 {
		t.Errorf("step 0 should have 0 previous results, got %d", len(step0.payload.PreviousResults))
	}

	// Subscribe to per-workflow complete now that we have the ID.
	_, _ = ts.bus.Subscribe(ctx, "workflow."+wfID+".workflow.complete", func(_ context.Context, evt *eventbus.Event) error {
		var cp protocolv1.WorkflowCompletePayload
		if err := proto.Unmarshal(evt.Payload, &cp); err == nil {
			completeOnce.Do(func() {
				capturedComplete = cp
				completedWfID = cp.WorkflowId
				completeWg.Done()
			})
		}
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	// Agent completes step 0.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS,
		[]byte(`{"risk_level":"high","affected_area":500}`), nil)

	// Step 1: resource-allocation
	var step1 stepCapture
	select {
	case step1 = <-stepCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for step 1 dispatch")
	}

	if step1.payload.StepIndex != 1 {
		t.Errorf("step 1 index = %d, want 1", step1.payload.StepIndex)
	}
	if step1.payload.Step.Capability != "resource-allocation" {
		t.Errorf("step 1 capability = %q, want resource-allocation", step1.payload.Step.Capability)
	}
	if len(step1.payload.PreviousResults) != 1 {
		t.Errorf("step 1 should have 1 previous result, got %d", len(step1.payload.PreviousResults))
	}

	// Agent completes step 1.
	publishStepResult(t, ts, wfID, 1, protocolv1.StepStatus_STEP_STATUS_SUCCESS,
		[]byte(`{"units_assigned":12,"helicopters":2}`), nil)

	// Step 2: evacuation-planning
	var step2 stepCapture
	select {
	case step2 = <-stepCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for step 2 dispatch")
	}

	if step2.payload.StepIndex != 2 {
		t.Errorf("step 2 index = %d, want 2", step2.payload.StepIndex)
	}
	if step2.payload.Step.Capability != "evacuation-planning" {
		t.Errorf("step 2 capability = %q, want evacuation-planning", step2.payload.Step.Capability)
	}
	if len(step2.payload.PreviousResults) != 2 {
		t.Errorf("step 2 should have 2 previous results, got %d", len(step2.payload.PreviousResults))
	}

	// Agent completes step 2 (final).
	publishStepResult(t, ts, wfID, 2, protocolv1.StepStatus_STEP_STATUS_SUCCESS,
		[]byte(`{"routes":["north","east"],"shelters":3}`), nil)

	// Wait for workflow.complete.
	waitDoneWithTimeout(t, &completeWg, 5*time.Second, "workflow.complete")

	// Validate the complete payload.
	if completedWfID != wfID {
		t.Errorf("completed workflow_id = %q, want %q", completedWfID, wfID)
	}
	if len(capturedComplete.Results) != 3 {
		t.Fatalf("complete results = %d, want 3", len(capturedComplete.Results))
	}
	for i, r := range capturedComplete.Results {
		if r.StepIndex != uint32(i) {
			t.Errorf("result[%d] step_index = %d, want %d", i, r.StepIndex, i)
		}
		if r.Status != protocolv1.StepStatus_STEP_STATUS_SUCCESS {
			t.Errorf("result[%d] status = %v, want SUCCESS", i, r.Status)
		}
	}
	if capturedComplete.StartedAt == 0 {
		t.Error("complete started_at should be non-zero")
	}
	if capturedComplete.CompletedAt == 0 {
		t.Error("complete completed_at should be non-zero")
	}
	if capturedComplete.CompletedAt <= capturedComplete.StartedAt {
		t.Error("completed_at should be after started_at")
	}

	// Verify final KV state.
	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusCompleted {
		t.Errorf("final status = %v, want COMPLETED", state.Status)
	}
	if len(state.StepResults) != 3 {
		t.Errorf("state step_results = %d, want 3", len(state.StepResults))
	}

	// Query state to validate US5 on completed workflow.
	var respWg sync.WaitGroup
	respWg.Add(1)
	var stateResp protocolv1.WorkflowStateResponsePayload
	var respOnce sync.Once

	_, _ = ts.bus.Subscribe(ctx, "workflow.state.response", func(_ context.Context, evt *eventbus.Event) error {
		var resp protocolv1.WorkflowStateResponsePayload
		if err := proto.Unmarshal(evt.Payload, &resp); err == nil {
			if resp.WorkflowId == wfID {
				respOnce.Do(func() { stateResp = resp; respWg.Done() })
			}
		}
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	reqData, _ := proto.Marshal(&protocolv1.WorkflowStateRequestPayload{WorkflowId: wfID})
	_ = ts.bus.Publish(ctx, &eventbus.Event{
		ID:            "state-query-e2e",
		Type:          "workflow.state.request",
		CorrelationID: "corr-e2e",
		Timestamp:     time.Now().UnixNano(),
		Payload:       reqData,
	})

	waitDoneWithTimeout(t, &respWg, 5*time.Second, "state response for completed workflow")

	if stateResp.Status != protocolv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Errorf("state query status = %v, want COMPLETED", stateResp.Status)
	}
	if len(stateResp.StepResults) != 3 {
		t.Errorf("state query step_results = %d, want 3", len(stateResp.StepResults))
	}
}
