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

// TestHumanApproval_Approve verifies the full approve path:
// 3-step workflow with step 1 as human_approval.
// Step 0 dispatches to agent, step 1 pauses for human, step 2 dispatches to agent.
func TestHumanApproval_Approve(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	const agentID = "agent-evac-001"
	ts.registerAgent(t, agentID, "evacuation-planning")

	ts.startEngine(t)

	// Track workflow.step dispatches to the agent.
	var stepMu sync.Mutex
	var capturedSteps []protocolv1.WorkflowStepPayload
	var stepWg sync.WaitGroup
	stepWg.Add(1) // wait for first agent step

	_, err := ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			stepMu.Lock()
			capturedSteps = append(capturedSteps, step)
			count := len(capturedSteps)
			stepMu.Unlock()
			if count == 1 {
				stepWg.Done()
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent direct: %v", err)
	}

	// Track human.decision.request events.
	var decisionReqWg sync.WaitGroup
	decisionReqWg.Add(1)
	var capturedDecisionReq protocolv1.HumanDecisionRequestPayload
	var decisionOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		var req protocolv1.HumanDecisionRequestPayload
		if err := proto.Unmarshal(evt.Payload, &req); err == nil {
			decisionOnce.Do(func() {
				capturedDecisionReq = req
				decisionReqWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe human.decision.request: %v", err)
	}

	// Start workflow: step 0 (agent), step 1 (human), step 2 (agent).
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "evac-with-approval",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "assess-risk", Capability: "evacuation-planning"},
			{Name: "approve-evac", HumanApproval: true, Prompt: "Approve evacuation plan?", ResourceIds: []string{"zone-a"}},
			{Name: "execute-evac", Capability: "evacuation-planning"},
		},
	})

	// Wait for step 0 to be dispatched to agent.
	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "step 0 dispatch")

	stepMu.Lock()
	wfID := capturedSteps[0].WorkflowId
	stepMu.Unlock()

	// Now that we have the workflow ID, subscribe to workflow.complete on the per-workflow stream.
	var completeWg sync.WaitGroup
	completeWg.Add(1)
	var capturedComplete protocolv1.WorkflowCompletePayload
	var completeOnce sync.Once

	completeSubject := "workflow." + wfID + ".workflow.complete"
	_, err = ts.bus.Subscribe(ctx, completeSubject, func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.WorkflowCompletePayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			completeOnce.Do(func() {
				capturedComplete = payload
				completeWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.complete: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Simulate agent completing step 0.
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("risk-data"), nil)

	// Wait for human.decision.request.
	waitDoneWithTimeout(t, &decisionReqWg, 5*time.Second, "human.decision.request")

	// Verify decision request content.
	if capturedDecisionReq.WorkflowId != wfID {
		t.Errorf("decision request workflow_id = %q, want %q", capturedDecisionReq.WorkflowId, wfID)
	}
	if capturedDecisionReq.StepIndex != 1 {
		t.Errorf("decision request step_index = %d, want 1", capturedDecisionReq.StepIndex)
	}
	if capturedDecisionReq.Prompt != "Approve evacuation plan?" {
		t.Errorf("decision request prompt = %q, want %q", capturedDecisionReq.Prompt, "Approve evacuation plan?")
	}
	if len(capturedDecisionReq.ResourceIds) != 1 || capturedDecisionReq.ResourceIds[0] != "zone-a" {
		t.Errorf("decision request resource_ids = %v, want [zone-a]", capturedDecisionReq.ResourceIds)
	}
	if len(capturedDecisionReq.PreviousResults) != 1 {
		t.Errorf("decision request previous_results len = %d, want 1", len(capturedDecisionReq.PreviousResults))
	} else {
		pr := capturedDecisionReq.PreviousResults[0]
		if pr.StepIndex != 0 {
			t.Errorf("previous_results[0].step_index = %d, want 0", pr.StepIndex)
		}
		if string(pr.Result) != "risk-data" {
			t.Errorf("previous_results[0].result = %q, want %q", string(pr.Result), "risk-data")
		}
	}

	// Verify ListPending returns the pending decision.
	pendingList := ts.engine.Coordinator().ListPending()
	if len(pendingList) != 1 {
		t.Fatalf("ListPending() len = %d, want 1", len(pendingList))
	}
	if pendingList[0].WorkflowID != wfID {
		t.Errorf("ListPending()[0].WorkflowID = %q, want %q", pendingList[0].WorkflowID, wfID)
	}
	if pendingList[0].StepName != "approve-evac" {
		t.Errorf("ListPending()[0].StepName = %q, want %q", pendingList[0].StepName, "approve-evac")
	}
	if pendingList[0].Prompt != "Approve evacuation plan?" {
		t.Errorf("ListPending()[0].Prompt = %q, want %q", pendingList[0].Prompt, "Approve evacuation plan?")
	}

	// Verify GetPending returns the same decision by ID.
	got := ts.engine.Coordinator().GetPending(capturedDecisionReq.DecisionId)
	if got == nil {
		t.Fatal("GetPending returned nil for known decision_id")
	}
	if got.DecisionID != capturedDecisionReq.DecisionId {
		t.Errorf("GetPending().DecisionID = %q, want %q", got.DecisionID, capturedDecisionReq.DecisionId)
	}

	// Verify workflow is in WAITING_HUMAN state.
	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusWaitingHuman {
		t.Errorf("workflow status = %v, want StatusWaitingHuman", state.Status)
	}
	if state.PendingDecision == nil {
		t.Fatal("expected PendingDecision to be set")
	}

	// Approve the decision.
	decisionID := capturedDecisionReq.DecisionId
	approvePayload := &protocolv1.HumanDecisionResponsePayload{
		DecisionId:  decisionID,
		WorkflowId:  wfID,
		Action:      protocolv1.DecisionAction_DECISION_ACTION_APPROVE,
		OperatorId:  "operator-1",
		Comment:     "Looks good",
		RespondedAt: time.Now().UnixNano(),
	}
	data, _ := proto.Marshal(approvePayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "human.decision.response",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish human.decision.response: %v", err)
	}

	// Wait for step 2 dispatch to agent (resume path).
	time.Sleep(500 * time.Millisecond)

	// Simulate agent completing step 2.
	publishStepResult(t, ts, wfID, 2, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("evac-done"), nil)

	// Wait for workflow.complete.
	waitDoneWithTimeout(t, &completeWg, 5*time.Second, "workflow.complete")

	if capturedComplete.WorkflowId != wfID {
		t.Errorf("complete workflow_id = %q, want %q", capturedComplete.WorkflowId, wfID)
	}
	if len(capturedComplete.Results) != 3 {
		t.Errorf("complete results len = %d, want 3", len(capturedComplete.Results))
	}

	// Verify final state is COMPLETED.
	finalState, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get final: %v", err)
	}
	if finalState.Status != workflow.StatusCompleted {
		t.Errorf("final status = %v, want StatusCompleted", finalState.Status)
	}

	// Verify ListPending is empty after decision was processed.
	if remaining := ts.engine.Coordinator().ListPending(); len(remaining) != 0 {
		t.Errorf("ListPending() after approval len = %d, want 0", len(remaining))
	}
}

// TestHumanApproval_Reject verifies the reject path:
// workflow transitions to FAILED with rejection reason.
func TestHumanApproval_Reject(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	const agentID = "agent-evac-002"
	ts.registerAgent(t, agentID, "resource-allocation")

	ts.startEngine(t)

	// Track human.decision.request.
	var decisionReqWg sync.WaitGroup
	decisionReqWg.Add(1)
	var capturedDecisionReq protocolv1.HumanDecisionRequestPayload
	var decisionOnce sync.Once

	_, err := ts.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		var req protocolv1.HumanDecisionRequestPayload
		if err := proto.Unmarshal(evt.Payload, &req); err == nil {
			decisionOnce.Do(func() {
				capturedDecisionReq = req
				decisionReqWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Track workflow.step dispatches.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var capturedWfID string
	var stepOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			stepOnce.Do(func() {
				capturedWfID = step.WorkflowId
				stepWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Start workflow: step 0 (agent), step 1 (human).
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "resource-approval",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "allocate", Capability: "resource-allocation"},
			{Name: "approve-alloc", HumanApproval: true, Prompt: "Approve allocation?"},
		},
	})

	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "step 0 dispatch")

	// Complete step 0.
	publishStepResult(t, ts, capturedWfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("allocated"), nil)

	waitDoneWithTimeout(t, &decisionReqWg, 5*time.Second, "human.decision.request")

	// Reject the decision.
	rejectPayload := &protocolv1.HumanDecisionResponsePayload{
		DecisionId:  capturedDecisionReq.DecisionId,
		WorkflowId:  capturedWfID,
		Action:      protocolv1.DecisionAction_DECISION_ACTION_REJECT,
		OperatorId:  "operator-2",
		Comment:     "Too risky",
		RespondedAt: time.Now().UnixNano(),
	}
	data, _ := proto.Marshal(rejectPayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "human.decision.response",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish reject: %v", err)
	}

	// Wait for state to transition.
	time.Sleep(500 * time.Millisecond)

	// Verify FAILED state with rejection reason.
	state, _, err := ts.store.Get(ctx, capturedWfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusFailed {
		t.Errorf("status = %v, want StatusFailed", state.Status)
	}
	if state.Error == nil {
		t.Fatal("expected workflow error")
	}
	if state.Error.Code != "DECISION_REJECTED" {
		t.Errorf("error code = %q, want DECISION_REJECTED", state.Error.Code)
	}
}

// TestHumanApproval_DecisionContext verifies US2: decision requests contain
// full context (input bytes, all previous step results, prompt, resource_ids)
// and ListPending returns the correct entry.
func TestHumanApproval_DecisionContext(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	const agentID = "agent-ctx-001"
	ts.registerAgent(t, agentID, "assessment")

	ts.startEngine(t)

	// Track human.decision.request events.
	var decisionReqWg sync.WaitGroup
	decisionReqWg.Add(1)
	var capturedReq protocolv1.HumanDecisionRequestPayload
	var decisionOnce sync.Once

	_, err := ts.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		var req protocolv1.HumanDecisionRequestPayload
		if err := proto.Unmarshal(evt.Payload, &req); err == nil {
			decisionOnce.Do(func() {
				capturedReq = req
				decisionReqWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Track step dispatches.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var capturedWfID string
	var stepOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			stepOnce.Do(func() {
				capturedWfID = step.WorkflowId
				stepWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Workflow with known input bytes on the human step.
	knownInput := []byte(`{"zone":"alpha","severity":5}`)
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "context-test",
		Initiator: "test-operator",
		Steps: []*protocolv1.StepDefinition{
			{Name: "initial-assessment", Capability: "assessment", Input: []byte("assess-zone-alpha")},
			{Name: "human-review", HumanApproval: true, Prompt: "Review assessment results?", ResourceIds: []string{"zone-alpha", "zone-beta"}, Input: knownInput},
		},
	})

	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "step 0 dispatch")

	// Complete step 0 with known result bytes.
	step0Result := []byte("assessment-complete-high-risk")
	publishStepResult(t, ts, capturedWfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, step0Result, nil)

	waitDoneWithTimeout(t, &decisionReqWg, 5*time.Second, "human.decision.request")

	// T016: Verify full context in decision request payload.
	if capturedReq.WorkflowId != capturedWfID {
		t.Errorf("decision req workflow_id = %q, want %q", capturedReq.WorkflowId, capturedWfID)
	}
	if capturedReq.StepIndex != 1 {
		t.Errorf("decision req step_index = %d, want 1", capturedReq.StepIndex)
	}
	if capturedReq.StepName != "human-review" {
		t.Errorf("decision req step_name = %q, want %q", capturedReq.StepName, "human-review")
	}
	if capturedReq.Prompt != "Review assessment results?" {
		t.Errorf("decision req prompt = %q, want %q", capturedReq.Prompt, "Review assessment results?")
	}

	// Verify input bytes match the step definition's input.
	if string(capturedReq.Input) != string(knownInput) {
		t.Errorf("decision req input = %q, want %q", string(capturedReq.Input), string(knownInput))
	}

	// Verify resource_ids.
	if len(capturedReq.ResourceIds) != 2 {
		t.Fatalf("decision req resource_ids len = %d, want 2", len(capturedReq.ResourceIds))
	}
	if capturedReq.ResourceIds[0] != "zone-alpha" || capturedReq.ResourceIds[1] != "zone-beta" {
		t.Errorf("decision req resource_ids = %v, want [zone-alpha zone-beta]", capturedReq.ResourceIds)
	}

	// Verify previous_results contain step 0 result with correct content.
	if len(capturedReq.PreviousResults) != 1 {
		t.Fatalf("decision req previous_results len = %d, want 1", len(capturedReq.PreviousResults))
	}
	pr := capturedReq.PreviousResults[0]
	if pr.StepIndex != 0 {
		t.Errorf("previous_results[0].step_index = %d, want 0", pr.StepIndex)
	}
	if string(pr.Result) != string(step0Result) {
		t.Errorf("previous_results[0].result = %q, want %q", string(pr.Result), string(step0Result))
	}
	if pr.Status != protocolv1.StepStatus_STEP_STATUS_SUCCESS {
		t.Errorf("previous_results[0].status = %v, want SUCCESS", pr.Status)
	}

	// T017/T019: Verify ListPending returns one entry with full context.
	pendingList := ts.engine.Coordinator().ListPending()
	if len(pendingList) != 1 {
		t.Fatalf("ListPending() len = %d, want 1", len(pendingList))
	}
	pd := pendingList[0]
	if pd.DecisionID != capturedReq.DecisionId {
		t.Errorf("ListPending()[0].DecisionID = %q, want %q", pd.DecisionID, capturedReq.DecisionId)
	}
	if pd.WorkflowID != capturedWfID {
		t.Errorf("ListPending()[0].WorkflowID = %q, want %q", pd.WorkflowID, capturedWfID)
	}
	if pd.StepIndex != 1 {
		t.Errorf("ListPending()[0].StepIndex = %d, want 1", pd.StepIndex)
	}
	if pd.StepName != "human-review" {
		t.Errorf("ListPending()[0].StepName = %q, want %q", pd.StepName, "human-review")
	}
	if pd.Prompt != "Review assessment results?" {
		t.Errorf("ListPending()[0].Prompt = %q, want %q", pd.Prompt, "Review assessment results?")
	}
	if len(pd.ResourceIDs) != 2 || pd.ResourceIDs[0] != "zone-alpha" || pd.ResourceIDs[1] != "zone-beta" {
		t.Errorf("ListPending()[0].ResourceIDs = %v, want [zone-alpha zone-beta]", pd.ResourceIDs)
	}
	if pd.RequestedAt == 0 {
		t.Error("ListPending()[0].RequestedAt is zero")
	}

	// Verify GetPending returns the same entry.
	got := ts.engine.Coordinator().GetPending(capturedReq.DecisionId)
	if got == nil {
		t.Fatal("GetPending returned nil for known decision_id")
	}
	if got.DecisionID != pd.DecisionID {
		t.Errorf("GetPending().DecisionID mismatch: %q vs %q", got.DecisionID, pd.DecisionID)
	}
}

// TestConflictDetection verifies US3: two workflows requesting decisions with
// overlapping resource_ids are annotated with ConflictIDs and a decision.conflict
// event is published. Approving one publishes decision.resolved with related IDs.
func TestConflictDetection(t *testing.T) {
	ts := newTestSetup(t, 10*time.Second)
	ctx := context.Background()

	const agent1 = "agent-conflict-001"
	const agent2 = "agent-conflict-002"
	ts.registerAgent(t, agent1, "fire-assessment")
	ts.registerAgent(t, agent2, "resource-dispatch")

	ts.startEngine(t)

	// Track human.decision.request events (expect 2).
	var decisionReqMu sync.Mutex
	var capturedDecisionReqs []protocolv1.HumanDecisionRequestPayload
	var decisionReqWg sync.WaitGroup
	decisionReqWg.Add(2)

	_, err := ts.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		var req protocolv1.HumanDecisionRequestPayload
		if err := proto.Unmarshal(evt.Payload, &req); err == nil {
			decisionReqMu.Lock()
			capturedDecisionReqs = append(capturedDecisionReqs, req)
			count := len(capturedDecisionReqs)
			decisionReqMu.Unlock()
			if count <= 2 {
				decisionReqWg.Done()
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe human.decision.request: %v", err)
	}

	// Track decision.conflict events.
	var conflictWg sync.WaitGroup
	conflictWg.Add(1)
	var capturedConflict protocolv1.DecisionConflictPayload
	var conflictOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "decision.conflict", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.DecisionConflictPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			conflictOnce.Do(func() {
				capturedConflict = payload
				conflictWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe decision.conflict: %v", err)
	}

	// Track decision.resolved events.
	var resolvedWg sync.WaitGroup
	resolvedWg.Add(1)
	var capturedResolved protocolv1.DecisionResolvedPayload
	var resolvedOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "decision.resolved", func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.DecisionResolvedPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			resolvedOnce.Do(func() {
				capturedResolved = payload
				resolvedWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe decision.resolved: %v", err)
	}

	// Track step dispatches for both agents.
	var step1Wg, step2Wg sync.WaitGroup
	step1Wg.Add(1)
	step2Wg.Add(1)
	var wfID1, wfID2 string
	var step1Once, step2Once sync.Once

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agent1+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step1Once.Do(func() {
				wfID1 = step.WorkflowId
				step1Wg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent1: %v", err)
	}

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agent2+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			step2Once.Do(func() {
				wfID2 = step.WorkflowId
				step2Wg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent2: %v", err)
	}

	// Start workflow 1: step 0 (agent), step 1 (human with resource zone-a).
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "fire-response-1",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "assess-fire-1", Capability: "fire-assessment"},
			{Name: "approve-response-1", HumanApproval: true, Prompt: "Approve fire response plan?", ResourceIds: []string{"zone-a"}},
		},
	})

	waitDoneWithTimeout(t, &step1Wg, 5*time.Second, "workflow 1 step 0 dispatch")

	// Start workflow 2: step 0 (agent), step 1 (human with resource zone-a — overlaps!).
	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:      "resource-dispatch-1",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "dispatch-resources", Capability: "resource-dispatch"},
			{Name: "approve-dispatch", HumanApproval: true, Prompt: "Approve resource dispatch?", ResourceIds: []string{"zone-a", "zone-b"}},
		},
	})

	waitDoneWithTimeout(t, &step2Wg, 5*time.Second, "workflow 2 step 0 dispatch")

	// Complete step 0 of workflow 1 to trigger human decision request.
	publishStepResult(t, ts, wfID1, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("fire-data"), nil)

	// Wait a bit to ensure first decision is registered before triggering the second.
	time.Sleep(300 * time.Millisecond)

	// Complete step 0 of workflow 2 to trigger second human decision (with conflict).
	publishStepResult(t, ts, wfID2, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("resource-data"), nil)

	// Wait for both decision requests.
	waitDoneWithTimeout(t, &decisionReqWg, 5*time.Second, "both human.decision.request events")

	// Wait for decision.conflict event.
	waitDoneWithTimeout(t, &conflictWg, 5*time.Second, "decision.conflict event")

	// Verify conflict event content.
	if capturedConflict.ConflictGroupId == "" {
		t.Error("conflict_group_id is empty")
	}
	if len(capturedConflict.DecisionIds) != 2 {
		t.Errorf("conflict decision_ids len = %d, want 2", len(capturedConflict.DecisionIds))
	}
	if len(capturedConflict.WorkflowIds) != 2 {
		t.Errorf("conflict workflow_ids len = %d, want 2", len(capturedConflict.WorkflowIds))
	}

	// Verify zone-a is in the conflicting resource_ids.
	hasZoneA := false
	for _, rid := range capturedConflict.ResourceIds {
		if rid == "zone-a" {
			hasZoneA = true
			break
		}
	}
	if !hasZoneA {
		t.Errorf("conflict resource_ids = %v, want to contain zone-a", capturedConflict.ResourceIds)
	}

	// Verify both pending decisions have ConflictIDs populated.
	pendingList := ts.engine.Coordinator().ListPending()
	if len(pendingList) != 2 {
		t.Fatalf("ListPending() len = %d, want 2", len(pendingList))
	}
	for _, pd := range pendingList {
		if len(pd.ConflictIDs) == 0 {
			t.Errorf("PendingDecision %s has no ConflictIDs", pd.DecisionID)
		}
	}

	// Verify KV state has ConflictIDs on both workflows.
	state1, _, err := ts.store.Get(ctx, wfID1)
	if err != nil {
		t.Fatalf("store.Get wf1: %v", err)
	}
	if state1.PendingDecision == nil || len(state1.PendingDecision.ConflictIDs) == 0 {
		t.Error("workflow 1 PendingDecision has no ConflictIDs in KV")
	}

	state2, _, err := ts.store.Get(ctx, wfID2)
	if err != nil {
		t.Fatalf("store.Get wf2: %v", err)
	}
	if state2.PendingDecision == nil || len(state2.PendingDecision.ConflictIDs) == 0 {
		t.Error("workflow 2 PendingDecision has no ConflictIDs in KV")
	}

	// Approve workflow 1's decision.
	decisionReqMu.Lock()
	var wf1DecisionID string
	for _, req := range capturedDecisionReqs {
		if req.WorkflowId == wfID1 {
			wf1DecisionID = req.DecisionId
			break
		}
	}
	decisionReqMu.Unlock()

	if wf1DecisionID == "" {
		t.Fatal("could not find decision ID for workflow 1")
	}

	approvePayload := &protocolv1.HumanDecisionResponsePayload{
		DecisionId:  wf1DecisionID,
		WorkflowId:  wfID1,
		Action:      protocolv1.DecisionAction_DECISION_ACTION_APPROVE,
		OperatorId:  "operator-conflict-1",
		Comment:     "Approved with conflict awareness",
		RespondedAt: time.Now().UnixNano(),
	}
	data, _ := proto.Marshal(approvePayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "human.decision.response",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish approve: %v", err)
	}

	// Wait for decision.resolved event.
	waitDoneWithTimeout(t, &resolvedWg, 5*time.Second, "decision.resolved event")

	// Verify resolved event.
	if capturedResolved.DecisionId != wf1DecisionID {
		t.Errorf("resolved decision_id = %q, want %q", capturedResolved.DecisionId, wf1DecisionID)
	}
	if capturedResolved.WorkflowId != wfID1 {
		t.Errorf("resolved workflow_id = %q, want %q", capturedResolved.WorkflowId, wfID1)
	}
	if capturedResolved.Action != protocolv1.DecisionAction_DECISION_ACTION_APPROVE {
		t.Errorf("resolved action = %v, want APPROVE", capturedResolved.Action)
	}
	if capturedResolved.ConflictGroupId == "" {
		t.Error("resolved conflict_group_id is empty")
	}
	if len(capturedResolved.RelatedDecisionIds) == 0 {
		t.Error("resolved related_decision_ids is empty")
	}
}
