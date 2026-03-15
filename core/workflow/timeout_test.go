package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ad-hok/agent-os/core/eventbus"
	"github.com/ad-hok/agent-os/core/workflow"
	protocolv1 "github.com/ad-hok/agent-os/protocol/gen/go/agentosprotocol/v1"
	"google.golang.org/protobuf/proto"
)

// TestUS4_StepTimeout verifies that a step that exceeds its timeout
// transitions the workflow to FAILED with STEP_TIMEOUT error code.
func TestUS4_StepTimeout(t *testing.T) {
	// Use a very short default timeout so the test runs quickly.
	ts := newTestSetup(t, 100*time.Millisecond)
	ctx := context.Background()

	const agentID = "agent-timeout-001"
	ts.registerAgent(t, agentID, "slow-capability")
	ts.startEngine(t)

	// Subscribe to workflow.failed.
	var failedWg sync.WaitGroup
	failedWg.Add(1)
	var capturedFailed protocolv1.WorkflowFailedPayload
	var failedOnce sync.Once

	// Capture the workflow ID first.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var wfID string
	var stepOnce sync.Once

	_, _ = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			stepOnce.Do(func() {
				wfID = step.WorkflowId
				stepWg.Done()
			})
		}
		return nil
	})

	publishWorkflowStart(t, ts.bus, &protocolv1.WorkflowDefinition{
		Name:  "timeout-workflow",
		Steps: []*protocolv1.StepDefinition{{Capability: "slow-capability"}},
	})

	waitDoneWithTimeout(t, &stepWg, 5*time.Second, "step dispatch")

	// Now subscribe to workflow.failed on per-workflow stream.
	_, err := ts.bus.Subscribe(ctx, "workflow."+wfID+".workflow.failed", func(_ context.Context, evt *eventbus.Event) error {
		var fp protocolv1.WorkflowFailedPayload
		if err := proto.Unmarshal(evt.Payload, &fp); err == nil {
			failedOnce.Do(func() {
				capturedFailed = fp
				failedWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.failed: %v", err)
	}

	// Do NOT send any step result — let the timeout fire.
	waitDoneWithTimeout(t, &failedWg, 3*time.Second, "workflow.failed from timeout")

	if capturedFailed.WorkflowId != wfID {
		t.Errorf("failed workflow_id = %q, want %q", capturedFailed.WorkflowId, wfID)
	}
	if capturedFailed.Error == nil {
		t.Fatal("expected error in WorkflowFailedPayload")
	}
	if capturedFailed.Error.Code != "STEP_TIMEOUT" {
		t.Errorf("error code = %q, want STEP_TIMEOUT", capturedFailed.Error.Code)
	}

	// Verify KV state.
	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusFailed {
		t.Errorf("status = %v, want FAILED", state.Status)
	}
}

// TestUS4_TimelyResultCancelsTimeout verifies that sending a step result
// before the timeout fires prevents the timeout from failing the workflow.
func TestUS4_TimelyResultCancelsTimeout(t *testing.T) {
	// 500ms timeout — we'll respond within 100ms.
	ts := newTestSetup(t, 500*time.Millisecond)
	ctx := context.Background()

	const agentID = "agent-timeout-002"
	ts.registerAgent(t, agentID, "fast-capability")
	ts.startEngine(t)

	wfID := startWorkflowAndCaptureID(t, ts, agentID, &protocolv1.WorkflowDefinition{
		Name:  "timely-workflow",
		Steps: []*protocolv1.StepDefinition{{Capability: "fast-capability"}},
	})

	// Subscribe to complete event.
	var completeWg sync.WaitGroup
	completeWg.Add(1)
	var completeOnce sync.Once
	_, _ = ts.bus.Subscribe(ctx, "workflow."+wfID+".workflow.complete", func(_ context.Context, evt *eventbus.Event) error {
		completeOnce.Do(func() { completeWg.Done() })
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	// Send result quickly (before timeout).
	publishStepResult(t, ts, wfID, 0, protocolv1.StepStatus_STEP_STATUS_SUCCESS, []byte("fast"), nil)
	waitDoneWithTimeout(t, &completeWg, 5*time.Second, "workflow.complete")

	// Wait past the original timeout to make sure it doesn't fire.
	time.Sleep(700 * time.Millisecond)

	state, _, err := ts.store.Get(ctx, wfID)
	if err != nil {
		t.Fatalf("store.Get: %v", err)
	}
	if state.Status != workflow.StatusCompleted {
		t.Errorf("status = %v, want COMPLETED (timeout should have been cancelled)", state.Status)
	}
}
