package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ad-hok/agent-os/core/testutil"
	"github.com/ad-hok/agent-os/core/workflow"
)

func newTestStore(t *testing.T) workflow.WorkflowStateStore {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	store, err := workflow.NewKVWorkflowStateStore(context.Background(), nc)
	if err != nil {
		t.Fatalf("NewKVWorkflowStateStore: %v", err)
	}
	return store
}

func sampleState(id string) workflow.WorkflowState {
	return workflow.WorkflowState{
		WorkflowID: id,
		Status:     workflow.StatusRunning,
		Definition: workflow.WorkflowDefinition{
			Name:      "test-workflow",
			Initiator: "test",
			Steps: []workflow.StepDefinition{
				{Name: "step-0", Capability: "risk-estimation"},
			},
		},
		CurrentStep: 0,
		CreatedAt:   time.Now().UnixNano(),
		UpdatedAt:   time.Now().UnixNano(),
	}
}

func TestKVWorkflowStateStore_Create(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	state := sampleState("wf-001")
	if err := store.Create(ctx, "wf-001", state); err != nil {
		t.Fatalf("Create: %v", err)
	}
}

func TestKVWorkflowStateStore_Get(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	state := sampleState("wf-002")
	if err := store.Create(ctx, "wf-002", state); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, rev, err := store.Get(ctx, "wf-002")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if rev == 0 {
		t.Fatal("expected non-zero revision")
	}
	if got.WorkflowID != "wf-002" {
		t.Errorf("WorkflowID = %q, want %q", got.WorkflowID, "wf-002")
	}
	if got.Status != workflow.StatusRunning {
		t.Errorf("Status = %v, want Running", got.Status)
	}
}

func TestKVWorkflowStateStore_GetNotFound(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, _, err := store.Get(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent workflow")
	}
	if err != workflow.ErrWorkflowNotFound {
		t.Errorf("expected ErrWorkflowNotFound, got %v", err)
	}
}

func TestKVWorkflowStateStore_UpdateWithCAS(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	state := sampleState("wf-003")
	if err := store.Create(ctx, "wf-003", state); err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, rev, err := store.Get(ctx, "wf-003")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	state.Status = workflow.StatusCompleted
	if err := store.Update(ctx, "wf-003", state, rev); err != nil {
		t.Fatalf("Update: %v", err)
	}

	updated, _, err := store.Get(ctx, "wf-003")
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if updated.Status != workflow.StatusCompleted {
		t.Errorf("Status = %v, want Completed", updated.Status)
	}
}

func TestKVWorkflowStateStore_CASConflict(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	state := sampleState("wf-004")
	if err := store.Create(ctx, "wf-004", state); err != nil {
		t.Fatalf("Create: %v", err)
	}

	_, rev, err := store.Get(ctx, "wf-004")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// First update succeeds.
	state.Status = workflow.StatusCompleted
	if err := store.Update(ctx, "wf-004", state, rev); err != nil {
		t.Fatalf("first Update: %v", err)
	}

	// Second update with old revision must fail.
	state.Status = workflow.StatusFailed
	err = store.Update(ctx, "wf-004", state, rev)
	if err == nil {
		t.Fatal("expected CAS conflict error, got nil")
	}
	if !errors.Is(err, workflow.ErrCASConflict) {
		t.Errorf("expected ErrCASConflict, got %v", err)
	}
}
