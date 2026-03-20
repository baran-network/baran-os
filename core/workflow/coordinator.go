package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// DefaultDecisionTimeout is the default timeout for human decisions (24h).
const DefaultDecisionTimeout = 24 * time.Hour

// ErrCodeDecisionTimeout is the error code when a human decision times out.
const ErrCodeDecisionTimeout = "DECISION_TIMEOUT"

// ErrCodeDecisionRejected is the error code when an operator rejects a decision.
const ErrCodeDecisionRejected = "DECISION_REJECTED"

// DecisionCoordinator manages human-in-the-loop decision requests and responses.
// It is the single owner of the pending decision index and coordinates with the
// workflow engine to pause/resume workflows based on operator decisions.
type DecisionCoordinator struct {
	bus      eventbus.EventBus
	store    WorkflowStateStore
	timeouts *StepTimeoutManager
	nodeID   string

	mu      sync.RWMutex
	pending map[string]*PendingDecision // decision_id -> PendingDecision

	// resumeCh is used to notify the engine that a decision has been made.
	// The engine sends a channel per workflow when requesting a decision,
	// and the coordinator sends the response back on it.
	resumeMu sync.RWMutex
	resumeCh map[string]chan DecisionResult // workflow_id -> result channel
}

// DecisionResult is sent from the coordinator to the engine when a decision is made.
type DecisionResult struct {
	Approved bool
	Response *protocolv1.HumanDecisionResponsePayload
}

// NewDecisionCoordinator creates a DecisionCoordinator.
func NewDecisionCoordinator(
	bus eventbus.EventBus,
	store WorkflowStateStore,
	timeouts *StepTimeoutManager,
	nodeID string,
) *DecisionCoordinator {
	return &DecisionCoordinator{
		bus:      bus,
		store:    store,
		timeouts: timeouts,
		nodeID:   nodeID,
		pending:  make(map[string]*PendingDecision),
		resumeCh: make(map[string]chan DecisionResult),
	}
}

// RequestDecision pauses a workflow at a human approval step. It generates a
// decision ID, updates workflow state to WAITING_HUMAN, publishes a
// human.decision.request event, and schedules a timeout.
// Returns a channel that will receive the decision result when the operator responds.
func (c *DecisionCoordinator) RequestDecision(
	ctx context.Context,
	workflowID string,
	state WorkflowState,
	revision uint64,
	stepDef StepDefinition,
	stepIndex uint32,
) (chan DecisionResult, error) {
	decisionID := uuid.Must(uuid.NewV7()).String()
	now := time.Now().UnixNano()

	pd := &PendingDecision{
		DecisionID:  decisionID,
		WorkflowID:  workflowID,
		StepIndex:   stepIndex,
		StepName:    stepDef.Name,
		Prompt:      stepDef.Prompt,
		ResourceIDs: stepDef.ResourceIDs,
		RequestedAt: now,
	}

	// Update workflow state to WAITING_HUMAN with pending decision.
	state.Status = StatusWaitingHuman
	state.PendingDecision = pd
	state.UpdatedAt = now
	if err := c.store.Update(ctx, workflowID, state, revision); err != nil {
		return nil, fmt.Errorf("update state to WAITING_HUMAN: %w", err)
	}

	// Add to in-memory pending index and detect conflicts.
	c.mu.Lock()
	c.pending[decisionID] = pd

	// T020: Scan existing pending decisions for overlapping ResourceIDs.
	var conflictingDecisions []*PendingDecision
	if len(stepDef.ResourceIDs) > 0 {
		newResources := make(map[string]struct{}, len(stepDef.ResourceIDs))
		for _, rid := range stepDef.ResourceIDs {
			newResources[rid] = struct{}{}
		}
		for _, existing := range c.pending {
			if existing.DecisionID == decisionID {
				continue
			}
			for _, rid := range existing.ResourceIDs {
				if _, overlap := newResources[rid]; overlap {
					conflictingDecisions = append(conflictingDecisions, existing)
					break
				}
			}
		}
	}

	// If conflicts found, annotate all involved decisions with ConflictIDs.
	if len(conflictingDecisions) > 0 {
		conflictGroupID := uuid.Must(uuid.NewV7()).String()
		allDecisionIDs := []string{decisionID}
		allWorkflowIDs := []string{workflowID}
		overlappingResources := make(map[string]struct{})

		for _, cd := range conflictingDecisions {
			allDecisionIDs = append(allDecisionIDs, cd.DecisionID)
			allWorkflowIDs = append(allWorkflowIDs, cd.WorkflowID)
			// Add this decision's ID to the existing decision's ConflictIDs.
			cd.ConflictIDs = appendUnique(cd.ConflictIDs, decisionID)
			// Add the existing decision's ID to the new decision's ConflictIDs.
			pd.ConflictIDs = appendUnique(pd.ConflictIDs, cd.DecisionID)
			// Collect overlapping resource IDs.
			for _, rid := range cd.ResourceIDs {
				overlappingResources[rid] = struct{}{}
			}
		}
		for _, rid := range stepDef.ResourceIDs {
			overlappingResources[rid] = struct{}{}
		}

		c.mu.Unlock()

		// Update KV state for conflicting decisions with new ConflictIDs.
		for _, cd := range conflictingDecisions {
			c.updateConflictIDs(ctx, cd.WorkflowID, cd.ConflictIDs)
		}
		// Update new decision's KV state with ConflictIDs.
		state.PendingDecision = pd
		// Re-read to get latest revision after the initial update.
		if latestState, latestRev, err := c.store.Get(ctx, workflowID); err == nil {
			latestState.PendingDecision = pd
			_ = c.store.Update(ctx, workflowID, latestState, latestRev)
		}

		// T021: Publish decision.conflict event.
		resourceSlice := make([]string, 0, len(overlappingResources))
		for rid := range overlappingResources {
			resourceSlice = append(resourceSlice, rid)
		}
		conflictPayload := &protocolv1.DecisionConflictPayload{
			ConflictGroupId: conflictGroupID,
			DecisionIds:     allDecisionIDs,
			WorkflowIds:     allWorkflowIDs,
			ResourceIds:     resourceSlice,
			DetectedAt:      time.Now().UnixNano(),
		}
		conflictData, err := proto.Marshal(conflictPayload)
		if err == nil {
			_ = c.bus.Publish(ctx, &eventbus.Event{
				ID:         uuid.Must(uuid.NewV7()).String(),
				Type:       "decision.conflict",
				SourceNode: c.nodeID,
				Timestamp:  time.Now().UnixNano(),
				Payload:    conflictData,
			})
		}

		// Store conflict group ID for resolution tracking.
		c.mu.Lock()
		pd.conflictGroupID = conflictGroupID
		for _, cd := range conflictingDecisions {
			cd.conflictGroupID = conflictGroupID
		}
		c.mu.Unlock()
	} else {
		c.mu.Unlock()
	}

	// Create resume channel for this workflow.
	resultCh := make(chan DecisionResult, 1)
	c.resumeMu.Lock()
	c.resumeCh[workflowID] = resultCh
	c.resumeMu.Unlock()

	// Build and publish human.decision.request event.
	reqPayload := &protocolv1.HumanDecisionRequestPayload{
		DecisionId:      decisionID,
		WorkflowId:      workflowID,
		StepIndex:       stepIndex,
		StepName:        stepDef.Name,
		Prompt:          stepDef.Prompt,
		Input:           state.Definition.Steps[stepIndex].Input,
		PreviousResults: stepResultsToProto(state.StepResults),
		ResourceIds:     stepDef.ResourceIDs,
		ConflictIds:     pd.ConflictIDs,
	}
	data, err := proto.Marshal(reqPayload)
	if err != nil {
		return nil, fmt.Errorf("marshal HumanDecisionRequestPayload: %w", err)
	}

	if err := c.bus.Publish(ctx, &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       "human.decision.request",
		SourceNode: c.nodeID,
		WorkflowID: workflowID,
		Timestamp:  now,
		Payload:    data,
	}); err != nil {
		return nil, fmt.Errorf("publish human.decision.request: %w", err)
	}

	// Schedule timeout (24h default, or per-step timeout).
	timeout := DefaultDecisionTimeout
	if stepDef.TimeoutSeconds > 0 {
		timeout = time.Duration(stepDef.TimeoutSeconds) * time.Second
	}
	c.timeouts.Schedule(workflowID, timeout, func() {
		c.handleTimeout(workflowID, decisionID, stepIndex)
	})

	return resultCh, nil
}

// ProcessResponse handles an operator's approve/reject decision.
// It updates workflow state, cancels the timeout, and notifies the engine.
func (c *DecisionCoordinator) ProcessResponse(ctx context.Context, payload *protocolv1.HumanDecisionResponsePayload) error {
	workflowID := payload.WorkflowId
	decisionID := payload.DecisionId

	state, revision, err := c.store.Get(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow state: %w", err)
	}

	// Guard: only process if workflow is WAITING_HUMAN (idempotent no-op otherwise).
	if state.Status != StatusWaitingHuman {
		return nil
	}

	// Verify decision ID matches.
	if state.PendingDecision == nil || state.PendingDecision.DecisionID != decisionID {
		return nil
	}

	// Cancel the timeout.
	c.timeouts.Cancel(workflowID)

	// Capture conflict info before removing from pending index.
	c.mu.Lock()
	pd := c.pending[decisionID]
	var conflictGroupID string
	var relatedDecisionIDs []string
	if pd != nil {
		conflictGroupID = pd.conflictGroupID
		relatedDecisionIDs = pd.ConflictIDs
	}
	delete(c.pending, decisionID)
	c.mu.Unlock()

	approved := payload.Action == protocolv1.DecisionAction_DECISION_ACTION_APPROVE

	if approved {
		state.Status = StatusRunning
		state.PendingDecision = nil
		state.UpdatedAt = time.Now().UnixNano()
		if err := c.store.Update(ctx, workflowID, state, revision); err != nil {
			// CAS conflict = already processed → no-op.
			return nil
		}
	} else {
		state.Status = StatusFailed
		state.PendingDecision = nil
		state.Error = newWorkflowError(ErrCodeDecisionRejected,
			fmt.Sprintf("decision rejected by %s: %s", payload.OperatorId, payload.Comment),
			state.CurrentStep, "")
		state.UpdatedAt = time.Now().UnixNano()
		if err := c.store.Update(ctx, workflowID, state, revision); err != nil {
			return nil
		}
	}

	// T022: Publish decision.resolved event with conflict context.
	resolvedPayload := &protocolv1.DecisionResolvedPayload{
		DecisionId:         decisionID,
		WorkflowId:         workflowID,
		Action:             payload.Action,
		ConflictGroupId:    conflictGroupID,
		RelatedDecisionIds: relatedDecisionIDs,
		ResolvedAt:         time.Now().UnixNano(),
	}
	resolvedData, err := proto.Marshal(resolvedPayload)
	if err == nil {
		_ = c.bus.Publish(ctx, &eventbus.Event{
			ID:         uuid.Must(uuid.NewV7()).String(),
			Type:       "decision.resolved",
			SourceNode: c.nodeID,
			WorkflowID: workflowID,
			Timestamp:  time.Now().UnixNano(),
			Payload:    resolvedData,
		})
	}

	// Notify the engine via resume channel.
	c.resumeMu.RLock()
	ch, ok := c.resumeCh[workflowID]
	c.resumeMu.RUnlock()

	if ok {
		result := DecisionResult{
			Approved: approved,
			Response: payload,
		}
		select {
		case ch <- result:
		default:
		}

		c.resumeMu.Lock()
		delete(c.resumeCh, workflowID)
		c.resumeMu.Unlock()
	}

	return nil
}

// ListPending returns all currently pending human decisions.
func (c *DecisionCoordinator) ListPending() []*PendingDecision {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*PendingDecision, 0, len(c.pending))
	for _, pd := range c.pending {
		result = append(result, pd)
	}
	return result
}

// GetPending returns a pending decision by ID, or nil if not found.
func (c *DecisionCoordinator) GetPending(decisionID string) *PendingDecision {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.pending[decisionID]
}

// RecoverPending scans all workflow states in the KV store and re-populates
// the in-memory pending index for any workflows in WAITING_HUMAN status.
// Called during startup to recover from restarts.
func (c *DecisionCoordinator) RecoverPending(ctx context.Context, listFn func(ctx context.Context) ([]WorkflowState, error)) error {
	states, err := listFn(ctx)
	if err != nil {
		return fmt.Errorf("list workflow states for recovery: %w", err)
	}

	for _, state := range states {
		if state.Status != StatusWaitingHuman || state.PendingDecision == nil {
			continue
		}

		pd := state.PendingDecision

		// Add to in-memory index.
		c.mu.Lock()
		c.pending[pd.DecisionID] = pd
		c.mu.Unlock()

		// Re-publish human.decision.request for visibility (idempotent via decision_id).
		reqPayload := &protocolv1.HumanDecisionRequestPayload{
			DecisionId:  pd.DecisionID,
			WorkflowId:  state.WorkflowID,
			StepIndex:   pd.StepIndex,
			Prompt:      pd.Prompt,
			ResourceIds: pd.ResourceIDs,
		}
		if int(pd.StepIndex) < len(state.Definition.Steps) {
			step := state.Definition.Steps[pd.StepIndex]
			reqPayload.StepName = step.Name
			reqPayload.Input = step.Input
			reqPayload.PreviousResults = stepResultsToProto(state.StepResults)
		}
		data, err := proto.Marshal(reqPayload)
		if err != nil {
			continue
		}

		// Use decision_id as event ID for idempotency.
		_ = c.bus.Publish(ctx, &eventbus.Event{
			ID:         pd.DecisionID,
			Type:       "human.decision.request",
			SourceNode: c.nodeID,
			WorkflowID: state.WorkflowID,
			Timestamp:  time.Now().UnixNano(),
			Payload:    data,
		})
	}

	return nil
}

// handleTimeout is called when a decision timeout fires.
func (c *DecisionCoordinator) handleTimeout(workflowID, decisionID string, stepIndex uint32) {
	ctx := context.Background()

	state, revision, err := c.store.Get(ctx, workflowID)
	if err != nil || state.Status != StatusWaitingHuman {
		return
	}

	// Verify the decision ID still matches (in case of recovery).
	if state.PendingDecision == nil || state.PendingDecision.DecisionID != decisionID {
		return
	}

	// Remove from pending index.
	c.mu.Lock()
	delete(c.pending, decisionID)
	c.mu.Unlock()

	state.Status = StatusFailed
	state.PendingDecision = nil
	state.Error = newWorkflowError(ErrCodeDecisionTimeout,
		fmt.Sprintf("human decision timed out for step %d", stepIndex),
		stepIndex, "")
	state.UpdatedAt = time.Now().UnixNano()

	_ = c.store.Update(ctx, workflowID, state, revision)

	// Notify engine of timeout (treated as rejection).
	c.resumeMu.RLock()
	ch, ok := c.resumeCh[workflowID]
	c.resumeMu.RUnlock()

	if ok {
		select {
		case ch <- DecisionResult{Approved: false}:
		default:
		}
		c.resumeMu.Lock()
		delete(c.resumeCh, workflowID)
		c.resumeMu.Unlock()
	}
}

// updateConflictIDs updates the ConflictIDs field on a workflow's PendingDecision in KV.
func (c *DecisionCoordinator) updateConflictIDs(ctx context.Context, workflowID string, conflictIDs []string) {
	state, rev, err := c.store.Get(ctx, workflowID)
	if err != nil || state.PendingDecision == nil {
		return
	}
	state.PendingDecision.ConflictIDs = conflictIDs
	_ = c.store.Update(ctx, workflowID, state, rev)
}

// appendUnique appends value to slice if not already present.
func appendUnique(slice []string, value string) []string {
	for _, s := range slice {
		if s == value {
			return slice
		}
	}
	return append(slice, value)
}
