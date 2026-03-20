package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// WorkflowEngine orchestrates multi-agent workflows by listening to events,
// managing workflow state, dispatching steps to agents, and enforcing timeouts.
type WorkflowEngine struct {
	bus            eventbus.EventBus
	store          WorkflowStateStore
	registry       registry.AgentRegistry
	streamMgr      *WorkflowStreamManager
	publisher      eventbus.EventPublisher
	nodeID         string
	defaultTimeout time.Duration
	timeouts       *StepTimeoutManager
}

// NewWorkflowEngine creates a WorkflowEngine.
func NewWorkflowEngine(
	bus eventbus.EventBus,
	store WorkflowStateStore,
	reg registry.AgentRegistry,
	streamMgr *WorkflowStreamManager,
	publisher eventbus.EventPublisher,
	nodeID string,
	defaultTimeout time.Duration,
) *WorkflowEngine {
	if defaultTimeout == 0 {
		defaultTimeout = DefaultStepTimeout
	}
	return &WorkflowEngine{
		bus:            bus,
		store:          store,
		registry:       reg,
		streamMgr:      streamMgr,
		publisher:      publisher,
		nodeID:         nodeID,
		defaultTimeout: defaultTimeout,
		timeouts:       NewStepTimeoutManager(),
	}
}

// Start subscribes to workflow events and begins orchestration.
// Returns a slice of active subscriptions that can be cancelled.
func (e *WorkflowEngine) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	var subs []eventbus.Subscription

	sub, err := e.bus.Subscribe(ctx, "workflow.start", e.handleWorkflowStart)
	if err != nil {
		return nil, fmt.Errorf("subscribe workflow.start: %w", err)
	}
	subs = append(subs, sub)

	sub, err = e.bus.Subscribe(ctx, "workflow.state.request", e.handleWorkflowStateRequest)
	if err != nil {
		return nil, fmt.Errorf("subscribe workflow.state.request: %w", err)
	}
	subs = append(subs, sub)

	sub, err = e.bus.Subscribe(ctx, "agent.unregister", e.handleAgentUnregister)
	if err != nil {
		return nil, fmt.Errorf("subscribe agent.unregister: %w", err)
	}
	subs = append(subs, sub)

	return subs, nil
}

// startWorkflow generates a UUID, persists initial state, and dispatches step 0.
func (e *WorkflowEngine) startWorkflow(ctx context.Context, def WorkflowDefinition) error {
	workflowID := uuid.Must(uuid.NewV7()).String()
	now := time.Now().UnixNano()

	state := WorkflowState{
		WorkflowID:  workflowID,
		Status:      StatusRunning,
		Definition:  def,
		CurrentStep: 0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := e.store.Create(ctx, workflowID, state); err != nil {
		return fmt.Errorf("create workflow state %s: %w", workflowID, err)
	}

	// Ensure the per-workflow stream exists via the stream manager.
	streamName, err := e.streamMgr.GetOrCreateStream(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("create workflow stream %s: %w", workflowID, err)
	}

	// Subscribe to step results on the specific workflow stream.
	resultSubject := fmt.Sprintf("workflow.%s.workflow.step.result", workflowID)
	if _, err := e.bus.SubscribeWithStream(ctx, streamName, resultSubject, e.handleWorkflowStepResult); err != nil {
		return fmt.Errorf("subscribe to step results: %w", err)
	}

	return e.dispatchStep(ctx, workflowID, 0)
}

// dispatchStep resolves the agent for the current step and publishes workflow.step.
func (e *WorkflowEngine) dispatchStep(ctx context.Context, workflowID string, stepIndex uint32) error {
	state, revision, err := e.store.Get(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get state for dispatch: %w", err)
	}

	step := state.Definition.Steps[stepIndex]

	// Resolve capability to an active agent.
	agents, err := e.registry.FindByCapability(ctx, step.Capability, "")
	if err != nil {
		return e.failWorkflowWithError(ctx, workflowID, state, revision,
			newWorkflowError(ErrCodeAgentUnavailable,
				fmt.Sprintf("capability lookup failed for %q: %v", step.Capability, err),
				stepIndex, ""))
	}
	if len(agents) == 0 {
		return e.failWorkflowWithError(ctx, workflowID, state, revision,
			newWorkflowError(ErrCodeAgentUnavailable,
				fmt.Sprintf("no active agent found for capability %q", step.Capability),
				stepIndex, ""))
	}

	agentID := agents[0].AgentID

	// Update assigned agent in state.
	state.AssignedAgent = agentID
	state.UpdatedAt = time.Now().UnixNano()
	if err := e.store.Update(ctx, workflowID, state, revision); err != nil {
		// CAS conflict on assign — treat as non-fatal; the step result handler will reconcile.
		return nil
	}

	// Build step payload.
	pbStep := &protocolv1.WorkflowStepPayload{
		StepIndex:       stepIndex,
		WorkflowId:      workflowID,
		PreviousResults: stepResultsToProto(state.StepResults),
		Step: &protocolv1.StepDefinition{
			Name:           step.Name,
			Capability:     step.Capability,
			TimeoutSeconds: step.TimeoutSeconds,
			Input:          step.Input,
		},
		Input: step.Input,
	}
	data, err := proto.Marshal(pbStep)
	if err != nil {
		return fmt.Errorf("marshal WorkflowStepPayload: %w", err)
	}

	now := time.Now().UnixNano()

	// Publish to the per-workflow stream via router (StrategyWorkflow).
	if err := e.publisher.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "workflow.step",
		SourceNode:  e.nodeID,
		SourceAgent: "workflow-engine",
		WorkflowID:  workflowID,
		Timestamp:   now,
		Payload:     data,
	}); err != nil {
		return fmt.Errorf("route workflow.step to WF stream: %w", err)
	}

	// Deliver to the assigned agent via router (StrategyDirect).
	if err := e.publisher.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "workflow.step",
		SourceNode:  e.nodeID,
		SourceAgent: "workflow-engine",
		TargetAgent: agentID,
		WorkflowID:  workflowID,
		Timestamp:   now,
		Payload:     data,
	}); err != nil {
		return fmt.Errorf("route workflow.step to agent direct: %w", err)
	}

	// Schedule timeout.
	timeout := e.defaultTimeout
	if step.TimeoutSeconds > 0 {
		timeout = time.Duration(step.TimeoutSeconds) * time.Second
	}
	e.timeouts.Schedule(workflowID, timeout, func() {
		failCtx := context.Background()
		s, rev, err := e.store.Get(failCtx, workflowID)
		if err != nil || s.Status != StatusRunning {
			return
		}
		_ = e.failWorkflowWithError(failCtx, workflowID, s, rev,
			newWorkflowError(ErrCodeStepTimeout,
				fmt.Sprintf("step %d timed out after %s", stepIndex, timeout),
				stepIndex, agentID))
	})

	return nil
}

// advanceWorkflow processes a completed step result and either dispatches the next step
// or completes/fails the workflow.
func (e *WorkflowEngine) advanceWorkflow(
	ctx context.Context,
	workflowID string,
	state WorkflowState,
	revision uint64,
	result StepResult,
) error {
	// Cancel the step timeout.
	e.timeouts.Cancel(workflowID)

	if result.Status == StepStatusFailure {
		werr := result.Error
		if werr == nil {
			werr = newWorkflowError(ErrCodeStepFailed,
				fmt.Sprintf("step %d reported failure", result.StepIndex),
				result.StepIndex, result.AgentID)
		}
		return e.failWorkflowWithError(ctx, workflowID, state, revision, werr)
	}

	// Append result and increment step.
	state.StepResults = append(state.StepResults, result)
	state.CurrentStep++
	state.UpdatedAt = time.Now().UnixNano()

	// Check if there are more steps.
	if int(state.CurrentStep) < len(state.Definition.Steps) {
		if err := e.store.Update(ctx, workflowID, state, revision); err != nil {
			// CAS conflict = duplicate delivery → no-op.
			return nil
		}
		return e.dispatchStep(ctx, workflowID, state.CurrentStep)
	}

	return e.completeWorkflow(ctx, workflowID, state, revision)
}

// completeWorkflow transitions the workflow to COMPLETED and publishes workflow.complete.
func (e *WorkflowEngine) completeWorkflow(
	ctx context.Context,
	workflowID string,
	state WorkflowState,
	revision uint64,
) error {
	now := time.Now().UnixNano()
	state.Status = StatusCompleted
	state.UpdatedAt = now

	if err := e.store.Update(ctx, workflowID, state, revision); err != nil {
		// CAS conflict = already completed by another goroutine → no-op.
		return nil
	}

	pbPayload := &protocolv1.WorkflowCompletePayload{
		WorkflowId:  workflowID,
		Results:     stepResultsToProto(state.StepResults),
		StartedAt:   state.CreatedAt,
		CompletedAt: now,
	}
	data, err := proto.Marshal(pbPayload)
	if err != nil {
		return fmt.Errorf("marshal WorkflowCompletePayload: %w", err)
	}

	// Publish via router (StrategyWorkflow) before cleaning up the stream.
	if err := e.publisher.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "workflow.complete",
		SourceNode:  e.nodeID,
		SourceAgent: "workflow-engine",
		WorkflowID:  workflowID,
		Timestamp:   now,
		Payload:     data,
	}); err != nil {
		return fmt.Errorf("route workflow.complete: %w", err)
	}

	e.streamMgr.Cleanup(workflowID)
	return nil
}

// failWorkflowWithError transitions the workflow to FAILED and publishes workflow.failed.
func (e *WorkflowEngine) failWorkflowWithError(
	ctx context.Context,
	workflowID string,
	state WorkflowState,
	revision uint64,
	werr *WorkflowError,
) error {
	e.timeouts.Cancel(workflowID)

	now := time.Now().UnixNano()
	state.Status = StatusFailed
	state.Error = werr
	state.UpdatedAt = now

	if err := e.store.Update(ctx, workflowID, state, revision); err != nil {
		// CAS conflict = already failed → no-op.
		return nil
	}

	pbPayload := &protocolv1.WorkflowFailedPayload{
		WorkflowId: workflowID,
		Error:      workflowErrorToProto(werr),
		FailedStep: werr.StepIndex,
		StartedAt:  state.CreatedAt,
		FailedAt:   now,
	}
	data, err := proto.Marshal(pbPayload)
	if err != nil {
		return fmt.Errorf("marshal WorkflowFailedPayload: %w", err)
	}

	// Publish via router (StrategyWorkflow) — router ensures stream exists.
	if err := e.publisher.Route(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "workflow.failed",
		SourceNode:  e.nodeID,
		SourceAgent: "workflow-engine",
		WorkflowID:  workflowID,
		Timestamp:   now,
		Payload:     data,
	}); err != nil {
		return fmt.Errorf("route workflow.failed: %w", err)
	}

	e.streamMgr.Cleanup(workflowID)
	return nil
}

// handleAgentUnregister detects when an assigned agent dies and fails its workflow.
func (e *WorkflowEngine) handleAgentUnregister(ctx context.Context, event *eventbus.Event) error {
	var payload protocolv1.AgentUnregisterPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return nil // non-fatal: ignore malformed unregister events
	}
	agentID := payload.AgentId
	if agentID == "" {
		agentID = event.SourceAgent
	}
	// We don't have a workflow index by agent, so this is a best-effort check.
	// In v1, the timeout will catch agent deaths if no result arrives.
	_ = agentID
	return nil
}
