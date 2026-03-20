package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// handleWorkflowStart processes a workflow.start event.
// It validates the definition, creates workflow state, and dispatches the first step.
func (e *WorkflowEngine) handleWorkflowStart(ctx context.Context, event *eventbus.Event) error {
	var payload protocolv1.WorkflowStartPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal WorkflowStartPayload: %w", err)
	}

	def, err := workflowDefinitionFromProto(payload.Definition)
	if err != nil {
		return e.publishError(ctx, event, ErrCodeInvalidDefinition, err.Error())
	}

	return e.startWorkflow(ctx, def)
}

// handleWorkflowStepResult processes a workflow.step.result event.
func (e *WorkflowEngine) handleWorkflowStepResult(ctx context.Context, event *eventbus.Event) error {
	workflowID := event.WorkflowID
	if workflowID == "" {
		return fmt.Errorf("missing workflow_id in step result event")
	}

	var payload protocolv1.WorkflowStepResultPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal WorkflowStepResultPayload: %w", err)
	}

	state, revision, err := e.store.Get(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow state %s: %w", workflowID, err)
	}

	// Ignore events for terminal workflows (idempotency).
	if state.Status == StatusCompleted || state.Status == StatusFailed {
		return nil
	}

	result := StepResult{
		StepIndex:   payload.StepIndex,
		AgentID:     event.SourceAgent,
		Status:      StepStatus(payload.Status),
		Result:      payload.Result,
		CompletedAt: time.Now().UnixNano(),
	}
	if payload.Error != nil {
		result.Error = &WorkflowError{
			Code:      payload.Error.Code,
			Message:   payload.Error.Message,
			StepIndex: payload.Error.StepIndex,
			AgentID:   payload.Error.AgentId,
		}
	}

	return e.advanceWorkflow(ctx, workflowID, state, revision, result)
}

// handleWorkflowStateRequest responds to workflow.state.request events.
func (e *WorkflowEngine) handleWorkflowStateRequest(ctx context.Context, event *eventbus.Event) error {
	var payload protocolv1.WorkflowStateRequestPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal WorkflowStateRequestPayload: %w", err)
	}

	state, _, err := e.store.Get(ctx, payload.WorkflowId)
	if err != nil {
		if err == ErrWorkflowNotFound {
			return e.publishError(ctx, event, ErrCodeWorkflowNotFound,
				fmt.Sprintf("workflow %s not found", payload.WorkflowId))
		}
		return fmt.Errorf("get workflow state: %w", err)
	}

	respPayload := workflowStateResponseFromState(state)
	data, err := proto.Marshal(respPayload)
	if err != nil {
		return fmt.Errorf("marshal WorkflowStateResponsePayload: %w", err)
	}

	return e.bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "workflow.state.response",
		SourceNode:    e.nodeID,
		SourceAgent:   "workflow-engine",
		CorrelationID: event.CorrelationID,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
}

// publishError emits an agent.error event for the given event context.
func (e *WorkflowEngine) publishError(ctx context.Context, event *eventbus.Event, code, message string) error {
	errPayload := &protocolv1.AgentErrorPayload{
		ErrorCode:  code,
		Message:    message,
		WorkflowId: event.WorkflowID,
	}
	data, err := proto.Marshal(errPayload)
	if err != nil {
		return fmt.Errorf("marshal error payload: %w", err)
	}
	return e.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  e.nodeID,
		SourceAgent: "workflow-engine",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
}

// handleDecisionResponse processes a human.decision.response event.
func (e *WorkflowEngine) handleDecisionResponse(ctx context.Context, event *eventbus.Event) error {
	var payload protocolv1.HumanDecisionResponsePayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal HumanDecisionResponsePayload: %w", err)
	}

	return e.coordinator.ProcessResponse(ctx, &payload)
}

// workflowDefinitionFromProto converts a proto WorkflowDefinition to the Go type.
func workflowDefinitionFromProto(pb *protocolv1.WorkflowDefinition) (WorkflowDefinition, error) {
	if pb == nil {
		return WorkflowDefinition{}, fmt.Errorf("%w: definition is nil", ErrInvalidDefinition)
	}
	if len(pb.Steps) == 0 {
		return WorkflowDefinition{}, fmt.Errorf("%w: must have at least 1 step", ErrInvalidDefinition)
	}
	if len(pb.Steps) > 100 {
		return WorkflowDefinition{}, fmt.Errorf("%w: exceeds maximum of 100 steps", ErrInvalidDefinition)
	}
	steps := make([]StepDefinition, len(pb.Steps))
	for i, s := range pb.Steps {
		if s.Capability == "" && !s.HumanApproval {
			return WorkflowDefinition{}, fmt.Errorf("%w: step %d has empty capability", ErrInvalidDefinition, i)
		}
		steps[i] = StepDefinition{
			Name:           s.Name,
			Capability:     s.Capability,
			TimeoutSeconds: s.TimeoutSeconds,
			Input:          s.Input,
			HumanApproval:  s.HumanApproval,
			Prompt:         s.Prompt,
			ResourceIDs:    s.ResourceIds,
		}
	}
	return WorkflowDefinition{
		Name:      pb.Name,
		Steps:     steps,
		Initiator: pb.Initiator,
	}, nil
}

// workflowStateResponseFromState builds a proto response from a WorkflowState.
func workflowStateResponseFromState(state WorkflowState) *protocolv1.WorkflowStateResponsePayload {
	resp := &protocolv1.WorkflowStateResponsePayload{
		WorkflowId:    state.WorkflowID,
		Status:        protocolv1.WorkflowStatus(state.Status),
		CurrentStep:   state.CurrentStep,
		AssignedAgent: state.AssignedAgent,
		CreatedAt:     state.CreatedAt,
		UpdatedAt:     state.UpdatedAt,
	}
	if state.Definition.Name != "" {
		steps := make([]*protocolv1.StepDefinition, len(state.Definition.Steps))
		for i, s := range state.Definition.Steps {
			steps[i] = &protocolv1.StepDefinition{
				Name:           s.Name,
				Capability:     s.Capability,
				TimeoutSeconds: s.TimeoutSeconds,
				Input:          s.Input,
				HumanApproval:  s.HumanApproval,
				Prompt:         s.Prompt,
				ResourceIds:    s.ResourceIDs,
			}
		}
		resp.Definition = &protocolv1.WorkflowDefinition{
			Name:      state.Definition.Name,
			Steps:     steps,
			Initiator: state.Definition.Initiator,
		}
	}
	resp.StepResults = stepResultsToProto(state.StepResults)
	if state.Error != nil {
		resp.Error = workflowErrorToProto(state.Error)
	}
	return resp
}

func stepResultsToProto(results []StepResult) []*protocolv1.StepResult {
	if len(results) == 0 {
		return nil
	}
	out := make([]*protocolv1.StepResult, len(results))
	for i, r := range results {
		pr := &protocolv1.StepResult{
			StepIndex:   r.StepIndex,
			AgentId:     r.AgentID,
			Status:      protocolv1.StepStatus(r.Status),
			Result:      r.Result,
			CompletedAt: r.CompletedAt,
		}
		if r.Error != nil {
			pr.Error = workflowErrorToProto(r.Error)
		}
		out[i] = pr
	}
	return out
}

func workflowErrorToProto(e *WorkflowError) *protocolv1.WorkflowError {
	if e == nil {
		return nil
	}
	return &protocolv1.WorkflowError{
		Code:      e.Code,
		Message:   e.Message,
		StepIndex: e.StepIndex,
		AgentId:   e.AgentID,
	}
}
