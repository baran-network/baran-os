package sdk

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// StepHandler is a function that processes a workflow step.
// It receives the step context and returns result bytes or an error.
type StepHandler func(ctx context.Context, step *StepContext) ([]byte, error)

// StepContext provides metadata and input for a workflow step invocation.
type StepContext struct {
	WorkflowID      string
	StepIndex       uint32
	StepName        string
	Capability      string
	Input           []byte
	PreviousResults []StepResult
}

// StepResult contains the result from a previous workflow step (read-only).
type StepResult struct {
	StepIndex uint32
	Status    string // "SUCCESS" or "FAILURE"
	Result    []byte
}

// subscribeDirect registers the agent to receive direct workflow step events
// on the subject pattern agent.direct.{agentID}.>. Called during Start.
func (a *Agent) subscribeDirect(ctx context.Context) error {
	subject := fmt.Sprintf("agent.direct.%s.>", a.id)
	sub, err := a.bus.Subscribe(ctx, subject, func(ctx context.Context, event *eventbus.Event) error {
		return a.dispatchStep(ctx, event)
	})
	if err != nil {
		return fmt.Errorf("subscribe direct %s: %w", subject, err)
	}

	a.mu.Lock()
	a.subs = append(a.subs, sub)
	a.mu.Unlock()

	return nil
}

// dispatchStep handles an incoming direct event, routing it to the appropriate
// registered capability handler.
func (a *Agent) dispatchStep(ctx context.Context, event *eventbus.Event) error {
	// Idempotency check — skip duplicate events.
	if a.cache.Has(event.ID) {
		a.logger.Debug("duplicate event, skipping", "event_id", event.ID)
		return nil
	}

	payload, err := unmarshalStepPayload(event.Payload)
	if err != nil {
		a.logger.Warn("failed to unmarshal step payload", "error", err, "event_id", event.ID)
		return nil
	}

	capName := ""
	if payload.Step != nil {
		capName = payload.Step.Capability
	}

	a.mu.RLock()
	entry, ok := a.capabilities[capName]
	a.mu.RUnlock()

	if !ok {
		a.logger.Warn("no handler for capability", "capability", capName, "event_id", event.ID)
		// Do not publish result — engine will timeout per spec
		return nil
	}

	stepCtx := buildStepContext(payload)

	// Run in a goroutine so the subscription is non-blocking.
	a.inFlight.Add(1)
	go func() {
		defer a.inFlight.Done()
		result, handlerErr := safeInvoke(entry.handler, ctx, stepCtx)
		if err := a.publishStepResult(ctx, payload.WorkflowId, payload.StepIndex, result, handlerErr); err != nil {
			a.logger.Warn("failed to publish step result", "error", err)
		}
		a.cache.Add(event.ID)
	}()

	return nil
}

// buildStepContext maps a WorkflowStepPayload to the SDK's StepContext type.
func buildStepContext(payload *protocolv1.WorkflowStepPayload) *StepContext {
	prev := make([]StepResult, 0, len(payload.PreviousResults))
	for _, r := range payload.PreviousResults {
		status := "FAILURE"
		if r.Status == protocolv1.StepStatus_STEP_STATUS_SUCCESS {
			status = "SUCCESS"
		}
		prev = append(prev, StepResult{
			StepIndex: r.StepIndex,
			Status:    status,
			Result:    r.Result,
		})
	}

	stepName := ""
	capName := ""
	var input []byte
	if payload.Step != nil {
		stepName = payload.Step.Name
		capName = payload.Step.Capability
		input = payload.Step.Input
	}
	if payload.Input != nil {
		input = payload.Input
	}

	return &StepContext{
		WorkflowID:      payload.WorkflowId,
		StepIndex:       payload.StepIndex,
		StepName:        stepName,
		Capability:      capName,
		Input:           input,
		PreviousResults: prev,
	}
}

// safeInvoke calls a StepHandler with panic recovery, converting any panic
// into a regular error.
func safeInvoke(handler StepHandler, ctx context.Context, step *StepContext) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			err = fmt.Errorf("handler panicked: %v\n%s", r, stack)
		}
	}()
	return handler(ctx, step)
}

// publishStepResult marshals and publishes a WorkflowStepResultPayload to the
// per-workflow stream. Subject: workflow.{workflowID}.workflow.step.result.
func (a *Agent) publishStepResult(ctx context.Context, workflowID string, stepIndex uint32, result []byte, handlerErr error) error {
	pbPayload := &protocolv1.WorkflowStepResultPayload{
		StepIndex: stepIndex,
		Status:    protocolv1.StepStatus_STEP_STATUS_SUCCESS,
		Result:    result,
	}

	if handlerErr != nil {
		pbPayload.Status = protocolv1.StepStatus_STEP_STATUS_FAILURE
		pbPayload.Result = nil
		pbPayload.Error = &protocolv1.WorkflowError{
			Code:      "STEP_FAILED",
			Message:   handlerErr.Error(),
			StepIndex: stepIndex,
			AgentId:   a.id,
		}
	}

	eventType := "workflow." + workflowID + ".workflow.step.result"

	data, err := proto.Marshal(pbPayload)
	if err != nil {
		return fmt.Errorf("marshal step result: %w", err)
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate event ID: %w", err)
	}

	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        eventType,
		SourceAgent: a.id,
		WorkflowID:  workflowID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}

	return a.bus.Publish(ctx, event)
}

// stripWorkflowPrefix removes the "workflow.{id}." prefix from an event type,
// returning only the suffix (e.g., "workflow.step.result").
func stripWorkflowPrefix(eventType string) string {
	if !strings.HasPrefix(eventType, "workflow.") {
		return eventType
	}
	rest := eventType[9:]
	dot := strings.Index(rest, ".")
	if dot < 0 {
		return eventType
	}
	return "workflow." + rest[dot+1:]
}
