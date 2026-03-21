package workflow

import "errors"

// Sentinel errors for workflow operations.
var (
	ErrWorkflowNotFound  = errors.New("workflow not found")
	ErrInvalidDefinition = errors.New("invalid workflow definition")
	ErrTerminalState     = errors.New("workflow is in a terminal state")
	ErrCASConflict       = errors.New("CAS conflict: revision mismatch")
)

// Error codes used in WorkflowError.
const (
	ErrCodeStepTimeout       = "STEP_TIMEOUT"
	ErrCodeAgentUnavailable  = "AGENT_UNAVAILABLE"
	ErrCodeStepFailed        = "STEP_FAILED"
	ErrCodeInvalidDefinition = "INVALID_DEFINITION"
	ErrCodeWorkflowNotFound  = "WORKFLOW_NOT_FOUND"
)

// WorkflowError holds structured error details for a failed step or workflow.
type WorkflowError struct {
	Code      string
	Message   string
	StepIndex uint32
	AgentID   string
}

func (e *WorkflowError) Error() string {
	return e.Code + ": " + e.Message
}

// newWorkflowError constructs a WorkflowError with the given code and message.
func newWorkflowError(code, message string, stepIndex uint32, agentID string) *WorkflowError {
	return &WorkflowError{
		Code:      code,
		Message:   message,
		StepIndex: stepIndex,
		AgentID:   agentID,
	}
}
