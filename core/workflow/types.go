package workflow

import "time"

// WorkflowStatus represents the lifecycle state of a workflow.
type WorkflowStatus int

const (
	StatusUnspecified WorkflowStatus = iota
	StatusCreated
	StatusRunning
	StatusCompleted
	StatusFailed
)

// StepStatus represents the outcome of a completed step.
type StepStatus int

const (
	StepStatusUnspecified StepStatus = iota
	StepStatusSuccess
	StepStatusFailure
)

// StepDefinition describes a single unit of work within a workflow.
type StepDefinition struct {
	Name           string
	Capability     string
	TimeoutSeconds uint32
	Input          []byte
}

// WorkflowDefinition describes the structure of a workflow.
type WorkflowDefinition struct {
	Name      string
	Steps     []StepDefinition
	Initiator string
}

// StepResult holds the output from a completed step.
type StepResult struct {
	StepIndex   uint32
	AgentID     string
	Status      StepStatus
	Result      []byte
	Error       *WorkflowError
	CompletedAt int64 // Unix nanos
}

// WorkflowState is the persisted representation of a running workflow.
type WorkflowState struct {
	WorkflowID    string
	Status        WorkflowStatus
	Definition    WorkflowDefinition
	CurrentStep   uint32
	StepResults   []StepResult
	AssignedAgent string
	Error         *WorkflowError
	CreatedAt     int64 // Unix nanos
	UpdatedAt     int64 // Unix nanos
}

// DefaultStepTimeout is used when a step's TimeoutSeconds is 0.
const DefaultStepTimeout = 30 * time.Second
