package a2a

import "time"

// TaskState represents the A2A task lifecycle state.
type TaskState string

const (
	TaskStateSubmitted     TaskState = "TASK_STATE_SUBMITTED"
	TaskStateWorking       TaskState = "TASK_STATE_WORKING"
	TaskStateCompleted     TaskState = "TASK_STATE_COMPLETED"
	TaskStateFailed        TaskState = "TASK_STATE_FAILED"
	TaskStateCanceled      TaskState = "TASK_STATE_CANCELED"
	TaskStateInputRequired TaskState = "TASK_STATE_INPUT_REQUIRED"
)

// AgentCard is the A2A Agent Card returned by discovery.
type AgentCard struct {
	Name                string              `json:"name"`
	Description         string              `json:"description"`
	Version             string              `json:"version"`
	SupportedInterfaces []SupportedInterface `json:"supported_interfaces"`
	Capabilities        AgentCapabilities   `json:"capabilities"`
	DefaultInputModes   []string            `json:"default_input_modes"`
	DefaultOutputModes  []string            `json:"default_output_modes"`
	Skills              []AgentSkill        `json:"skills"`
}

// SupportedInterface describes a protocol binding.
type SupportedInterface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocol_binding"`
	ProtocolVersion string `json:"protocol_version"`
}

// AgentCapabilities describes protocol features supported.
type AgentCapabilities struct {
	Streaming         bool `json:"streaming"`
	PushNotifications bool `json:"pushNotifications"`
	ExtendedAgentCard bool `json:"extendedAgentCard"`
}

// AgentSkill maps a Baran capability to an A2A skill.
type AgentSkill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	InputModes  []string `json:"input_modes"`
	OutputModes []string `json:"output_modes"`
}

// Task represents an A2A task with its status and artifacts.
type Task struct {
	ID        string     `json:"id"`
	Status    TaskStatus `json:"status"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
}

// TaskStatus holds the current state and timestamp of a task.
type TaskStatus struct {
	State     TaskState `json:"state"`
	UpdatedAt string    `json:"updatedAt"`
}

// NewTaskStatus creates a TaskStatus with the current time.
func NewTaskStatus(state TaskState) TaskStatus {
	return TaskStatus{
		State:     state,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

// Message represents an A2A message with role and parts.
type Message struct {
	MessageID string `json:"message_id"`
	Role      string `json:"role"`
	Parts     []Part `json:"parts"`
}

// Part is a union type for message content.
type Part struct {
	Text string `json:"text,omitempty"`
	Data *Data  `json:"data,omitempty"`
}

// Data represents structured data in a message part.
type Data struct {
	MimeType string `json:"mimeType"`
	Content  any    `json:"content"`
}

// Artifact represents an output artifact from task execution.
type Artifact struct {
	ArtifactID string `json:"artifact_id"`
	Parts      []Part `json:"parts"`
}

// SendMessageParams holds the parameters for message/send.
type SendMessageParams struct {
	Message       Message       `json:"message"`
	Configuration SendTaskConfig `json:"configuration"`
}

// SendTaskConfig holds configuration for task dispatch.
type SendTaskConfig struct {
	AcceptedOutputModes []string `json:"acceptedOutputModes,omitempty"`
	Skill               string   `json:"skill"`
}

// TaskQueryParams holds the parameters for tasks/get and tasks/cancel.
type TaskQueryParams struct {
	ID string `json:"id"`
}

// JSONRPCRequest represents an incoming JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	ID      any    `json:"id"`
	Params  any    `json:"params,omitempty"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	ID      any           `json:"id"`
	Result  any           `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
}

// JSONRPCError represents a JSON-RPC 2.0 error object.
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Standard A2A error codes.
const (
	ErrCodeTaskNotFound           = -32001
	ErrCodeTaskNotCancelable      = -32002
	ErrCodeUnsupportedOperation   = -32003
	ErrCodeContentTypeNotSupported = -32004
	ErrCodeSkillNotFound          = -32005
	ErrCodeParseError             = -32700
	ErrCodeInvalidRequest         = -32600
	ErrCodeMethodNotFound         = -32601
	ErrCodeInvalidParams          = -32602
)
