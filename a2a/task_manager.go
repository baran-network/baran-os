package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// TaskManager bridges A2A tasks to Baran workflows.
type TaskManager struct {
	bus      eventbus.EventBus
	registry registry.AgentRegistry
	store    workflow.WorkflowStateStore
	logger   *slog.Logger
}

// NewTaskManager creates a TaskManager.
func NewTaskManager(bus eventbus.EventBus, reg registry.AgentRegistry, store workflow.WorkflowStateStore, logger *slog.Logger) *TaskManager {
	return &TaskManager{bus: bus, registry: reg, store: store, logger: logger}
}

// CreateTask finds an agent for the requested skill, creates a single-step
// workflow, and returns the task (workflow) ID.
func (m *TaskManager) CreateTask(ctx context.Context, params SendMessageParams) (*Task, error) {
	skill := params.Configuration.Skill
	if skill == "" {
		return nil, &A2AError{Code: ErrCodeInvalidParams, Message: "missing skill in configuration"}
	}

	agents, err := m.registry.FindByCapability(ctx, skill, "")
	if err != nil {
		return nil, fmt.Errorf("capability lookup: %w", err)
	}
	if len(agents) == 0 {
		return nil, &A2AError{Code: ErrCodeSkillNotFound, Message: fmt.Sprintf("no agent found for skill %q", skill)}
	}

	input, err := json.Marshal(params.Message)
	if err != nil {
		return nil, fmt.Errorf("marshal message: %w", err)
	}

	def := &protocolv1.WorkflowDefinition{
		Name:      fmt.Sprintf("a2a-task-%s", skill),
		Initiator: "a2a-gateway",
		Steps: []*protocolv1.StepDefinition{
			{
				Name:       skill,
				Capability: skill,
				Input:      input,
			},
		},
	}

	payload := &protocolv1.WorkflowStartPayload{Definition: def}
	data, err := proto.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow start: %w", err)
	}

	workflowID := uuid.Must(uuid.NewV7()).String()
	event := &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
		Metadata: map[string]string{
			"a2a_task":    "true",
			"workflow_id": workflowID,
		},
	}

	if err := m.bus.Publish(ctx, event); err != nil {
		return nil, fmt.Errorf("publish workflow.start: %w", err)
	}

	m.logger.Info("a2a task created", "task_id", workflowID, "skill", skill, "agent", agents[0].AgentID)

	return &Task{
		ID:     workflowID,
		Status: NewTaskStatus(TaskStateSubmitted),
	}, nil
}

// GetTask retrieves a task by querying the corresponding workflow state.
func (m *TaskManager) GetTask(ctx context.Context, taskID string) (*Task, error) {
	state, _, err := m.store.Get(ctx, taskID)
	if err != nil {
		return nil, &A2AError{Code: ErrCodeTaskNotFound, Message: fmt.Sprintf("task %q not found", taskID)}
	}

	task := &Task{
		ID:     taskID,
		Status: workflowStatusToTaskStatus(state),
	}

	if state.Status == workflow.StatusCompleted && len(state.StepResults) > 0 {
		lastResult := state.StepResults[len(state.StepResults)-1]
		if len(lastResult.Result) > 0 {
			task.Artifacts = []Artifact{
				{
					ArtifactID: "result-0",
					Parts:      []Part{{Text: string(lastResult.Result)}},
				},
			}
		}
	}

	return task, nil
}

// CancelTask sets the workflow to FAILED (Baran has no CANCELED state).
func (m *TaskManager) CancelTask(ctx context.Context, taskID string) (*Task, error) {
	state, revision, err := m.store.Get(ctx, taskID)
	if err != nil {
		return nil, &A2AError{Code: ErrCodeTaskNotFound, Message: fmt.Sprintf("task %q not found", taskID)}
	}

	if state.Status == workflow.StatusCompleted || state.Status == workflow.StatusFailed {
		return nil, &A2AError{Code: ErrCodeTaskNotCancelable, Message: "task already in terminal state"}
	}

	state.Status = workflow.StatusFailed
	state.UpdatedAt = time.Now().UnixNano()
	if err := m.store.Update(ctx, taskID, state, revision); err != nil {
		return nil, fmt.Errorf("update workflow state: %w", err)
	}

	m.logger.Info("a2a task canceled", "task_id", taskID)

	return &Task{
		ID:     taskID,
		Status: NewTaskStatus(TaskStateCanceled),
	}, nil
}

// workflowStatusToTaskStatus maps Baran workflow status to A2A task status.
func workflowStatusToTaskStatus(state workflow.WorkflowState) TaskStatus {
	var s TaskState
	switch state.Status {
	case workflow.StatusCreated:
		s = TaskStateSubmitted
	case workflow.StatusRunning:
		s = TaskStateWorking
	case workflow.StatusCompleted:
		s = TaskStateCompleted
	case workflow.StatusFailed:
		s = TaskStateFailed
	case workflow.StatusWaitingHuman:
		s = TaskStateInputRequired
	default:
		s = TaskStateSubmitted
	}

	ts := time.Unix(0, state.UpdatedAt).UTC().Format(time.RFC3339)
	return TaskStatus{State: s, UpdatedAt: ts}
}

// A2AError is a typed error for A2A JSON-RPC responses.
type A2AError struct {
	Code    int
	Message string
}

func (e *A2AError) Error() string {
	return fmt.Sprintf("a2a error %d: %s", e.Code, e.Message)
}
