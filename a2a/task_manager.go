package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// TaskManager bridges A2A tasks to Baran workflows.
// For agents with origin="a2a", it dispatches directly to the external A2A endpoint.
type TaskManager struct {
	bus        eventbus.EventBus
	registry   registry.AgentRegistry
	store      workflow.WorkflowStateStore
	httpClient *http.Client
	logger     *slog.Logger
}

// NewTaskManager creates a TaskManager with a default HTTP client.
func NewTaskManager(bus eventbus.EventBus, reg registry.AgentRegistry, store workflow.WorkflowStateStore, logger *slog.Logger) *TaskManager {
	return &TaskManager{
		bus:        bus,
		registry:   reg,
		store:      store,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		logger:     logger,
	}
}

// CreateTask finds an agent for the requested skill, then either:
//   - dispatches directly to an external A2A agent (origin="a2a"), or
//   - creates a single-step Baran workflow and returns the task (workflow) ID.
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

	// If the best-match agent is an external A2A virtual agent, proxy the request.
	if agents[0].Origin == "a2a" {
		return m.dispatchToVirtualAgent(ctx, agents[0], skill, params)
	}

	// Standard path: create a Baran workflow.
	return m.createBaranWorkflow(ctx, skill, agents[0], params)
}

// createBaranWorkflow publishes a workflow.start event for a local/remote agent.
func (m *TaskManager) createBaranWorkflow(ctx context.Context, skill string, _ registry.AgentRegistration, params SendMessageParams) (*Task, error) {
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

	m.logger.Info("a2a task created via baran workflow", "task_id", workflowID, "skill", skill)

	return &Task{
		ID:     workflowID,
		Status: NewTaskStatus(TaskStateSubmitted),
	}, nil
}

// dispatchToVirtualAgent proxies a task to an external A2A agent identified by
// the a2a_endpoint stored in the agent's capability parameters. It translates
// the incoming request to A2A message/send, polls tasks/get until a terminal
// state is reached, and returns the result as a Baran Task.
func (m *TaskManager) dispatchToVirtualAgent(ctx context.Context, agent registry.AgentRegistration, skill string, params SendMessageParams) (*Task, error) {
	endpoint := a2aEndpointFromAgent(agent, skill)
	if endpoint == "" {
		return nil, &A2AError{Code: ErrCodeInvalidParams, Message: fmt.Sprintf("a2a_endpoint not set on virtual agent %s", agent.AgentID)}
	}

	// Build the outbound message/send request.
	outReq := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "message/send",
		ID:      uuid.Must(uuid.NewV7()).String(),
		Params:  params,
	}

	body, err := json.Marshal(outReq)
	if err != nil {
		return nil, fmt.Errorf("marshal outbound request: %w", err)
	}

	baseURL := strings.TrimRight(endpoint, "/")
	httpResp, err := m.httpClient.Post(baseURL+"/", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("POST to external agent %s: %w", baseURL, err)
	}
	defer httpResp.Body.Close()

	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode response from external agent: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, &A2AError{Code: rpcResp.Error.Code, Message: rpcResp.Error.Message}
	}

	// Extract the remote task ID.
	taskData, err := json.Marshal(rpcResp.Result)
	if err != nil {
		return nil, fmt.Errorf("marshal task result: %w", err)
	}
	var remoteTask Task
	if err := json.Unmarshal(taskData, &remoteTask); err != nil {
		return nil, fmt.Errorf("unmarshal remote task: %w", err)
	}
	if remoteTask.ID == "" {
		return nil, fmt.Errorf("external agent returned empty task ID")
	}

	m.logger.Info("dispatched task to external A2A agent",
		"agent_id", agent.AgentID,
		"remote_task_id", remoteTask.ID,
		"skill", skill,
	)

	// Poll until terminal state or context cancellation.
	finalTask, err := m.pollUntilTerminal(ctx, baseURL, remoteTask.ID)
	if err != nil {
		// Return submitted task on poll error — caller can query later.
		m.logger.Warn("poll for external task result failed, returning submitted state",
			"remote_task_id", remoteTask.ID,
			"error", err,
		)
		return &Task{ID: remoteTask.ID, Status: NewTaskStatus(TaskStateSubmitted)}, nil
	}
	return finalTask, nil
}

// pollUntilTerminal polls tasks/get on the remote endpoint until the task
// reaches a terminal state (COMPLETED, FAILED, CANCELED) or ctx is cancelled.
// Poll interval: 1s, max duration: 5 minutes.
func (m *TaskManager) pollUntilTerminal(ctx context.Context, baseURL, taskID string) (*Task, error) {
	deadline := time.Now().Add(5 * time.Minute)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("timeout waiting for task %s", taskID)
			}

			task, err := m.getRemoteTask(baseURL, taskID)
			if err != nil {
				m.logger.Warn("poll error", "task_id", taskID, "error", err)
				continue
			}

			switch task.Status.State {
			case TaskStateCompleted, TaskStateFailed, TaskStateCanceled:
				return task, nil
			}
		}
	}
}

// getRemoteTask calls tasks/get on the remote A2A endpoint.
func (m *TaskManager) getRemoteTask(baseURL, taskID string) (*Task, error) {
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tasks/get",
		ID:      uuid.Must(uuid.NewV7()).String(),
		Params:  TaskQueryParams{ID: taskID},
	}
	body, _ := json.Marshal(req)

	resp, err := m.httpClient.Post(baseURL+"/", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("POST tasks/get: %w", err)
	}
	defer resp.Body.Close()

	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	data, _ := json.Marshal(rpcResp.Result)
	var task Task
	if err := json.Unmarshal(data, &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &task, nil
}

// a2aEndpointFromAgent extracts the a2a_endpoint from the agent's capability parameters.
func a2aEndpointFromAgent(agent registry.AgentRegistration, skill string) string {
	for _, cap := range agent.Capabilities {
		if cap.Name == skill {
			return cap.Parameters["a2a_endpoint"]
		}
	}
	// Fallback: use endpoint from any capability.
	for _, cap := range agent.Capabilities {
		if ep := cap.Parameters["a2a_endpoint"]; ep != "" {
			return ep
		}
	}
	return ""
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
