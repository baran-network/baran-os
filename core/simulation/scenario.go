package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// ScenarioSessionState represents the lifecycle state of a scenario session.
type ScenarioSessionState string

const (
	ScenarioStateRegistered ScenarioSessionState = "registered"
	ScenarioStateRunning    ScenarioSessionState = "running"
	ScenarioStateCompleted  ScenarioSessionState = "completed"
	ScenarioStateStopped    ScenarioSessionState = "stopped"
	ScenarioStateFailed     ScenarioSessionState = "failed"
)

// StepCondition defines an optional wait condition after a scenario step.
type StepCondition struct {
	ExpectEventType string `json:"expect_event_type"`
	TimeoutMs       int    `json:"timeout_ms"`
}

// ScenarioStep is a single step within a scenario definition.
type ScenarioStep struct {
	EventType     string            `json:"event_type"`
	DelayMs       int               `json:"delay_ms"`
	SourceAgent   string            `json:"source_agent"`
	TargetAgent   string            `json:"target_agent"`
	WorkflowID    string            `json:"workflow_id"`
	CorrelationID string            `json:"correlation_id"`
	PayloadJSON   json.RawMessage   `json:"payload_json"`
	Metadata      map[string]string `json:"metadata"`
	Condition     *StepCondition    `json:"condition"`
}

// ScenarioDefinition is a reusable blueprint for a simulation scenario.
type ScenarioDefinition struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Steps       []ScenarioStep `json:"steps"`
	CreatedAt   time.Time      `json:"created_at"`
}

// Validate checks that a scenario definition is well-formed.
func (sd *ScenarioDefinition) Validate() error {
	if sd.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(sd.Steps) == 0 {
		return fmt.Errorf("steps must not be empty")
	}
	for i, step := range sd.Steps {
		if step.EventType == "" {
			return fmt.Errorf("step %d: event_type is required", i)
		}
		if step.DelayMs < 0 {
			return fmt.Errorf("step %d: delay_ms must not be negative", i)
		}
		if step.Condition != nil {
			if step.Condition.ExpectEventType == "" {
				return fmt.Errorf("step %d: condition.expect_event_type is required", i)
			}
			if step.Condition.TimeoutMs <= 0 {
				return fmt.Errorf("step %d: condition.timeout_ms must be > 0", i)
			}
		}
	}
	return nil
}

// ScenarioSession represents a single execution of a scenario definition.
type ScenarioSession struct {
	ID             string               `json:"id"`
	ScenarioID     string               `json:"scenario_id"`
	ScenarioName   string               `json:"scenario_name"`
	State          ScenarioSessionState `json:"state"`
	CurrentStep    int                  `json:"current_step"`
	TotalSteps     int                  `json:"total_steps"`
	InjectedEvents int                  `json:"injected_events"`
	ErrorMessage   string               `json:"error_message"`
	CreatedAt      time.Time            `json:"created_at"`
	StartedAt      *time.Time           `json:"started_at"`
	CompletedAt    *time.Time           `json:"completed_at"`
	DurationMs     int64                `json:"duration_ms"`

	cancel context.CancelFunc
	mu     sync.Mutex
}

// snapshot returns a copy of the session safe for external use.
func (s *ScenarioSession) snapshot() *ScenarioSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &ScenarioSession{
		ID:             s.ID,
		ScenarioID:     s.ScenarioID,
		ScenarioName:   s.ScenarioName,
		State:          s.State,
		CurrentStep:    s.CurrentStep,
		TotalSteps:     s.TotalSteps,
		InjectedEvents: s.InjectedEvents,
		ErrorMessage:   s.ErrorMessage,
		CreatedAt:      s.CreatedAt,
		StartedAt:      s.StartedAt,
		CompletedAt:    s.CompletedAt,
		DurationMs:     s.DurationMs,
	}
}

// ScenarioEventNotification is delivered to SSE watchers for scenario session events.
type ScenarioEventNotification struct {
	Name string // SSE event name: scenario.event, scenario.complete, scenario.stopped, scenario.failed
	Data string // JSON-encoded payload
}

// scenarioWatcherSet manages SSE subscriber channels for a single scenario session.
type scenarioWatcherSet struct {
	mu       sync.Mutex
	clients  map[string]chan ScenarioEventNotification
	terminal *ScenarioEventNotification
}

func (s *scenarioWatcherSet) subscribe(clientID string) chan ScenarioEventNotification {
	ch := make(chan ScenarioEventNotification, 64)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal != nil {
		ch <- *s.terminal
		close(ch)
		return ch
	}
	s.clients[clientID] = ch
	return ch
}

func (s *scenarioWatcherSet) unsubscribe(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, clientID)
}

func (s *scenarioWatcherSet) broadcast(evt ScenarioEventNotification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.clients {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (s *scenarioWatcherSet) terminate(evt ScenarioEventNotification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminal = &evt
	for clientID, ch := range s.clients {
		select {
		case ch <- evt:
		default:
		}
		close(ch)
		delete(s.clients, clientID)
	}
}

// ScenarioEngine manages scenario registration, session lifecycle, and execution.
type ScenarioEngine struct {
	injector        *EventInjector
	js              jetstream.JetStream
	scenarios       sync.Map // map[string]*ScenarioDefinition
	sessions        sync.Map // map[string]*ScenarioSession
	sessionWatchers sync.Map // map[string]*scenarioWatcherSet
}

// NewScenarioEngine creates a ScenarioEngine.
func NewScenarioEngine(injector *EventInjector, js jetstream.JetStream) *ScenarioEngine {
	return &ScenarioEngine{
		injector: injector,
		js:       js,
	}
}

// RegisterScenario registers a new scenario definition.
// Returns an error if a scenario with the same name already exists.
func (e *ScenarioEngine) RegisterScenario(def *ScenarioDefinition) (*ScenarioDefinition, error) {
	if err := def.Validate(); err != nil {
		return nil, err
	}

	// Check for duplicate name.
	var duplicate bool
	e.scenarios.Range(func(_, v interface{}) bool {
		if v.(*ScenarioDefinition).Name == def.Name {
			duplicate = true
			return false
		}
		return true
	})
	if duplicate {
		return nil, fmt.Errorf("scenario with name '%s' already exists", def.Name)
	}

	def.ID = uuid.Must(uuid.NewV7()).String()
	def.CreatedAt = time.Now()
	e.scenarios.Store(def.ID, def)
	return def, nil
}

// GetScenario returns a scenario definition by ID, or nil if not found.
func (e *ScenarioEngine) GetScenario(id string) *ScenarioDefinition {
	v, ok := e.scenarios.Load(id)
	if !ok {
		return nil
	}
	return v.(*ScenarioDefinition)
}

// ListScenarios returns all registered scenario definitions.
func (e *ScenarioEngine) ListScenarios() []*ScenarioDefinition {
	var result []*ScenarioDefinition
	e.scenarios.Range(func(_, v interface{}) bool {
		result = append(result, v.(*ScenarioDefinition))
		return true
	})
	return result
}

// StartScenario creates a new session for the given scenario and begins execution.
func (e *ScenarioEngine) StartScenario(ctx context.Context, scenarioID string) (*ScenarioSession, error) {
	def := e.GetScenario(scenarioID)
	if def == nil {
		return nil, fmt.Errorf("scenario not found: %s", scenarioID)
	}

	sessionID := uuid.Must(uuid.NewV7()).String()
	now := time.Now()
	session := &ScenarioSession{
		ID:           sessionID,
		ScenarioID:   def.ID,
		ScenarioName: def.Name,
		State:        ScenarioStateRunning,
		TotalSteps:   len(def.Steps),
		CreatedAt:    now,
		StartedAt:    &now,
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	session.cancel = cancel
	e.sessions.Store(sessionID, session)

	go e.executeScenario(sessionCtx, session, def)

	return session.snapshot(), nil
}

// StopSession stops a running scenario session.
func (e *ScenarioEngine) StopSession(sessionID string) error {
	v, ok := e.sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	session := v.(*ScenarioSession)
	session.mu.Lock()
	if session.State != ScenarioStateRunning {
		state := session.State
		session.mu.Unlock()
		return fmt.Errorf("session is not running (current state: %s)", state)
	}
	if session.cancel != nil {
		session.cancel()
	}
	session.mu.Unlock()
	return nil
}

// GetSession returns a snapshot of the session with the given ID, or nil if not found.
func (e *ScenarioEngine) GetSession(sessionID string) *ScenarioSession {
	v, ok := e.sessions.Load(sessionID)
	if !ok {
		return nil
	}
	return v.(*ScenarioSession).snapshot()
}

// ListSessions returns snapshots of all sessions, optionally filtered by state.
func (e *ScenarioEngine) ListSessions(stateFilter string) []*ScenarioSession {
	var result []*ScenarioSession
	e.sessions.Range(func(_, v interface{}) bool {
		s := v.(*ScenarioSession)
		snap := s.snapshot()
		if stateFilter == "" || string(snap.State) == stateFilter {
			result = append(result, snap)
		}
		return true
	})
	return result
}

// WatchSession registers an SSE subscriber for the given scenario session.
func (e *ScenarioEngine) WatchSession(sessionID string) (<-chan ScenarioEventNotification, func(), error) {
	if _, ok := e.sessions.Load(sessionID); !ok {
		return nil, nil, fmt.Errorf("session not found: %s", sessionID)
	}
	clientID := uuid.Must(uuid.NewV7()).String()
	ws := e.watcherSet(sessionID)
	ch := ws.subscribe(clientID)
	return ch, func() { ws.unsubscribe(clientID) }, nil
}

func (e *ScenarioEngine) watcherSet(sessionID string) *scenarioWatcherSet {
	actual, _ := e.sessionWatchers.LoadOrStore(sessionID, &scenarioWatcherSet{
		clients: make(map[string]chan ScenarioEventNotification),
	})
	return actual.(*scenarioWatcherSet)
}

// executeScenario runs the scenario step by step in a goroutine.
func (e *ScenarioEngine) executeScenario(ctx context.Context, session *ScenarioSession, def *ScenarioDefinition) {
	startedAt := time.Now()

	// Publish simulation.start coordination event.
	_ = e.injector.PublishCoordination(ctx, "simulation.start", session.ID, &protocolv1.SimulationStartPayload{
		SessionId:    session.ID,
		ScenarioName: def.Name,
		TotalSteps:   int32(len(def.Steps)),
	})

	for i, step := range def.Steps {
		// Check for cancellation before each step.
		select {
		case <-ctx.Done():
			e.terminateSession(session, ScenarioStateStopped, "operator_request", "", startedAt)
			return
		default:
		}

		// Apply inter-step delay.
		if step.DelayMs > 0 {
			select {
			case <-ctx.Done():
				e.terminateSession(session, ScenarioStateStopped, "operator_request", "", startedAt)
				return
			case <-time.After(time.Duration(step.DelayMs) * time.Millisecond):
			}
		}

		// Inject the synthetic event.
		req := InjectRequest{
			EventType:     step.EventType,
			SourceAgent:   step.SourceAgent,
			TargetAgent:   step.TargetAgent,
			WorkflowID:    step.WorkflowID,
			CorrelationID: step.CorrelationID,
			PayloadJSON:   step.PayloadJSON,
			Metadata:      step.Metadata,
		}

		result, err := e.injector.Inject(ctx, req, session.ID, def.Name)
		if err != nil {
			errMsg := fmt.Sprintf("step %d: inject failed: %s", i, err.Error())
			e.terminateSession(session, ScenarioStateFailed, "error", errMsg, startedAt)
			return
		}

		session.mu.Lock()
		session.CurrentStep = i + 1
		session.InjectedEvents++
		injected := session.InjectedEvents
		session.mu.Unlock()

		// Publish inject_event coordination event.
		_ = e.injector.PublishCoordination(ctx, "simulation.inject_event", session.ID, &protocolv1.SimulationInjectEventPayload{
			SessionId:         session.ID,
			StepIndex:         int32(i),
			OriginalEventType: step.EventType,
		})

		// Broadcast to SSE watchers.
		evtData, _ := json.Marshal(map[string]interface{}{
			"event_id":   result.EventID,
			"event_type": step.EventType,
			"step_index": i,
			"sequence":   result.Sequence,
			"injected":   injected,
			"timestamp":  time.Now().UTC().Format(time.RFC3339),
		})
		e.watcherSet(session.ID).broadcast(ScenarioEventNotification{
			Name: "scenario.event",
			Data: string(evtData),
		})

		// Handle condition (wait for expected event on SIMULATION stream).
		if step.Condition != nil {
			if err := e.waitForCondition(ctx, session, step.Condition, i); err != nil {
				errMsg := fmt.Sprintf("step %d: %s", i, err.Error())
				e.terminateSession(session, ScenarioStateFailed, "condition_timeout", errMsg, startedAt)
				return
			}
		}
	}

	// All steps completed successfully.
	e.terminateSession(session, ScenarioStateCompleted, "completed", "", startedAt)
}

// waitForCondition subscribes to the SIMULATION stream and waits for the expected event type.
func (e *ScenarioEngine) waitForCondition(ctx context.Context, session *ScenarioSession, cond *StepCondition, stepIndex int) error {
	timeout := time.Duration(cond.TimeoutMs) * time.Millisecond

	// Create an ephemeral ordered consumer on the SIMULATION stream.
	cons, err := e.js.CreateConsumer(ctx, "SIMULATION", jetstream.ConsumerConfig{
		FilterSubject: "simulation.>",
		DeliverPolicy: jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return fmt.Errorf("create condition consumer: %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		msg, err := cons.Next(jetstream.FetchMaxWait(timeout))
		if err != nil {
			select {
			case <-timeoutCtx.Done():
				if ctx.Err() != nil {
					return fmt.Errorf("cancelled while waiting for %s", cond.ExpectEventType)
				}
				return fmt.Errorf("expected %s within %dms", cond.ExpectEventType, cond.TimeoutMs)
			default:
				return fmt.Errorf("expected %s within %dms", cond.ExpectEventType, cond.TimeoutMs)
			}
		}

		// Parse the event to check its type.
		var evt protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data(), &evt); err != nil {
			_ = msg.Ack()
			continue
		}
		_ = msg.Ack()

		if evt.Type == cond.ExpectEventType {
			return nil
		}

		// Check timeout.
		select {
		case <-timeoutCtx.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("cancelled while waiting for %s", cond.ExpectEventType)
			}
			return fmt.Errorf("expected %s within %dms", cond.ExpectEventType, cond.TimeoutMs)
		default:
		}
	}
}

// terminateSession transitions a session to a terminal state and broadcasts the terminal event.
func (e *ScenarioEngine) terminateSession(session *ScenarioSession, state ScenarioSessionState, reason, errMsg string, startedAt time.Time) {
	now := time.Now()
	durationMs := now.Sub(startedAt).Milliseconds()

	session.mu.Lock()
	session.State = state
	session.CompletedAt = &now
	session.DurationMs = durationMs
	if errMsg != "" {
		session.ErrorMessage = errMsg
	}
	injected := session.InjectedEvents
	session.mu.Unlock()

	// Publish simulation.stop coordination event.
	_ = e.injector.PublishCoordination(context.Background(), "simulation.stop", session.ID, &protocolv1.SimulationStopPayload{
		SessionId:      session.ID,
		Reason:         reason,
		InjectedEvents: int32(injected),
		DurationMs:     durationMs,
		ErrorMessage:   errMsg,
	})

	// Determine SSE event name based on terminal state.
	var sseEventName string
	switch state {
	case ScenarioStateCompleted:
		sseEventName = "scenario.complete"
	case ScenarioStateStopped:
		sseEventName = "scenario.stopped"
	case ScenarioStateFailed:
		sseEventName = "scenario.failed"
	}

	data, _ := json.Marshal(map[string]interface{}{
		"session_id":      session.ID,
		"reason":          reason,
		"injected_events": injected,
		"duration_ms":     durationMs,
		"error_message":   errMsg,
	})
	e.watcherSet(session.ID).terminate(ScenarioEventNotification{
		Name: sseEventName,
		Data: string(data),
	})
}
