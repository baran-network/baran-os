package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// SessionState represents the lifecycle state of a replay session.
type SessionState string

const (
	SessionStatePending   SessionState = "pending"
	SessionStateRunning   SessionState = "running"
	SessionStateCompleted SessionState = "completed"
	SessionStateStopped   SessionState = "stopped"
	SessionStateError     SessionState = "error"
)

// replaySubject is the NATS subject used to publish replayed data events.
// It routes to the SIMULATION stream (matches simulation.>) while preserving
// the original event type inside the protobuf payload.
const replaySubject = "simulation.replay.event"

// ReplayEventNotification is delivered to SSE watchers for each session event.
type ReplayEventNotification struct {
	Name string // SSE event name: replay.event, replay.complete, replay.stopped, replay.error
	Data string // JSON-encoded payload
}

// sessionWatcherSet manages SSE subscriber channels for a single replay session.
// It is safe for concurrent use.
type sessionWatcherSet struct {
	mu       sync.Mutex
	clients  map[string]chan ReplayEventNotification
	terminal *ReplayEventNotification // set on termination, for late subscribers
}

// subscribe registers a new SSE client and returns its event channel.
// If the session has already terminated, the terminal event is delivered immediately
// and the returned channel is already closed.
func (s *sessionWatcherSet) subscribe(clientID string) chan ReplayEventNotification {
	ch := make(chan ReplayEventNotification, 64)
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

// unsubscribe removes a client. Safe to call even if already removed.
func (s *sessionWatcherSet) unsubscribe(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, clientID)
}

// broadcast sends an event to all active clients. Slow clients are skipped.
func (s *sessionWatcherSet) broadcast(evt ReplayEventNotification) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ch := range s.clients {
		select {
		case ch <- evt:
		default:
		}
	}
}

// terminate sends the final event to all clients, closes their channels, and
// records the terminal state so late subscribers receive it immediately.
func (s *sessionWatcherSet) terminate(evt ReplayEventNotification) {
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

// ReplaySession represents a single replay execution with its lifecycle state.
type ReplaySession struct {
	ID             string
	WorkflowID     string
	State          SessionState
	Speed          float64
	TotalEvents    int
	ReplayedEvents int
	ErrorMessage   string
	CreatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time

	// cancel stops the session goroutine; nil if not yet started.
	cancel context.CancelFunc
	mu     sync.Mutex
}

// setState updates the session state under the session's lock.
func (s *ReplaySession) setState(state SessionState) {
	s.mu.Lock()
	s.State = state
	s.mu.Unlock()
}

// snapshot returns a copy of the session's data fields, safe to return to callers.
// It explicitly copies only the data fields to avoid copying the mutex (which is
// undefined behavior in Go when copied after first use).
func (s *ReplaySession) snapshot() *ReplaySession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &ReplaySession{
		ID:             s.ID,
		WorkflowID:     s.WorkflowID,
		State:          s.State,
		Speed:          s.Speed,
		TotalEvents:    s.TotalEvents,
		ReplayedEvents: s.ReplayedEvents,
		ErrorMessage:   s.ErrorMessage,
		CreatedAt:      s.CreatedAt,
		StartedAt:      s.StartedAt,
		CompletedAt:    s.CompletedAt,
		// cancel and mu intentionally omitted
	}
}

// ReplayEngine manages replay sessions and orchestrates event re-publishing.
type ReplayEngine struct {
	store           EventStore
	bus             eventbus.EventBus
	js              jetstream.JetStream
	nodeID          string
	sessions        sync.Map // map[string]*ReplaySession
	sessionWatchers sync.Map // map[string]*sessionWatcherSet
}

// NewReplayEngine creates a ReplayEngine backed by the given EventStore.
// bus is used to publish coordination events (simulation.replay.*).
// js is used to publish replayed data events directly to the SIMULATION stream.
func NewReplayEngine(store EventStore, bus eventbus.EventBus, js jetstream.JetStream, nodeID string) *ReplayEngine {
	return &ReplayEngine{store: store, bus: bus, js: js, nodeID: nodeID}
}

// CreateSession loads workflow events and registers a new PENDING replay session.
// Returns an error if the workflow has no events or loading fails.
func (e *ReplayEngine) CreateSession(ctx context.Context, workflowID string, speed float64) (*ReplaySession, error) {
	events, err := e.store.GetWorkflowEvents(ctx, workflowID)
	if err != nil {
		return nil, fmt.Errorf("load events for workflow %s: %w", workflowID, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no events found for workflow %s", workflowID)
	}

	id := uuid.Must(uuid.NewV7()).String()
	session := &ReplaySession{
		ID:          id,
		WorkflowID:  workflowID,
		State:       SessionStatePending,
		Speed:       speed,
		TotalEvents: len(events),
		CreatedAt:   time.Now(),
	}
	e.sessions.Store(id, session)

	// Start the session goroutine immediately.
	if err := e.startSession(ctx, session, events); err != nil {
		session.setState(SessionStateError)
		session.mu.Lock()
		session.ErrorMessage = err.Error()
		session.mu.Unlock()
		return session, nil
	}

	return session, nil
}

// startSession launches the replay goroutine for the given session.
func (e *ReplayEngine) startSession(ctx context.Context, session *ReplaySession, events []StoredEvent) error {
	sessionCtx, cancel := context.WithCancel(ctx)

	session.mu.Lock()
	now := time.Now()
	session.State = SessionStateRunning
	session.StartedAt = &now
	session.cancel = cancel
	session.mu.Unlock()

	go e.runSession(sessionCtx, session, events)
	return nil
}

// StopSession cancels a running session, transitioning it to STOPPED.
// It accesses the real session (not a snapshot) to invoke the cancel function.
func (e *ReplayEngine) StopSession(sessionID string) error {
	v, ok := e.sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	session := v.(*ReplaySession) // real session, not a snapshot
	session.mu.Lock()
	if session.State != SessionStateRunning {
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

// ListSessions returns snapshots of all known replay sessions, optionally filtered by state.
func (e *ReplayEngine) ListSessions(stateFilter string) []*ReplaySession {
	var result []*ReplaySession
	e.sessions.Range(func(_, v interface{}) bool {
		s := v.(*ReplaySession)
		snap := s.snapshot()
		if stateFilter == "" || string(snap.State) == stateFilter {
			result = append(result, snap)
		}
		return true
	})
	return result
}

// GetSession returns a snapshot of the session with the given ID, or nil if not found.
func (e *ReplayEngine) GetSession(id string) *ReplaySession {
	v, ok := e.sessions.Load(id)
	if !ok {
		return nil
	}
	return v.(*ReplaySession).snapshot()
}

// watcherSet returns (or creates) the watcher set for a session.
func (e *ReplayEngine) watcherSet(sessionID string) *sessionWatcherSet {
	actual, _ := e.sessionWatchers.LoadOrStore(sessionID, &sessionWatcherSet{
		clients: make(map[string]chan ReplayEventNotification),
	})
	return actual.(*sessionWatcherSet)
}

// WatchSession registers an SSE subscriber for the given session.
// Returns a channel that receives replay events and a cleanup function.
// If the session is already in a terminal state, the terminal event is delivered
// immediately and the channel is closed. Returns an error if the session does not exist.
func (e *ReplayEngine) WatchSession(sessionID string) (<-chan ReplayEventNotification, func(), error) {
	if _, ok := e.sessions.Load(sessionID); !ok {
		return nil, nil, fmt.Errorf("session not found: %s", sessionID)
	}
	clientID := uuid.Must(uuid.NewV7()).String()
	ws := e.watcherSet(sessionID)
	ch := ws.subscribe(clientID)
	return ch, func() { ws.unsubscribe(clientID) }, nil
}

// broadcastReplayEvent sends a replay.event notification to all SSE watchers.
func (e *ReplayEngine) broadcastReplayEvent(session *ReplaySession, se StoredEvent, newID string, seq uint64, position int) {
	meta := make(map[string]string, len(se.Event.Metadata)+4)
	for k, v := range se.Event.Metadata {
		meta[k] = v
	}
	meta["simulation.replay"] = "true"
	meta["simulation.session_id"] = session.ID
	meta["simulation.original_timestamp"] = fmt.Sprintf("%d", se.Event.Timestamp)
	meta["simulation.original_id"] = se.Event.ID

	evtMap := map[string]interface{}{
		"id":             newID,
		"type":           se.Event.Type,
		"source_node":    e.nodeID,
		"source_agent":   se.Event.SourceAgent,
		"target_agent":   se.Event.TargetAgent,
		"workflow_id":    se.Event.WorkflowID,
		"correlation_id": se.Event.CorrelationID,
		"metadata":       meta,
	}
	data, err := json.Marshal(map[string]interface{}{
		"event":    evtMap,
		"stream":   "SIMULATION",
		"sequence": seq,
		"position": position,
		"total":    session.TotalEvents,
	})
	if err != nil {
		return
	}
	e.watcherSet(session.ID).broadcast(ReplayEventNotification{Name: "replay.event", Data: string(data)})
}

func (e *ReplayEngine) broadcastStopped(sessionID string, replayed int) {
	data, _ := json.Marshal(map[string]interface{}{
		"session_id":     sessionID,
		"total_replayed": replayed,
	})
	e.watcherSet(sessionID).terminate(ReplayEventNotification{Name: "replay.stopped", Data: string(data)})
}

func (e *ReplayEngine) broadcastError(sessionID, errMsg string) {
	data, _ := json.Marshal(map[string]interface{}{
		"session_id": sessionID,
		"error":      errMsg,
	})
	e.watcherSet(sessionID).terminate(ReplayEventNotification{Name: "replay.error", Data: string(data)})
}

func (e *ReplayEngine) broadcastComplete(sessionID string, totalReplayed int) {
	data, _ := json.Marshal(map[string]interface{}{
		"session_id":     sessionID,
		"total_replayed": totalReplayed,
	})
	e.watcherSet(sessionID).terminate(ReplayEventNotification{Name: "replay.complete", Data: string(data)})
}

// runSession is the goroutine that replays events for a session.
func (e *ReplayEngine) runSession(ctx context.Context, session *ReplaySession, events []StoredEvent) {
	startedAt := time.Now()

	// Publish simulation.replay.start coordination event.
	_ = e.publishCoordination(ctx, "simulation.replay.start", session, &protocolv1.SimulationReplayStartPayload{
		SessionId:   session.ID,
		WorkflowId:  session.WorkflowID,
		Speed:       session.Speed,
		TotalEvents: int32(session.TotalEvents),
	})

	var prevTimestamp int64
	replayed := 0

	for i, se := range events {
		select {
		case <-ctx.Done():
			// Operator requested stop.
			session.mu.Lock()
			session.State = SessionStateStopped
			now := time.Now()
			session.CompletedAt = &now
			session.ReplayedEvents = replayed
			session.mu.Unlock()

			_ = e.publishCoordination(context.Background(), "simulation.replay.stop", session, &protocolv1.SimulationReplayStopPayload{
				SessionId:      session.ID,
				Reason:         "operator_request",
				ReplayedEvents: int32(replayed),
			})
			e.broadcastStopped(session.ID, replayed)
			return
		default:
		}

		// Apply speed-based inter-event delay (skip for first event).
		if i > 0 && session.Speed > 0 && prevTimestamp > 0 {
			gap := se.Event.Timestamp - prevTimestamp
			if gap > 0 {
				delay := time.Duration(float64(gap) / session.Speed)
				// Cap individual delay at 30s to avoid stalling.
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
				select {
				case <-ctx.Done():
					session.mu.Lock()
					session.State = SessionStateStopped
					now := time.Now()
					session.CompletedAt = &now
					session.ReplayedEvents = replayed
					session.mu.Unlock()
					_ = e.publishCoordination(context.Background(), "simulation.replay.stop", session, &protocolv1.SimulationReplayStopPayload{
						SessionId:      session.ID,
						Reason:         "operator_request",
						ReplayedEvents: int32(replayed),
					})
					e.broadcastStopped(session.ID, replayed)
					return
				case <-time.After(delay):
				}
			}
		}
		prevTimestamp = se.Event.Timestamp

		seq, newID, err := e.publishReplayedEvent(ctx, session, se)
		if err != nil {
			session.mu.Lock()
			session.State = SessionStateError
			session.ErrorMessage = err.Error()
			now := time.Now()
			session.CompletedAt = &now
			session.ReplayedEvents = replayed
			session.mu.Unlock()

			_ = e.publishCoordination(context.Background(), "simulation.replay.stop", session, &protocolv1.SimulationReplayStopPayload{
				SessionId:      session.ID,
				Reason:         "error",
				ReplayedEvents: int32(replayed),
			})
			e.broadcastError(session.ID, err.Error())
			return
		}
		replayed++

		session.mu.Lock()
		session.ReplayedEvents = replayed
		session.mu.Unlock()

		e.broadcastReplayEvent(session, se, newID, seq, replayed)
	}

	// All events replayed successfully.
	session.mu.Lock()
	session.State = SessionStateCompleted
	now := time.Now()
	session.CompletedAt = &now
	session.ReplayedEvents = replayed
	session.mu.Unlock()

	durationMs := time.Since(startedAt).Milliseconds()
	_ = e.publishCoordination(context.Background(), "simulation.replay.complete", session, &protocolv1.SimulationReplayCompletePayload{
		SessionId:   session.ID,
		TotalEvents: int32(replayed),
		DurationMs:  durationMs,
	})
	e.broadcastComplete(session.ID, replayed)
}

// publishReplayedEvent publishes a single replayed data event to the SIMULATION stream.
// It assigns a new UUID v7 ID and adds replay metadata. The NATS subject is
// simulation.replay.event (routes to SIMULATION stream), while the protobuf Type
// field retains the original event type.
// Returns the NATS stream sequence, the new event ID, and any error.
func (e *ReplayEngine) publishReplayedEvent(ctx context.Context, session *ReplaySession, se StoredEvent) (uint64, string, error) {
	newID := uuid.Must(uuid.NewV7()).String()

	meta := make(map[string]string, len(se.Event.Metadata)+4)
	for k, v := range se.Event.Metadata {
		meta[k] = v
	}
	meta["simulation.replay"] = "true"
	meta["simulation.session_id"] = session.ID
	meta["simulation.original_timestamp"] = fmt.Sprintf("%d", se.Event.Timestamp)
	meta["simulation.original_id"] = se.Event.ID

	pb := &protocolv1.AgentEvent{
		Id:            newID,
		Type:          se.Event.Type,
		SourceNode:    e.nodeID,
		SourceAgent:   se.Event.SourceAgent,
		TargetAgent:   se.Event.TargetAgent,
		WorkflowId:    se.Event.WorkflowID,
		CorrelationId: se.Event.CorrelationID,
		Timestamp:     time.Now().UnixNano(),
		Metadata:      meta,
		Payload:       se.Event.Payload,
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		return 0, "", fmt.Errorf("marshal replayed event: %w", err)
	}

	msg := nats.NewMsg(replaySubject)
	msg.Data = data
	msg.Header.Set("Nats-Msg-Id", newID)
	ack, err := e.js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, "", err
	}
	return ack.Sequence, newID, nil
}

// publishCoordination publishes a simulation.replay.* coordination event via EventBus.
func (e *ReplayEngine) publishCoordination(ctx context.Context, eventType string, session *ReplaySession, payload proto.Message) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal coordination payload: %w", err)
	}

	return e.bus.Publish(ctx, &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       eventType,
		SourceNode: e.nodeID,
		WorkflowID: session.WorkflowID,
		Timestamp:  time.Now().UnixNano(),
		Metadata: map[string]string{
			"simulation.session_id": session.ID,
		},
		Payload: data,
	})
}
