package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/simulation"
)

// ReplayHandler serves REST API endpoints for event store queries and replay session management.
type ReplayHandler struct {
	store  simulation.EventStore
	engine *simulation.ReplayEngine
}

// NewReplayHandler creates a ReplayHandler.
func NewReplayHandler(store simulation.EventStore, engine *simulation.ReplayEngine) *ReplayHandler {
	return &ReplayHandler{store: store, engine: engine}
}

// RegisterRoutes registers event store and replay API endpoints on the given mux.
func (h *ReplayHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/events", h.handleQueryEvents)
	mux.HandleFunc("/api/events/workflows/", h.handleWorkflowEvents)
	mux.HandleFunc("/api/replay/sessions", h.handleReplaySessions)
	mux.HandleFunc("/api/replay/sessions/", h.handleReplaySessionRoutes)
}

func (h *ReplayHandler) handleQueryEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	q := r.URL.Query()

	startStr := q.Get("start")
	if startStr == "" {
		writeJSONError(w, http.StatusBadRequest, "start parameter is required")
		return
	}
	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid time format, expected RFC3339")
		return
	}

	query := simulation.EventQuery{
		StartTime:   startTime,
		WorkflowID:  q.Get("workflow_id"),
		SourceAgent: q.Get("source_agent"),
	}

	if endStr := q.Get("end"); endStr != "" {
		endTime, err := time.Parse(time.RFC3339, endStr)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid time format, expected RFC3339")
			return
		}
		query.EndTime = endTime
	}

	if typeStr := q.Get("type"); typeStr != "" {
		query.EventTypes = []string{typeStr}
	}

	if limitStr := q.Get("limit"); limitStr != "" {
		limit, err := strconv.Atoi(limitStr)
		if err != nil || limit <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid limit parameter")
			return
		}
		if limit > 10000 {
			limit = 10000
		}
		query.Limit = limit
	}

	if offsetStr := q.Get("offset"); offsetStr != "" {
		offset, err := strconv.Atoi(offsetStr)
		if err != nil || offset < 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid offset parameter")
			return
		}
		query.Offset = offset
	}

	events, err := h.store.Query(r.Context(), query)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to query event store")
		return
	}

	if events == nil {
		events = []simulation.StoredEvent{}
	}

	limit := query.Limit
	if limit == 0 {
		limit = 1000
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"events": storedEventsToJSON(events),
		"total":  len(events),
		"limit":  limit,
		"offset": query.Offset,
	})
}

func (h *ReplayHandler) handleWorkflowEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Extract workflow ID from path: /api/events/workflows/{workflow_id}
	workflowID := strings.TrimPrefix(r.URL.Path, "/api/events/workflows/")
	if workflowID == "" {
		writeJSONError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}

	events, err := h.store.GetWorkflowEvents(r.Context(), workflowID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to query workflow events")
		return
	}

	if len(events) == 0 {
		writeJSONError(w, http.StatusNotFound, "no events found for workflow "+workflowID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"workflow_id": workflowID,
		"events":      storedEventsToJSON(events),
		"total":       len(events),
	})
}

func (h *ReplayHandler) handleReplaySessions(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreateSession(w, r)
	case http.MethodGet:
		h.handleListSessions(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *ReplayHandler) handleReplaySessionRoutes(w http.ResponseWriter, r *http.Request) {
	// Path: /api/replay/sessions/{id} or /api/replay/sessions/{id}/stop
	path := strings.TrimPrefix(r.URL.Path, "/api/replay/sessions/")
	if path == "" {
		writeJSONError(w, http.StatusBadRequest, "session_id is required")
		return
	}

	if strings.HasSuffix(path, "/stop") {
		sessionID := strings.TrimSuffix(path, "/stop")
		h.handleStopSession(w, r, sessionID)
		return
	}

	if strings.HasSuffix(path, "/stream") {
		sessionID := strings.TrimSuffix(path, "/stream")
		h.handleStreamSession(w, r, sessionID)
		return
	}

	// GET /api/replay/sessions/{id}
	h.handleGetSession(w, r, path)
}

func (h *ReplayHandler) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkflowID string  `json:"workflow_id"`
		Speed      float64 `json:"speed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.WorkflowID == "" {
		writeJSONError(w, http.StatusBadRequest, "workflow_id is required")
		return
	}

	session, err := h.engine.CreateSession(r.Context(), req.WorkflowID, req.Speed)
	if err != nil {
		if strings.Contains(err.Error(), "no events found") {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to create replay session")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session": sessionToJSON(session),
	})
}

func (h *ReplayHandler) handleListSessions(w http.ResponseWriter, r *http.Request) {
	stateFilter := r.URL.Query().Get("state")
	sessions := h.engine.ListSessions(stateFilter)
	if sessions == nil {
		sessions = []*simulation.ReplaySession{}
	}

	result := make([]map[string]interface{}, len(sessions))
	for i, s := range sessions {
		result[i] = sessionToJSON(s)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"sessions": result,
	})
}

func (h *ReplayHandler) handleGetSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	session := h.engine.GetSession(sessionID)
	if session == nil {
		writeJSONError(w, http.StatusNotFound, "session not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sessionToJSON(session))
}

func (h *ReplayHandler) handleStopSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := h.engine.StopSession(sessionID); err != nil {
		if strings.Contains(err.Error(), "session not found") {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "session is not running") {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to stop session")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":     "stopped",
		"session_id": sessionID,
	})
}

func (h *ReplayHandler) handleStreamSession(w http.ResponseWriter, r *http.Request, sessionID string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	ch, cleanup, err := h.engine.WatchSession(sessionID)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "session not found")
		return
	}
	defer cleanup()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt, ok := <-ch:
			if !ok {
				// Channel closed — session terminated.
				return
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Name, evt.Data)
			flusher.Flush()
		}
	}
}

// sessionToJSON converts a ReplaySession to a JSON-friendly map.
func sessionToJSON(s *simulation.ReplaySession) map[string]interface{} {
	m := map[string]interface{}{
		"id":              s.ID,
		"workflow_id":     s.WorkflowID,
		"state":           string(s.State),
		"speed":           s.Speed,
		"total_events":    s.TotalEvents,
		"replayed_events": s.ReplayedEvents,
		"created_at":      s.CreatedAt.UTC().Format(time.RFC3339),
	}
	if s.ErrorMessage != "" {
		m["error_message"] = s.ErrorMessage
	}
	if s.StartedAt != nil {
		m["started_at"] = s.StartedAt.UTC().Format(time.RFC3339)
	}
	if s.CompletedAt != nil {
		m["completed_at"] = s.CompletedAt.UTC().Format(time.RFC3339)
	}
	return m
}

// storedEventsToJSON converts StoredEvent slice to a JSON-friendly representation.
func storedEventsToJSON(events []simulation.StoredEvent) []map[string]interface{} {
	result := make([]map[string]interface{}, len(events))
	for i, se := range events {
		evt := map[string]interface{}{
			"id":             se.Event.ID,
			"type":           se.Event.Type,
			"source_node":    se.Event.SourceNode,
			"source_agent":   se.Event.SourceAgent,
			"target_agent":   se.Event.TargetAgent,
			"workflow_id":    se.Event.WorkflowID,
			"correlation_id": se.Event.CorrelationID,
			"timestamp":      se.Event.Timestamp,
			"metadata":       se.Event.Metadata,
		}
		if se.Event.Payload != nil {
			evt["payload"] = se.Event.Payload
		}
		result[i] = map[string]interface{}{
			"event":    evt,
			"stream":   se.Stream,
			"sequence": se.Sequence,
		}
	}
	return result
}

// writeJSONError writes a JSON error response.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
