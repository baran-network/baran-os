package runtime

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/simulation"
)

// ScenarioHandler serves REST API endpoints for synthetic event injection
// and scenario lifecycle management.
type ScenarioHandler struct {
	injector *simulation.EventInjector
	engine   *simulation.ScenarioEngine
}

// NewScenarioHandler creates a ScenarioHandler.
func NewScenarioHandler(injector *simulation.EventInjector, engine *simulation.ScenarioEngine) *ScenarioHandler {
	return &ScenarioHandler{injector: injector, engine: engine}
}

// RegisterRoutes registers simulation API endpoints on the given mux.
func (h *ScenarioHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/simulation/inject", h.handleInject)
	mux.HandleFunc("/api/simulation/scenarios", h.handleScenarios)
	mux.HandleFunc("/api/simulation/scenarios/", h.handleScenarioRoutes)
	mux.HandleFunc("/api/simulation/sessions", h.handleSessions)
	mux.HandleFunc("/api/simulation/sessions/", h.handleSessionRoutes)
}

// handleInject handles POST /api/simulation/inject.
func (h *ScenarioHandler) handleInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req simulation.InjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	result, err := h.injector.Inject(r.Context(), req, "", "")
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"event_id": result.EventID,
		"stream":   result.Stream,
		"sequence": result.Sequence,
	})
}

// handleScenarios handles POST and GET /api/simulation/scenarios.
func (h *ScenarioHandler) handleScenarios(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleRegisterScenario(w, r)
	case http.MethodGet:
		h.handleListScenarios(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleScenarioRoutes dispatches routes under /api/simulation/scenarios/{id}[/action].
func (h *ScenarioHandler) handleScenarioRoutes(w http.ResponseWriter, r *http.Request) {
	// Path: /api/simulation/scenarios/{id} or /api/simulation/scenarios/{id}/start
	path := strings.TrimPrefix(r.URL.Path, "/api/simulation/scenarios/")
	if path == "" {
		writeJSONError(w, http.StatusBadRequest, "scenario_id is required")
		return
	}

	if strings.HasSuffix(path, "/start") {
		scenarioID := strings.TrimSuffix(path, "/start")
		h.handleStartScenario(w, r, scenarioID)
		return
	}

	h.handleGetScenario(w, r, path)
}

// handleSessions handles GET /api/simulation/sessions.
func (h *ScenarioHandler) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	stateFilter := r.URL.Query().Get("state")
	sessions := h.engine.ListSessions(stateFilter)
	if sessions == nil {
		sessions = []*simulation.ScenarioSession{}
	}

	result := make([]map[string]interface{}, len(sessions))
	for i, s := range sessions {
		result[i] = scenarioSessionToJSON(s)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"sessions": result,
	})
}

// handleSessionRoutes dispatches routes under /api/simulation/sessions/{id}[/action].
func (h *ScenarioHandler) handleSessionRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/simulation/sessions/")
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

	h.handleGetSession(w, r, path)
}

func (h *ScenarioHandler) handleRegisterScenario(w http.ResponseWriter, r *http.Request) {
	var def simulation.ScenarioDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	registered, err := h.engine.RegisterScenario(&def)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeJSONError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"scenario": scenarioSummaryToJSON(registered),
	})
}

func (h *ScenarioHandler) handleListScenarios(w http.ResponseWriter, r *http.Request) {
	scenarios := h.engine.ListScenarios()
	if scenarios == nil {
		scenarios = []*simulation.ScenarioDefinition{}
	}

	result := make([]map[string]interface{}, len(scenarios))
	for i, s := range scenarios {
		result[i] = scenarioSummaryToJSON(s)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"scenarios": result,
	})
}

func (h *ScenarioHandler) handleGetScenario(w http.ResponseWriter, r *http.Request, scenarioID string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	def := h.engine.GetScenario(scenarioID)
	if def == nil {
		writeJSONError(w, http.StatusNotFound, "scenario not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"scenario": scenarioDetailToJSON(def),
	})
}

func (h *ScenarioHandler) handleStartScenario(w http.ResponseWriter, r *http.Request, scenarioID string) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	session, err := h.engine.StartScenario(r.Context(), scenarioID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to start scenario")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session": scenarioSessionToJSON(session),
	})
}

func (h *ScenarioHandler) handleGetSession(w http.ResponseWriter, r *http.Request, sessionID string) {
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
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session": scenarioSessionToJSON(session),
	})
}

func (h *ScenarioHandler) handleStopSession(w http.ResponseWriter, r *http.Request, sessionID string) {
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

	session := h.engine.GetSession(sessionID)
	if session == nil {
		writeJSONError(w, http.StatusNotFound, "session not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"session": scenarioSessionToJSON(session),
	})
}

func (h *ScenarioHandler) handleStreamSession(w http.ResponseWriter, r *http.Request, sessionID string) {
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
				return
			}
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Name, evt.Data)
			flusher.Flush()
		}
	}
}

// scenarioSummaryToJSON converts a ScenarioDefinition to a summary JSON map (no steps detail).
func scenarioSummaryToJSON(d *simulation.ScenarioDefinition) map[string]interface{} {
	return map[string]interface{}{
		"id":          d.ID,
		"name":        d.Name,
		"description": d.Description,
		"steps":       len(d.Steps),
		"created_at":  d.CreatedAt.UTC().Format(time.RFC3339),
	}
}

// scenarioDetailToJSON converts a ScenarioDefinition to a full JSON map including steps.
func scenarioDetailToJSON(d *simulation.ScenarioDefinition) map[string]interface{} {
	steps := make([]map[string]interface{}, len(d.Steps))
	for i, s := range d.Steps {
		step := map[string]interface{}{
			"event_type":     s.EventType,
			"delay_ms":       s.DelayMs,
			"source_agent":   s.SourceAgent,
			"target_agent":   s.TargetAgent,
			"workflow_id":    s.WorkflowID,
			"correlation_id": s.CorrelationID,
			"metadata":       s.Metadata,
			"condition":      s.Condition,
		}
		if s.PayloadJSON != nil {
			step["payload_json"] = s.PayloadJSON
		}
		steps[i] = step
	}
	return map[string]interface{}{
		"id":          d.ID,
		"name":        d.Name,
		"description": d.Description,
		"steps":       steps,
		"created_at":  d.CreatedAt.UTC().Format(time.RFC3339),
	}
}

// scenarioSessionToJSON converts a ScenarioSession to a JSON-friendly map.
func scenarioSessionToJSON(s *simulation.ScenarioSession) map[string]interface{} {
	m := map[string]interface{}{
		"id":              s.ID,
		"scenario_id":     s.ScenarioID,
		"scenario_name":   s.ScenarioName,
		"state":           string(s.State),
		"current_step":    s.CurrentStep,
		"total_steps":     s.TotalSteps,
		"injected_events": s.InjectedEvents,
		"duration_ms":     s.DurationMs,
		"error_message":   s.ErrorMessage,
		"created_at":      s.CreatedAt.UTC().Format(time.RFC3339),
	}
	if s.StartedAt != nil {
		m["started_at"] = s.StartedAt.UTC().Format(time.RFC3339)
	}
	if s.CompletedAt != nil {
		m["completed_at"] = s.CompletedAt.UTC().Format(time.RFC3339)
	}
	return m
}
