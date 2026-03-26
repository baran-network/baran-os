package sidecar

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
)

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "agent ID is required")
		return
	}

	ma, ok := s.manager.Get(agentID)
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "agent not found")
		return
	}

	var req PublishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	if req.EventType == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "event_type is required")
		return
	}

	if !s.registry.Has(req.EventType) {
		writeError(w, http.StatusBadRequest, "UNKNOWN_EVENT_TYPE",
			"event type not registered in payload schema: "+req.EventType)
		return
	}

	// Re-encode payload map back to JSON for translation.
	payloadJSON, err := json.Marshal(req.Payload)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PAYLOAD", "cannot encode payload")
		return
	}

	protoBytes, err := JSONToProto(s.registry, req.EventType, payloadJSON)
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, "PAYLOAD_SCHEMA_MISMATCH",
			"payload does not match schema for event type: "+err.Error())
		return
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate event ID")
		return
	}

	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        req.EventType,
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     protoBytes,
		Metadata:    req.Metadata,
	}

	// Support target_agent and route.capability from metadata.
	if v, ok := req.Metadata["target_agent"]; ok {
		event.TargetAgent = v
		delete(event.Metadata, "target_agent")
	}

	ma.mu.Lock()
	ma.LastActivity = now()
	ma.mu.Unlock()

	// Publish via the shared EventBus (same connection used by the SDK Agent).
	if err := s.manager.bus.Publish(r.Context(), event); err != nil {
		s.logger.Error("publish event failed", "error", err, "agent_id", agentID)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"event_id": eventID.String(),
		"status":   "accepted",
	})
}
