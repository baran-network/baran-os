package sidecar

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "agent ID is required")
		return
	}

	_, ok := s.manager.Get(agentID)
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "agent not found")
		return
	}

	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}
	if req.DeliveryID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "delivery_id is required")
		return
	}

	// ACK is a no-op in the current implementation: JetStream acks are
	// managed by the SDK's durable consumer. Future work: advance consumer position.
	writeJSON(w, http.StatusOK, map[string]string{"status": "acknowledged"})
}
