package sidecar

import (
	"errors"
	"net/http"
)

type deregisterResponse struct {
	AgentID string `json:"agent_id"`
	Status  string `json:"status"`
}

func (s *Server) handleDeregister(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "agent ID is required")
		return
	}

	if err := s.manager.Deregister(r.Context(), agentID); err != nil {
		if errors.Is(err, ErrAgentNotFound) {
			writeError(w, http.StatusNotFound, "AGENT_NOT_FOUND", "agent not found")
			return
		}
		s.logger.Error("deregister agent failed", "error", err, "agent_id", agentID)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, deregisterResponse{
		AgentID: agentID,
		Status:  "deregistered",
	})
}
