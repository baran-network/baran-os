package sidecar

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
)

type registerResponse struct {
	AgentID      string `json:"agent_id"`
	Status       string `json:"status"`
	RegisteredAt string `json:"registered_at"`
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "name is required")
		return
	}
	if req.AgentType == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "agent_type is required")
		return
	}
	if req.Version == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "version is required")
		return
	}

	// Auto-generate agent ID if not provided.
	if req.AgentID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to generate agent ID")
			return
		}
		req.AgentID = id.String()
	}

	ma, err := s.manager.Register(r.Context(), req)
	if err != nil {
		switch {
		case errors.Is(err, ErrAgentAlreadyExists):
			writeError(w, http.StatusConflict, "AGENT_CONFLICT", "an agent with this ID is already registered")
		case errors.Is(err, ErrMaxAgentsReached):
			writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "maximum concurrent agent limit reached")
		default:
			s.logger.Error("register agent failed", "error", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		}
		return
	}

	writeJSON(w, http.StatusCreated, registerResponse{
		AgentID:      ma.AgentID,
		Status:       "active",
		RegisteredAt: ma.RegisteredAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
}
