package sidecar

import (
	"net/http"
)

type healthResponse struct {
	Status      string `json:"status"`
	NATSUrl     string `json:"nats_url"`
	AgentCount  int    `json:"agent_count"`
	MaxAgents   int    `json:"max_agents"`
	Port        int    `json:"port"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Status:     "ok",
		NATSUrl:    s.cfg.NATSUrl,
		AgentCount: s.manager.Count(),
		MaxAgents:  s.cfg.MaxAgents,
		Port:       s.cfg.Port,
	})
}
