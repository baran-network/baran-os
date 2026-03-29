package a2a

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/baran-network/baran-os/core/registry"
)

// DiscoveryHandler serves A2A Agent Card discovery requests.
type DiscoveryHandler struct {
	registry registry.AgentRegistry
	cfg      *GatewayConfig
	logger   *slog.Logger
}

// NewDiscoveryHandler creates a DiscoveryHandler.
func NewDiscoveryHandler(reg registry.AgentRegistry, cfg *GatewayConfig, logger *slog.Logger) *DiscoveryHandler {
	return &DiscoveryHandler{registry: reg, cfg: cfg, logger: logger}
}

// HandleDiscovery serves GET /.well-known/agent-card.json.
func (h *DiscoveryHandler) HandleDiscovery(w http.ResponseWriter, r *http.Request) {
	agents, err := h.registry.List(r.Context())
	if err != nil {
		h.logger.Error("failed to list agents", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Filter to active agents only.
	active := make([]registry.AgentRegistration, 0, len(agents))
	for _, a := range agents {
		if a.Status == registry.StatusActive {
			active = append(active, a)
		}
	}

	externalURL := ""
	card := GenerateAgentCard(active, externalURL, h.cfg.A2APort)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(card)
}
