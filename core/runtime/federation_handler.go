package runtime

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/baran-network/baran-os/core/federation"
)

// FederationHandler serves REST API endpoints for federation state inspection.
type FederationHandler struct {
	gateway *federation.FederationGateway
}

// NewFederationHandler creates a FederationHandler.
func NewFederationHandler(gw *federation.FederationGateway) *FederationHandler {
	return &FederationHandler{gateway: gw}
}

// RegisterRoutes registers federation API endpoints on the given mux.
func (h *FederationHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/federation/nodes", h.handleNodes)
}

// nodeJSON is the JSON representation of a federated node.
type nodeJSON struct {
	NodeID            string `json:"node_id"`
	Address           string `json:"address"`
	Status            string `json:"status"`
	CapabilitiesCount int32  `json:"capabilities_count"`
	LastSeen          string `json:"last_seen"`
	JoinedAt          string `json:"joined_at"`
	MissedHeartbeats  int32  `json:"missed_heartbeats"`
	Version           string `json:"version"`
}

func nodeToJSON(n federation.NodeInfo) nodeJSON {
	lastSeen := ""
	if n.LastSeen > 0 {
		lastSeen = time.Unix(0, n.LastSeen).UTC().Format(time.RFC3339)
	}
	joinedAt := ""
	if n.JoinedAt > 0 {
		joinedAt = time.Unix(0, n.JoinedAt).UTC().Format(time.RFC3339)
	}
	return nodeJSON{
		NodeID:            n.NodeID,
		Address:           n.Address,
		Status:            n.Status.String(),
		CapabilitiesCount: n.CapabilitiesCount,
		LastSeen:          lastSeen,
		JoinedAt:          joinedAt,
		MissedHeartbeats:  n.MissedHeartbeats,
		Version:           n.Version,
	}
}

// handleNodes handles GET /api/federation/nodes.
// Returns an empty array when federation is disabled.
func (h *FederationHandler) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	nodes := []nodeJSON{}

	if h.gateway != nil && h.gateway.IsEnabled() {
		list, err := h.gateway.NodeRegistry().List(r.Context())
		if err == nil {
			for _, n := range list {
				nodes = append(nodes, nodeToJSON(n))
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodes,
	})
}
