package runtime

import (
	"encoding/json"
	"net/http"
	"time"
)

// HealthResponse is the JSON response from the /healthz endpoint.
type HealthResponse struct {
	Status     string            `json:"status"`
	NodeID     string            `json:"node_id"`
	Uptime     string            `json:"uptime"`
	Subsystems map[string]string `json:"subsystems"`
	Federation *FederationHealth `json:"federation,omitempty"`
}

// FederationHealth holds federation-specific health metrics.
type FederationHealth struct {
	Enabled      bool `json:"enabled"`
	NodeCount    int  `json:"node_count"`
	HealthyNodes int  `json:"healthy_nodes"`
}

func (r *Runtime) healthHandler(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	subsystems := make(map[string]string, len(r.subsystemStatus))
	for k, v := range r.subsystemStatus {
		subsystems[k] = v
	}
	r.mu.RUnlock()

	status := "healthy"
	if r.startedAt.IsZero() {
		status = "starting"
	} else {
		for _, v := range subsystems {
			if v != "up" {
				status = "degraded"
				break
			}
		}
	}

	resp := HealthResponse{
		Status:     status,
		NodeID:     r.nodeID,
		Uptime:     time.Since(r.startedAt).Round(time.Second).String(),
		Subsystems: subsystems,
	}

	if r.federation != nil {
		fh := &FederationHealth{Enabled: r.federation.IsEnabled()}
		if fh.Enabled {
			if nodes, err := r.federation.NodeRegistry().List(req.Context()); err == nil {
				fh.NodeCount = len(nodes)
				for _, n := range nodes {
					if n.Status.String() == "active" {
						fh.HealthyNodes++
					}
				}
			}
		}
		resp.Federation = fh
	}

	w.Header().Set("Content-Type", "application/json")
	if status != "healthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(resp)
}
