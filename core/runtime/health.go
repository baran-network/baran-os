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

	w.Header().Set("Content-Type", "application/json")
	if status != "healthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}
