package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/runtime"
)

// startRuntimeForOperator starts a runtime and waits for it to be ready.
// Returns the base URL and a cancel function.
func startRuntimeForOperator(t *testing.T) (string, context.CancelFunc) {
	t.Helper()
	cfg := testConfig(t)
	rt := runtime.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := rt.Run(ctx); err != nil {
			t.Logf("runtime stopped: %v", err)
		}
	}()

	deadline := time.After(15 * time.Second)
	for !rt.Ready() {
		select {
		case <-deadline:
			cancel()
			t.Fatal("runtime did not become ready within 15s")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	return fmt.Sprintf("http://%s", rt.HealthAddr()), cancel
}

func TestOperatorAgentsEmpty(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/agents")
	if err != nil {
		t.Fatalf("GET /api/agents: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		Agents []interface{} `json:"agents"`
		Total  int           `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Total != 0 {
		t.Errorf("expected 0 agents, got %d", body.Total)
	}
}

func TestOperatorAgentNotFound(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/agents/nonexistent-id")
	if err != nil {
		t.Fatalf("GET /api/agents/{id}: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestOperatorWorkflowsEmpty(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/workflows")
	if err != nil {
		t.Fatalf("GET /api/workflows: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		Workflows []interface{} `json:"workflows"`
		Total     int           `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Total != 0 {
		t.Errorf("expected 0 workflows, got %d", body.Total)
	}
}

func TestOperatorWorkflowNotFound(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/workflows/nonexistent-id")
	if err != nil {
		t.Fatalf("GET /api/workflows/{id}: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestOperatorCapabilities(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/capabilities")
	if err != nil {
		t.Fatalf("GET /api/capabilities: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body struct {
		Capabilities []struct {
			Name     string `json:"name"`
			Category string `json:"category"`
		} `json:"capabilities"`
		Total int `json:"total"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Total == 0 {
		t.Error("expected at least one capability from the standard catalog")
	}
	for _, c := range body.Capabilities {
		if c.Name == "" || c.Category == "" {
			t.Errorf("capability missing name or category: %+v", c)
		}
	}
}

func TestOperatorStats(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	resp, err := http.Get(base + "/api/stats")
	if err != nil {
		t.Fatalf("GET /api/stats: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var stats struct {
		TotalAgents      int     `json:"total_agents"`
		HealthyCount     int     `json:"healthy_count"`
		EventThroughput  float64 `json:"event_throughput"`
		ActiveWorkflows  int     `json:"active_workflows"`
		PendingDecisions int     `json:"pending_decisions"`
		FederationNodes  int     `json:"federation_nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	// Fresh runtime: zero agents, zero workflows, zero decisions.
	if stats.TotalAgents != 0 {
		t.Errorf("expected 0 total agents, got %d", stats.TotalAgents)
	}
	if stats.ActiveWorkflows != 0 {
		t.Errorf("expected 0 active workflows, got %d", stats.ActiveWorkflows)
	}
}

func TestOperatorEventStream_Keepalive(t *testing.T) {
	base, cancel := startRuntimeForOperator(t)
	defer cancel()

	ctx, reqCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer reqCancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events/stream", nil)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil && ctx.Err() == nil {
		t.Fatalf("GET /api/events/stream: %v", err)
	}
	if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Errorf("expected text/event-stream, got %s", ct)
		}
	}
}
