package a2a_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/baran-network/baran-os/a2a"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/taxonomy"
	"github.com/baran-network/baran-os/core/testutil"
	"github.com/baran-network/baran-os/core/workflow"
)

type testEnv struct {
	server *httptest.Server
	url    string
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()

	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	cat := taxonomy.NewStandardCatalog()
	reg, err := registry.NewKVRegistryWithCatalog(ctx, nc, 3, 6, cat)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		t.Fatalf("create workflow state store: %v", err)
	}

	// Register a test agent with code.generation capability.
	_, err = reg.Register(ctx, registry.AgentRegistration{
		AgentID:   "test-coder-01",
		AgentType: "coder",
		Version:   "1.0.0",
		Capabilities: []registry.Capability{
			{
				Name:        "code.generation",
				Version:     "1.0.0",
				Description: "Generate code from natural language",
			},
		},
		Status: registry.StatusActive,
		NodeID: "test-node",
		Origin: "local",
	})
	if err != nil {
		t.Fatalf("register test agent: %v", err)
	}

	cfg := &a2a.GatewayConfig{
		A2APort:  0,
		NATSUrl:  "unused",
		PSK:      "test-secret",
		LogLevel: "debug",
	}

	logger := slog.Default()
	srv := a2a.NewServerWithDeps(cfg, bus, reg, store, cat, logger)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return &testEnv{server: ts, url: ts.URL}
}

func (e *testEnv) doGet(t *testing.T, path string) *http.Response {
	t.Helper()
	resp, err := http.Get(e.url + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

func (e *testEnv) doJSONRPC(t *testing.T, method string, params any) a2a.JSONRPCResponse {
	t.Helper()
	req := a2a.JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		ID:      "test-1",
		Params:  params,
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	httpReq, _ := http.NewRequest("POST", e.url+"/", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer test-secret")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST /: %v", err)
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result a2a.JSONRPCResponse
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, string(data))
	}
	return result
}

func TestAgentCardDiscovery(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.doGet(t, "/.well-known/agent-card.json")
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var card a2a.AgentCard
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		t.Fatalf("decode agent card: %v", err)
	}

	if card.Name != "Baran OS Node" {
		t.Errorf("unexpected card name: %s", card.Name)
	}

	if len(card.Skills) == 0 {
		t.Fatal("expected at least one skill")
	}

	found := false
	for _, s := range card.Skills {
		if s.ID == "code.generation" {
			found = true
			if s.Name == "" {
				t.Error("skill name should not be empty")
			}
			if len(s.Tags) == 0 {
				t.Error("skill tags should not be empty")
			}
			break
		}
	}
	if !found {
		t.Error("expected skill code.generation in Agent Card")
	}

	if card.Capabilities.Streaming {
		t.Error("streaming should be false")
	}
}

func TestMessageSend(t *testing.T) {
	env := setupTestEnv(t)

	params := map[string]any{
		"message": map[string]any{
			"message_id": "msg-1",
			"role":       "user",
			"parts":      []map[string]any{{"text": "Write a hello world function"}},
		},
		"configuration": map[string]any{
			"skill": "code.generation",
		},
	}

	result := env.doJSONRPC(t, "message/send", params)

	if result.Error != nil {
		t.Fatalf("unexpected error: %+v", result.Error)
	}

	// Parse the result as a Task.
	taskData, _ := json.Marshal(result.Result)
	var task a2a.Task
	if err := json.Unmarshal(taskData, &task); err != nil {
		t.Fatalf("unmarshal task: %v", err)
	}

	if task.ID == "" {
		t.Error("task ID should not be empty")
	}
	if task.Status.State != a2a.TaskStateSubmitted {
		t.Errorf("expected TASK_STATE_SUBMITTED, got %s", task.Status.State)
	}
}

func TestMessageSendSkillNotFound(t *testing.T) {
	env := setupTestEnv(t)

	params := map[string]any{
		"message": map[string]any{
			"message_id": "msg-1",
			"role":       "user",
			"parts":      []map[string]any{{"text": "test"}},
		},
		"configuration": map[string]any{
			"skill": "nonexistent.skill",
		},
	}

	result := env.doJSONRPC(t, "message/send", params)

	if result.Error == nil {
		t.Fatal("expected error for nonexistent skill")
	}

	errCode := int(result.Error.Code)
	if errCode != a2a.ErrCodeSkillNotFound {
		t.Errorf("expected error code %d, got %d", a2a.ErrCodeSkillNotFound, errCode)
	}
}

func TestUnsupportedMethod(t *testing.T) {
	env := setupTestEnv(t)

	result := env.doJSONRPC(t, "message/stream", nil)

	if result.Error == nil {
		t.Fatal("expected error for unsupported method")
	}
	if result.Error.Code != a2a.ErrCodeMethodNotFound {
		t.Errorf("expected error code %d, got %d", a2a.ErrCodeMethodNotFound, result.Error.Code)
	}
}

func TestAuthRequired(t *testing.T) {
	env := setupTestEnv(t)

	// POST without auth token.
	body, _ := json.Marshal(a2a.JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tasks/get",
		ID:      "1",
		Params:  map[string]any{"id": "fake"},
	})
	resp, err := http.Post(env.url+"/", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /: %v", err)
	}
	defer resp.Body.Close()

	var result a2a.JSONRPCResponse
	json.NewDecoder(resp.Body).Decode(&result)

	if result.Error == nil {
		t.Fatal("expected auth error")
	}
}

// TestExternalAgentOnboarding verifies that the gateway onboards an external
// A2A agent, registers it in the registry as origin="a2a", dispatches an
// incoming message/send request to it, and marks it UNHEALTHY when the mock
// server stops responding.
func TestExternalAgentOnboarding(t *testing.T) {
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	bus, err := natseventbus.NewFromConn(ctx, nc)
	if err != nil {
		t.Fatalf("create eventbus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	cat := taxonomy.NewStandardCatalog()
	reg, err := registry.NewKVRegistryWithCatalog(ctx, nc, 3, 6, cat)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		t.Fatalf("create workflow state store: %v", err)
	}

	// --- Start mock external A2A agent server ---
	taskReturned := false
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/agent-card.json":
			json.NewEncoder(w).Encode(a2a.AgentCard{
				Name:    "Mock External Agent",
				Version: "1.0.0",
				Skills: []a2a.AgentSkill{
					{
						ID:          "nlp.summarization",
						Name:        "Summarization",
						Description: "Summarize text",
						InputModes:  []string{"text/plain"},
						OutputModes: []string{"text/plain"},
					},
				},
			})
		case "/":
			var req a2a.JSONRPCRequest
			json.NewDecoder(r.Body).Decode(&req)
			switch req.Method {
			case "message/send":
				taskReturned = true
				json.NewEncoder(w).Encode(a2a.JSONRPCResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result: a2a.Task{
						ID:     "mock-task-1",
						Status: a2a.NewTaskStatus(a2a.TaskStateCompleted),
					},
				})
			case "tasks/get":
				json.NewEncoder(w).Encode(a2a.JSONRPCResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Result: a2a.Task{
						ID:     "mock-task-1",
						Status: a2a.NewTaskStatus(a2a.TaskStateCompleted),
					},
				})
			default:
				http.Error(w, "not found", http.StatusNotFound)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(mockServer.Close)

	cfg := &a2a.GatewayConfig{
		A2APort:  0,
		NATSUrl:  "unused",
		PSK:      "",
		LogLevel: "debug",
	}
	logger := slog.Default()
	srv := a2a.NewServerWithDeps(cfg, bus, reg, store, cat, logger)

	// Onboard the external agent.
	extMgr := srv.ExternalAgentManager()
	extCfg := a2a.ExternalAgentConfig{
		Name:         "mock-ext",
		Endpoint:     mockServer.URL,
		PollInterval: 100 * time.Millisecond,
	}
	agentID, err := extMgr.OnboardExternalAgent(ctx, extCfg)
	if err != nil {
		t.Fatalf("OnboardExternalAgent: %v", err)
	}
	if agentID == "" {
		t.Fatal("expected non-empty agentID")
	}

	// Verify VirtualAgent is registered with origin="a2a".
	agents, err := reg.FindByCapability(ctx, "nlp.summarization", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(agents) == 0 {
		t.Fatal("expected virtual agent registered for nlp.summarization")
	}
	if agents[0].Origin != "a2a" {
		t.Errorf("expected origin=a2a, got %s", agents[0].Origin)
	}

	// Dispatch a message/send — should proxy to mock server.
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	params := map[string]any{
		"message": map[string]any{
			"message_id": "msg-ext-1",
			"role":       "user",
			"parts":      []map[string]any{{"text": "Summarize this."}},
		},
		"configuration": map[string]any{
			"skill": "nlp.summarization",
		},
	}
	reqBody, _ := json.Marshal(a2a.JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "message/send",
		ID:      "req-1",
		Params:  params,
	})

	httpReq, _ := http.NewRequest("POST", ts.URL+"/", bytes.NewReader(reqBody))
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("POST /: %v", err)
	}
	defer resp.Body.Close()

	var result a2a.JSONRPCResponse
	json.NewDecoder(resp.Body).Decode(&result)

	if result.Error != nil {
		t.Fatalf("unexpected error from message/send: %+v", result.Error)
	}
	if !taskReturned {
		t.Error("expected mock server to receive message/send")
	}

	taskData, _ := json.Marshal(result.Result)
	var task a2a.Task
	json.Unmarshal(taskData, &task)
	if task.ID == "" {
		t.Error("expected non-empty task ID in response")
	}

	// Stop mock server and verify health polling marks agent UNHEALTHY.
	mockServer.Close()
	extMgr.StartHealthPolling(ctx, agentID, mockServer.URL, 50*time.Millisecond)

	// Wait for at least 3 missed heartbeats to trigger UNHEALTHY transition.
	deadline := time.Now().Add(2 * time.Second)
	var finalStatus registry.AgentLifecycleStatus
	for time.Now().Before(deadline) {
		a, _, err := reg.Get(ctx, agentID)
		if err != nil {
			break
		}
		finalStatus = a.Status
		if finalStatus == registry.StatusUnhealthy || finalStatus == registry.StatusDead {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if finalStatus != registry.StatusUnhealthy && finalStatus != registry.StatusDead {
		t.Errorf("expected UNHEALTHY or DEAD after mock server stops, got %s", finalStatus)
	}
}
