package sidecar_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/testutil"
	"github.com/baran-network/baran-os/sidecar"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// testEnv holds a running sidecar server backed by a real embedded NATS.
type testEnv struct {
	server *httptest.Server
	url    string
	bus    eventbus.EventBus
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

	cfg := &sidecar.SidecarConfig{
		Port:      0, // unused, httptest handles port
		NATSUrl:   "unused",
		PSK:       "test-secret",
		LogLevel:  "debug",
		MaxAgents: 3,
	}

	logger := slog.Default()
	srv := sidecar.NewServerWithBus(cfg, bus, logger)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return &testEnv{server: ts, url: ts.URL, bus: bus}
}

func (e *testEnv) doRequest(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.url+path, bodyReader)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-secret")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// doRegisterAgent registers an agent and returns its agent_id.
func doRegisterAgent(t *testing.T, env *testEnv, id, name string) string {
	t.Helper()
	body := map[string]any{
		"agent_id":   id,
		"name":       name,
		"agent_type": "test",
		"version":    "1.0.0",
	}
	resp := env.doRequest(t, "POST", "/agents", body)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("register agent %s: got %d: %s", id, resp.StatusCode, b)
	}
	var result map[string]string
	decodeJSON(t, resp, &result)
	return result["agent_id"]
}

// --- Phase 3: Registration / Deregistration ---

func TestRegisterAgent(t *testing.T) {
	env := setupTestEnv(t)

	body := map[string]any{
		"name":       "test-agent",
		"agent_type": "analyzer",
		"version":    "1.0.0",
		"capabilities": []map[string]any{
			{
				"name":        "test.cap",
				"version":     "1.0.0",
				"description": "test capability",
			},
		},
	}

	resp := env.doRequest(t, "POST", "/agents", body)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, b)
	}

	var result map[string]string
	decodeJSON(t, resp, &result)

	if result["agent_id"] == "" {
		t.Fatal("expected agent_id in response")
	}
	if result["status"] != "active" {
		t.Fatalf("expected status=active, got %s", result["status"])
	}
	if result["registered_at"] == "" {
		t.Fatal("expected registered_at in response")
	}
}

func TestRegisterAgentWithExplicitID(t *testing.T) {
	env := setupTestEnv(t)

	body := map[string]any{
		"agent_id":   "explicit-agent-id-001",
		"name":       "explicit-agent",
		"agent_type": "worker",
		"version":    "2.0.0",
	}

	resp := env.doRequest(t, "POST", "/agents", body)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, b)
	}

	var result map[string]string
	decodeJSON(t, resp, &result)

	if result["agent_id"] != "explicit-agent-id-001" {
		t.Fatalf("expected explicit agent_id, got %s", result["agent_id"])
	}
}

func TestRegisterDuplicateAgent(t *testing.T) {
	env := setupTestEnv(t)

	body := map[string]any{
		"agent_id":   "dup-agent-001",
		"name":       "dup-agent",
		"agent_type": "worker",
		"version":    "1.0.0",
	}

	resp := env.doRequest(t, "POST", "/agents", body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("first register: expected 201, got %d", resp.StatusCode)
	}

	resp2 := env.doRequest(t, "POST", "/agents", body)
	if resp2.StatusCode != http.StatusConflict {
		b, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		t.Fatalf("duplicate register: expected 409, got %d: %s", resp2.StatusCode, b)
	}
	resp2.Body.Close()
}

func TestRegisterMaxAgents(t *testing.T) {
	env := setupTestEnv(t) // MaxAgents = 3

	for i := 0; i < 3; i++ {
		body := map[string]any{
			"agent_id":   fmt.Sprintf("max-agent-%d", i),
			"name":       fmt.Sprintf("agent-%d", i),
			"agent_type": "worker",
			"version":    "1.0.0",
		}
		resp := env.doRequest(t, "POST", "/agents", body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("register agent %d: expected 201, got %d", i, resp.StatusCode)
		}
	}

	// 4th should fail
	body := map[string]any{
		"agent_id":   "max-agent-overflow",
		"name":       "overflow",
		"agent_type": "worker",
		"version":    "1.0.0",
	}
	resp := env.doRequest(t, "POST", "/agents", body)
	if resp.StatusCode != http.StatusServiceUnavailable {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("expected 503, got %d: %s", resp.StatusCode, b)
	}
	resp.Body.Close()
}

func TestRegisterMissingFields(t *testing.T) {
	env := setupTestEnv(t)

	tests := []struct {
		name string
		body map[string]any
	}{
		{"missing name", map[string]any{"agent_type": "worker", "version": "1.0"}},
		{"missing type", map[string]any{"name": "test", "version": "1.0"}},
		{"missing version", map[string]any{"name": "test", "agent_type": "worker"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := env.doRequest(t, "POST", "/agents", tt.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", resp.StatusCode)
			}
			resp.Body.Close()
		})
	}
}

func TestDeregisterAgent(t *testing.T) {
	env := setupTestEnv(t)

	// Register first
	body := map[string]any{
		"agent_id":   "dereg-agent-001",
		"name":       "to-deregister",
		"agent_type": "worker",
		"version":    "1.0.0",
	}
	resp := env.doRequest(t, "POST", "/agents", body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register: expected 201, got %d", resp.StatusCode)
	}

	// Deregister
	resp2 := env.doRequest(t, "DELETE", "/agents/dereg-agent-001", nil)
	if resp2.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		t.Fatalf("deregister: expected 200, got %d: %s", resp2.StatusCode, b)
	}

	var result map[string]string
	decodeJSON(t, resp2, &result)

	if result["status"] != "deregistered" {
		t.Fatalf("expected status=deregistered, got %s", result["status"])
	}

	// Verify agent is gone — re-registering should work
	resp3 := env.doRequest(t, "POST", "/agents", body)
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusCreated {
		t.Fatalf("re-register: expected 201, got %d", resp3.StatusCode)
	}
}

func TestDeregisterNotFound(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.doRequest(t, "DELETE", "/agents/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestHealthEndpoint(t *testing.T) {
	env := setupTestEnv(t)

	resp := env.doRequest(t, "GET", "/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]any
	decodeJSON(t, resp, &result)

	if result["status"] != "ok" {
		t.Fatalf("expected status=ok, got %v", result["status"])
	}
}

func TestAuthRequired(t *testing.T) {
	env := setupTestEnv(t)

	req, _ := http.NewRequest("GET", env.url+"/health", nil)
	// No Authorization header
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without auth, got %d", resp.StatusCode)
	}
}

// --- Phase 4: Event Publishing ---

// TestPublishEvent verifies that an external agent can publish events via HTTP
// and the event is routed through the EventBus to native subscribers.
func TestPublishEvent(t *testing.T) {
	env := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	agentID := doRegisterAgent(t, env, "pub-agent-001", "publisher")

	// Subscribe on the bus to verify the event arrives.
	received := make(chan *eventbus.Event, 1)
	sub, err := env.bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, e *eventbus.Event) error {
		if e.SourceAgent == agentID {
			received <- e
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish via HTTP.
	body := map[string]any{
		"event_type": "agent.health.ping",
		"payload": map[string]any{
			"agentId": agentID,
		},
	}
	resp := env.doRequest(t, "POST", "/agents/"+agentID+"/events", body)
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, b)
	}
	var result map[string]string
	decodeJSON(t, resp, &result)
	if result["status"] != "accepted" {
		t.Fatalf("expected status=accepted, got %s", result["status"])
	}
	if result["event_id"] == "" {
		t.Fatal("expected event_id in response")
	}

	// Verify the event arrived on the bus.
	select {
	case e := <-received:
		if e.SourceAgent != agentID {
			t.Fatalf("wrong source agent: got %s", e.SourceAgent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: published event not received on bus")
	}
}

// TestPublishUnknownEventType verifies 400 is returned for unknown event types.
func TestPublishUnknownEventType(t *testing.T) {
	env := setupTestEnv(t)

	agentID := doRegisterAgent(t, env, "pub-agent-002", "publisher-bad")

	body := map[string]any{
		"event_type": "totally.unknown.type",
		"payload":    map[string]any{},
	}
	resp := env.doRequest(t, "POST", "/agents/"+agentID+"/events", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestPublishAgentNotFound verifies 404 for unregistered agents.
func TestPublishAgentNotFound(t *testing.T) {
	env := setupTestEnv(t)

	body := map[string]any{
		"event_type": "agent.health.ping",
		"payload":    map[string]any{},
	}
	resp := env.doRequest(t, "POST", "/agents/nonexistent-agent/events", body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// --- Phase 5: SSE Event Subscription ---

// sseReadResult holds the outcome of reading one SSE event.
type sseReadResult struct {
	data string
	err  error
}

// readFirstSSEEvent reads lines from reader until a complete SSE event is received.
// It skips comments (lines starting with ":") and notifies via connectedCh when
// the "connected" comment is seen.
func readFirstSSEEvent(reader *bufio.Reader, connectedCh chan<- struct{}) <-chan sseReadResult {
	ch := make(chan sseReadResult, 1)
	go func() {
		var id, eventType, data string
		connectedSent := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				ch <- sseReadResult{err: err}
				return
			}
			line = strings.TrimRight(line, "\r\n")

			if strings.HasPrefix(line, ": connected") && !connectedSent {
				connectedSent = true
				select {
				case connectedCh <- struct{}{}:
				default:
				}
				continue
			}
			// Skip all other comment lines.
			if strings.HasPrefix(line, ":") {
				continue
			}

			switch {
			case strings.HasPrefix(line, "id: "):
				id = line[4:]
			case strings.HasPrefix(line, "event: "):
				eventType = line[7:]
			case strings.HasPrefix(line, "data: "):
				data = line[6:]
			case line == "":
				// Empty line = event boundary.
				if data != "" {
					_ = id
					_ = eventType
					ch <- sseReadResult{data: data}
					return
				}
				// Reset accumulated fields.
				id, eventType, data = "", "", ""
			}
		}
	}()
	return ch
}

// TestSubscribeSSE verifies that an external agent receives events in real-time
// over an SSE stream, and that the ACK endpoint accepts delivery confirmations.
func TestSubscribeSSE(t *testing.T) {
	env := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	agentID := doRegisterAgent(t, env, "sse-sub-agent", "sse-subscriber")

	// Open SSE stream.
	req, _ := http.NewRequestWithContext(ctx, "GET", env.url+"/agents/"+agentID+"/events", nil)
	req.Header.Set("Authorization", "Bearer test-secret")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("SSE connect: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("SSE status: got %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("expected text/event-stream content-type, got %s", ct)
	}

	connectedCh := make(chan struct{}, 1)
	reader := bufio.NewReader(resp.Body)
	resultCh := readFirstSSEEvent(reader, connectedCh)

	// Wait for SSE handler to signal it is ready.
	select {
	case <-connectedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SSE connected comment")
	}

	// Publish an event directly to the DIRECT stream targeting this agent.
	// The sidecar SSE handler subscribes to "agent.direct.<agentID>.>" so the
	// event type must use that prefix. normalizeEventType strips it before registry lookup.
	directType := fmt.Sprintf("agent.direct.%s.agent.health.ping", agentID)
	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:          "sse-test-event-001",
		Type:        directType,
		SourceAgent: "test-sender",
		Timestamp:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish direct event: %v", err)
	}

	// Wait for SSE to deliver the event.
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("SSE read error: %v", result.err)
		}
		// Data must be valid JSON (translated from protobuf).
		var payload map[string]any
		if err := json.Unmarshal([]byte(result.data), &payload); err != nil {
			t.Fatalf("SSE data is not valid JSON %q: %v", result.data, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SSE event")
	}

	// Test ACK endpoint — should return 200.
	ackBody := map[string]any{"delivery_id": "sse-test-event-001"}
	ackResp := env.doRequest(t, "POST", "/agents/"+agentID+"/ack", ackBody)
	if ackResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(ackResp.Body)
		ackResp.Body.Close()
		t.Fatalf("ACK: expected 200, got %d: %s", ackResp.StatusCode, b)
	}
	ackResp.Body.Close()
}

// --- Phase 6: WebSocket Event Streaming ---

// TestWebSocketStreaming verifies full-duplex WebSocket communication:
// the sidecar delivers events as JSON frames and routes client publish messages.
func TestWebSocketStreaming(t *testing.T) {
	env := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	agentID := doRegisterAgent(t, env, "ws-stream-agent", "ws-streamer")

	// Dial WebSocket. Auth via query param (Bearer header not supported by WS upgrade).
	wsURL := strings.Replace(env.url, "http://", "ws://", 1) +
		"/agents/" + agentID + "/ws?token=test-secret"

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("WS dial: %v", err)
	}
	defer conn.CloseNow()

	// Read the "connected" message from the server.
	var connMsg map[string]any
	if err := wsjson.Read(ctx, conn, &connMsg); err != nil {
		t.Fatalf("read WS connected msg: %v", err)
	}
	if connMsg["type"] != "connected" {
		t.Fatalf("expected type=connected, got %v", connMsg["type"])
	}

	// Publish an event targeting this agent directly on the bus.
	directType := fmt.Sprintf("agent.direct.%s.agent.health.ping", agentID)
	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:          "ws-test-event-001",
		Type:        directType,
		SourceAgent: "test-sender",
		Timestamp:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish direct event: %v", err)
	}

	// Read the event delivered over WebSocket.
	var eventMsg map[string]any
	if err := wsjson.Read(ctx, conn, &eventMsg); err != nil {
		t.Fatalf("read WS event msg: %v", err)
	}
	if eventMsg["type"] != "event" {
		t.Fatalf("expected type=event, got %v", eventMsg["type"])
	}
	// After normalizeEventType strips "agent.direct.<id>." prefix, event_type = "agent.health.ping".
	if eventMsg["event_type"] != "agent.health.ping" {
		t.Fatalf("expected event_type=agent.health.ping, got %v", eventMsg["event_type"])
	}

	// Test client → server publish: subscribe on bus and verify arrival.
	received := make(chan *eventbus.Event, 1)
	sub, err := env.bus.Subscribe(ctx, "agent.health.ping", func(_ context.Context, e *eventbus.Event) error {
		if e.SourceAgent == agentID {
			received <- e
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	pubMsg := map[string]any{
		"type":       "publish",
		"event_type": "agent.health.ping",
		"payload":    map[string]any{"agentId": agentID},
	}
	if err := wsjson.Write(ctx, conn, pubMsg); err != nil {
		t.Fatalf("WS publish: %v", err)
	}

	select {
	case e := <-received:
		if e.SourceAgent != agentID {
			t.Fatalf("wrong source agent in published event: %s", e.SourceAgent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for WS-published event on bus")
	}

	conn.Close(websocket.StatusNormalClosure, "test complete")
}

// --- Phase 9: End-to-End Cross-Agent Communication ---

// TestE2EExternalAndNativeAgentRoundTrip simulates a Python SDK agent and a native
// Go agent communicating through the sidecar. The external agent registers via HTTP,
// subscribes via SSE, receives an event from the native agent, and publishes a response
// that the native agent receives — verifying the full bidirectional loop.
func TestE2EExternalAndNativeAgentRoundTrip(t *testing.T) {
	env := setupTestEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Register the "external" agent (simulates Python SDK calling POST /agents).
	externalID := doRegisterAgent(t, env, "e2e-external-001", "python-echo")

	// 2. Register a "native" agent (also via sidecar, simulating a second participant).
	nativeID := doRegisterAgent(t, env, "e2e-native-001", "go-orchestrator")

	// 3. External agent opens SSE stream (simulates Python SDK's SSE client).
	sseReq, _ := http.NewRequestWithContext(ctx, "GET", env.url+"/agents/"+externalID+"/events", nil)
	sseReq.Header.Set("Authorization", "Bearer test-secret")
	sseReq.Header.Set("Accept", "text/event-stream")

	sseResp, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("SSE connect: %v", err)
	}
	defer sseResp.Body.Close()

	if sseResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(sseResp.Body)
		t.Fatalf("SSE: expected 200, got %d: %s", sseResp.StatusCode, b)
	}

	connectedCh := make(chan struct{}, 1)
	reader := bufio.NewReader(sseResp.Body)
	sseEventCh := readFirstSSEEvent(reader, connectedCh)

	select {
	case <-connectedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SSE connected")
	}

	// 4. Native agent subscribes on the bus for responses from the external agent.
	nativeReceived := make(chan *eventbus.Event, 1)
	sub, err := env.bus.Subscribe(ctx, "agent.health.pong", func(_ context.Context, e *eventbus.Event) error {
		if e.SourceAgent == externalID {
			nativeReceived <- e
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// 5. Native agent publishes an event targeting the external agent via direct routing.
	directType := fmt.Sprintf("agent.direct.%s.agent.health.ping", externalID)
	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:          "e2e-step-001",
		Type:        directType,
		SourceAgent: nativeID,
		Timestamp:   time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("publish direct event: %v", err)
	}

	// 6. External agent receives the event via SSE.
	select {
	case result := <-sseEventCh:
		if result.err != nil {
			t.Fatalf("SSE read error: %v", result.err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(result.data), &payload); err != nil {
			t.Fatalf("SSE data not valid JSON: %v", err)
		}
		t.Logf("external agent received event via SSE: %s", result.data)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: external agent did not receive event via SSE")
	}

	// 7. External agent ACKs the event (simulates Python SDK's ack call).
	ackResp := env.doRequest(t, "POST", "/agents/"+externalID+"/ack", map[string]any{
		"delivery_id": "e2e-step-001",
	})
	if ackResp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(ackResp.Body)
		ackResp.Body.Close()
		t.Fatalf("ACK: expected 200, got %d: %s", ackResp.StatusCode, b)
	}
	ackResp.Body.Close()

	// 8. External agent publishes a response (simulates Python SDK publishing result).
	pubResp := env.doRequest(t, "POST", "/agents/"+externalID+"/events", map[string]any{
		"event_type": "agent.health.pong",
		"payload": map[string]any{
			"agentId": externalID,
			"status":  "HEALTHY",
		},
	})
	if pubResp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(pubResp.Body)
		pubResp.Body.Close()
		t.Fatalf("publish response: expected 202, got %d: %s", pubResp.StatusCode, b)
	}
	pubResp.Body.Close()

	// 9. Native agent receives the response on the bus.
	select {
	case e := <-nativeReceived:
		if e.SourceAgent != externalID {
			t.Fatalf("wrong source: expected %s, got %s", externalID, e.SourceAgent)
		}
		t.Logf("native agent received response from external agent: %s", e.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: native agent did not receive response from external agent")
	}
}
