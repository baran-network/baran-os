package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/router"
	"github.com/baran-network/baran-os/core/runtime"
	"github.com/baran-network/baran-os/core/testutil"
	"github.com/baran-network/baran-os/core/workflow"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"log/slog"
)

// uiTestSetup holds all components wired with embedded NATS for UI handler testing.
type uiTestSetup struct {
	bus       *natseventbus.Bus
	store     *workflow.KVWorkflowStateStore
	reg       registry.AgentRegistry
	engine    *workflow.WorkflowEngine
	uiHandler *runtime.UIHandler
	server    *httptest.Server
}

func newUITestSetup(t *testing.T) *uiTestSetup {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	streams := router.DefaultStreamRegistry()
	bus, err := natseventbus.NewFromConn(ctx, nc, streams)
	if err != nil {
		t.Fatalf("NewFromConn: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		t.Fatalf("NewKVWorkflowStateStore: %v", err)
	}

	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("NewKVRegistry: %v", err)
	}

	streamMgr := workflow.NewWorkflowStreamManager(bus, streams)
	rtr := router.NewDefaultRouter(bus, reg, streams, streamMgr)
	engine := workflow.NewWorkflowEngine(bus, store, reg, streamMgr, rtr, "test-node", 10*time.Second)

	logger := slog.Default()
	uiHandler := runtime.NewUIHandler(engine.Coordinator(), bus, "test-node", logger)

	// Wire HTTP routes.
	mux := http.NewServeMux()
	uiHandler.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Subscribe SSE event handlers.
	sseSubs, err := uiHandler.SubscribeEvents(ctx)
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range sseSubs {
			_ = s.Unsubscribe()
		}
	})

	return &uiTestSetup{
		bus:       bus,
		store:     store,
		reg:       reg,
		engine:    engine,
		uiHandler: uiHandler,
		server:    server,
	}
}

func (ts *uiTestSetup) startEngine(t *testing.T) {
	t.Helper()
	subs, err := ts.engine.Start(context.Background())
	if err != nil {
		t.Fatalf("engine.Start: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	})
	time.Sleep(200 * time.Millisecond)
}

func (ts *uiTestSetup) registerAgent(t *testing.T, agentID, capability string) {
	t.Helper()
	_, err := ts.reg.Register(context.Background(), registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    "test-agent",
		Version:      "1.0.0",
		Capabilities: []registry.Capability{{Name: capability, Version: "1.0.0"}},
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

// TestUIHandler_ListAndRespond tests the full UI flow:
// 1. Start a workflow that reaches WAITING_HUMAN
// 2. GET /api/decisions → decision listed
// 3. GET /api/decisions/{id} → decision details
// 4. POST /api/decisions/{id}/respond with approve → workflow resumes
func TestUIHandler_ListAndRespond(t *testing.T) {
	ts := newUITestSetup(t)
	ctx := context.Background()

	const agentID = "agent-evac-ui"
	ts.registerAgent(t, agentID, "evacuation-planning")
	ts.startEngine(t)

	// Track human.decision.request to get the decision ID.
	var decisionReqWg sync.WaitGroup
	decisionReqWg.Add(1)
	var capturedReq *protocolv1.HumanDecisionRequestPayload
	var once sync.Once

	_, err := ts.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		req := &protocolv1.HumanDecisionRequestPayload{}
		if err := proto.Unmarshal(evt.Payload, req); err == nil {
			once.Do(func() {
				capturedReq = req
				decisionReqWg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe human.decision.request: %v", err)
	}

	// Also track step 0 dispatch to complete it.
	var stepWg sync.WaitGroup
	stepWg.Add(1)
	var stepOnce sync.Once

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil {
			stepOnce.Do(func() { stepWg.Done() })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent direct: %v", err)
	}

	// Start a 3-step workflow: agent → human → agent.
	def := &protocolv1.WorkflowDefinition{
		Name:      "ui-test-workflow",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "assess-risk", Capability: "evacuation-planning", TimeoutSeconds: 5},
			{Name: "approve-evac", HumanApproval: true, Prompt: "Approve evacuation?", ResourceIds: []string{"zone-a"}},
			{Name: "execute-evac", Capability: "evacuation-planning", TimeoutSeconds: 5},
		},
	}
	startPayload := &protocolv1.WorkflowStartPayload{Definition: def}
	data, _ := proto.Marshal(startPayload)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish workflow.start: %v", err)
	}

	// Wait for step 0 to be dispatched.
	waitDone(t, &stepWg, 5*time.Second, "step 0 dispatch")

	// Complete step 0 by publishing a result.
	time.Sleep(100 * time.Millisecond) // let state settle
	// We need the workflow ID — look it up from the store.
	states, err := ts.store.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(states) == 0 {
		t.Fatal("no workflows found")
	}
	workflowID := states[0].WorkflowID

	stepResult := &protocolv1.WorkflowStepResultPayload{
		StepIndex: 0,
		Status:    protocolv1.StepStatus_STEP_STATUS_SUCCESS,
		Result:    []byte("risk-assessment-done"),
	}
	resultData, _ := proto.Marshal(stepResult)
	if err := ts.bus.Publish(ctx, &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       fmt.Sprintf("workflow.%s.workflow.step.result", workflowID),
		SourceAgent: agentID,
		WorkflowID: workflowID,
		Timestamp:  time.Now().UnixNano(),
		Payload:    resultData,
	}); err != nil {
		t.Fatalf("publish step result: %v", err)
	}

	// Wait for human.decision.request.
	waitDone(t, &decisionReqWg, 5*time.Second, "human.decision.request")
	decisionID := capturedReq.GetDecisionId()

	// Give coordinator time to register the pending decision.
	time.Sleep(200 * time.Millisecond)

	// --- Test GET /api/decisions ---
	resp, err := http.Get(ts.server.URL + "/api/decisions")
	if err != nil {
		t.Fatalf("GET /api/decisions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var listResp struct {
		Decisions []json.RawMessage `json:"decisions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(listResp.Decisions))
	}

	// --- Test GET /api/decisions/{id} ---
	resp2, err := http.Get(ts.server.URL + "/api/decisions/" + decisionID)
	if err != nil {
		t.Fatalf("GET /api/decisions/{id}: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}

	var detail map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail["decision_id"] != decisionID {
		t.Fatalf("expected decision_id %s, got %v", decisionID, detail["decision_id"])
	}
	if detail["prompt"] != "Approve evacuation?" {
		t.Fatalf("unexpected prompt: %v", detail["prompt"])
	}

	// --- Test GET /api/decisions/{id} for non-existent ---
	resp3, err := http.Get(ts.server.URL + "/api/decisions/non-existent-id")
	if err != nil {
		t.Fatalf("GET non-existent: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp3.StatusCode)
	}

	// --- Test POST /api/decisions/{id}/respond with invalid action ---
	invalidBody := `{"action":"maybe","operator_id":"op1"}`
	resp4, err := http.Post(ts.server.URL+"/api/decisions/"+decisionID+"/respond",
		"application/json", strings.NewReader(invalidBody))
	if err != nil {
		t.Fatalf("POST invalid: %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != 400 {
		t.Fatalf("expected 400 for invalid action, got %d", resp4.StatusCode)
	}

	// --- Test POST /api/decisions/{id}/respond with approve ---
	// Handle step 2 dispatch (complete it automatically).
	var step2Wg sync.WaitGroup
	step2Wg.Add(1)
	var step2Once sync.Once

	_, err = ts.bus.Subscribe(ctx, "agent.direct."+agentID+".>", func(_ context.Context, evt *eventbus.Event) error {
		var step protocolv1.WorkflowStepPayload
		if err := proto.Unmarshal(evt.Payload, &step); err == nil && step.StepIndex == 2 {
			step2Once.Do(func() {
				// Complete step 2.
				res := &protocolv1.WorkflowStepResultPayload{
					StepIndex: 2,
					Status:    protocolv1.StepStatus_STEP_STATUS_SUCCESS,
					Result:    []byte("evacuation-done"),
				}
				d, _ := proto.Marshal(res)
				_ = ts.bus.Publish(context.Background(), &eventbus.Event{
					ID:          uuid.Must(uuid.NewV7()).String(),
					Type:        fmt.Sprintf("workflow.%s.workflow.step.result", workflowID),
					SourceAgent: agentID,
					WorkflowID:  workflowID,
					Timestamp:   time.Now().UnixNano(),
					Payload:     d,
				})
				step2Wg.Done()
			})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe step 2: %v", err)
	}

	approveBody := `{"action":"approve","operator_id":"operator-jane","comment":"Approved via UI test"}`
	resp5, err := http.Post(ts.server.URL+"/api/decisions/"+decisionID+"/respond",
		"application/json", strings.NewReader(approveBody))
	if err != nil {
		t.Fatalf("POST approve: %v", err)
	}
	defer resp5.Body.Close()
	if resp5.StatusCode != 200 {
		body, _ := io.ReadAll(resp5.Body)
		t.Fatalf("expected 200, got %d: %s", resp5.StatusCode, body)
	}

	var approveResp map[string]interface{}
	if err := json.NewDecoder(resp5.Body).Decode(&approveResp); err != nil {
		t.Fatalf("decode approve response: %v", err)
	}
	if approveResp["status"] != "accepted" {
		t.Fatalf("expected status 'accepted', got %v", approveResp["status"])
	}

	// Wait for step 2 to be dispatched and completed.
	waitDone(t, &step2Wg, 5*time.Second, "step 2 dispatch")

	// Poll for workflow completion via state store.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, _, err := ts.store.Get(ctx, workflowID)
		if err == nil && st.Status == 3 { // StatusCompleted = 3
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	finalState, _, err := ts.store.Get(ctx, workflowID)
	if err != nil {
		t.Fatalf("get final state: %v", err)
	}
	if finalState.Status != 3 { // StatusCompleted
		t.Fatalf("expected workflow COMPLETED (3), got %d", finalState.Status)
	}

	// Verify decision is no longer pending.
	resp6, err := http.Get(ts.server.URL + "/api/decisions")
	if err != nil {
		t.Fatalf("GET decisions after approve: %v", err)
	}
	defer resp6.Body.Close()

	var finalList struct {
		Decisions []json.RawMessage `json:"decisions"`
	}
	if err := json.NewDecoder(resp6.Body).Decode(&finalList); err != nil {
		t.Fatalf("decode final list: %v", err)
	}
	if len(finalList.Decisions) != 0 {
		t.Fatalf("expected 0 pending decisions after approve, got %d", len(finalList.Decisions))
	}

	// Verify POST to already-resolved decision returns 409.
	resp7, err := http.Post(ts.server.URL+"/api/decisions/"+decisionID+"/respond",
		"application/json", strings.NewReader(approveBody))
	if err != nil {
		t.Fatalf("POST already resolved: %v", err)
	}
	defer resp7.Body.Close()
	if resp7.StatusCode != 409 {
		t.Fatalf("expected 409 for already resolved, got %d", resp7.StatusCode)
	}
}

// TestUIHandler_StaticAssets verifies that /ui/ serves the embedded HTML.
func TestUIHandler_StaticAssets(t *testing.T) {
	ts := newUITestSetup(t)

	resp, err := http.Get(ts.server.URL + "/ui/index.html")
	if err != nil {
		t.Fatalf("GET /ui/index.html: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Baran OS") {
		t.Fatal("index.html does not contain 'Baran OS'")
	}

	resp2, err := http.Get(ts.server.URL + "/ui/app.js")
	if err != nil {
		t.Fatalf("GET /ui/app.js: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		t.Fatalf("expected 200 for app.js, got %d", resp2.StatusCode)
	}
}

func waitDone(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for %s", msg)
	}
}
