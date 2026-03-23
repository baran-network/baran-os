package wildfire_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/discovery"
	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/health"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/router"
	"github.com/baran-network/baran-os/core/testutil"
	"github.com/baran-network/baran-os/core/workflow"
	wildfire "github.com/baran-network/baran-os/examples/wildfire/proto/gen"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/baran-network/baran-os/sdk"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const nodeID = "test-node"

// publishDirectStep dispatches a workflow step directly to an agent.
func publishDirectStep(t *testing.T, bus eventbus.EventBus, agentID, workflowID string, stepIndex uint32, capability string, input []byte, prevResults []*protocolv1.StepResult) {
	t.Helper()
	payload := &protocolv1.WorkflowStepPayload{
		StepIndex:       stepIndex,
		WorkflowId:      workflowID,
		PreviousResults: prevResults,
		Step: &protocolv1.StepDefinition{
			Name:       capability,
			Capability: capability,
			Input:      input,
		},
		Input: input,
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal step payload: %v", err)
	}

	evtID := uuid.Must(uuid.NewV7()).String()
	evt := &eventbus.Event{
		ID:          evtID,
		Type:        fmt.Sprintf("agent.direct.%s.workflow.step", agentID),
		SourceAgent: "test",
		TargetAgent: agentID,
		WorkflowID:  workflowID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish step: %v", err)
	}
}

// runtimeEnv holds all runtime subsystems for integration testing.
type runtimeEnv struct {
	bus    *natseventbus.Bus
	reg    registry.AgentRegistry
	engine *workflow.WorkflowEngine
}

// newRuntimeEnv starts embedded NATS and wires all runtime subsystems.
func newRuntimeEnv(t *testing.T) *runtimeEnv {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()

	// Stream registry shared across bus, router, and stream manager.
	streams := router.DefaultStreamRegistry()

	bus, err := natseventbus.NewFromConn(ctx, nc, streams)
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}

	// Registry handler.
	regH := registry.NewHandler(bus, reg, nodeID)
	regSubs, err := regH.Start(ctx)
	if err != nil {
		t.Fatalf("start registry handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range regSubs {
			_ = s.Unsubscribe()
		}
	})

	// Router + WorkflowStreamManager.
	streamMgr := workflow.NewWorkflowStreamManager(bus, streams)
	rtr := router.NewDefaultRouter(bus, reg, streams, streamMgr, nil)

	// Workflow engine.
	store, err := workflow.NewKVWorkflowStateStore(ctx, nc)
	if err != nil {
		t.Fatalf("create workflow state store: %v", err)
	}
	engine := workflow.NewWorkflowEngine(bus, store, reg, streamMgr, rtr, nodeID, 30*time.Second)
	wfSubs, err := engine.Start(ctx)
	if err != nil {
		t.Fatalf("start workflow engine: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range wfSubs {
			_ = s.Unsubscribe()
		}
	})

	// Health monitor.
	healthCfg := health.Config{
		HeartbeatInterval:  10 * time.Second,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
	}
	mon := health.New(bus, reg, nodeID, healthCfg)
	monSub, err := mon.Start(ctx)
	if err != nil {
		t.Fatalf("start health monitor: %v", err)
	}
	t.Cleanup(func() {
		_ = monSub.Unsubscribe()
		mon.Stop()
	})

	// Capability announcer.
	ann := discovery.NewCapabilityAnnouncer(bus, reg, nodeID)
	annSubs, err := ann.Start(ctx)
	if err != nil {
		t.Fatalf("start capability announcer: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range annSubs {
			_ = s.Unsubscribe()
		}
	})

	// Discovery handler.
	dh := discovery.NewDiscoveryHandler(bus, reg, nodeID)
	dhSubs, err := dh.Start(ctx)
	if err != nil {
		t.Fatalf("start discovery handler: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range dhSubs {
			_ = s.Unsubscribe()
		}
	})

	return &runtimeEnv{bus: bus, reg: reg, engine: engine}
}

// createRiskAgent creates the risk-estimation agent.
func createRiskAgent(bus eventbus.EventBus) (*sdk.Agent, error) {
	agent, err := sdk.New("risk-estimation", "wildfire-agent", "0.1.0",
		sdk.WithEventBus(bus),
	)
	if err != nil {
		return nil, err
	}

	agent.Handle(sdk.Capability{
		Name: "risk-estimation", Version: "0.1.0",
		Description: "Estimates wildfire risk",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var incident wildfire.WildfireIncident
		if err := proto.Unmarshal(step.Input, &incident); err != nil {
			return nil, err
		}

		time.Sleep(500 * time.Millisecond)

		var riskScore float64
		var threatLevel wildfire.ThreatLevel
		switch incident.Severity {
		case wildfire.Severity_SEVERITY_HIGH:
			riskScore = 0.8
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_SEVERE
		default:
			riskScore = 0.5
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_MODERATE
		}

		assessment := &wildfire.RiskAssessment{
			RiskScore:                 riskScore,
			SpreadRateHectaresPerHour: 30.0,
			ThreatLevel:               threatLevel,
			AffectedZones:             []string{"Zone-A", "Zone-B", "Zone-C"},
			EstimatedDurationHours:    48.0,
		}
		return proto.Marshal(assessment)
	})

	return agent, nil
}

// createResourceAgent creates the resource-allocation agent.
func createResourceAgent(bus eventbus.EventBus) (*sdk.Agent, error) {
	agent, err := sdk.New("resource-allocation", "wildfire-agent", "0.1.0",
		sdk.WithEventBus(bus),
	)
	if err != nil {
		return nil, err
	}

	agent.Handle(sdk.Capability{
		Name: "resource-allocation", Version: "0.1.0",
		Description: "Allocates emergency resources",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		if len(step.PreviousResults) == 0 {
			return nil, fmt.Errorf("no previous results")
		}
		var risk wildfire.RiskAssessment
		if err := proto.Unmarshal(step.PreviousResults[0].Result, &risk); err != nil {
			return nil, err
		}

		time.Sleep(500 * time.Millisecond)

		plan := &wildfire.ResourcePlan{
			AssignedFireTrucks:           12,
			AssignedCrews:                8,
			AssignedAircraft:             3,
			AssignedAmbulances:           6,
			StagingArea:                  "Base Camp Alpha - Highway 395",
			EstimatedResponseTimeMinutes: 25,
		}
		return proto.Marshal(plan)
	})

	return agent, nil
}

// createEvacuationAgent creates the evacuation-planning agent.
func createEvacuationAgent(bus eventbus.EventBus) (*sdk.Agent, error) {
	agent, err := sdk.New("evacuation-planning", "wildfire-agent", "0.1.0",
		sdk.WithEventBus(bus),
	)
	if err != nil {
		return nil, err
	}

	agent.Handle(sdk.Capability{
		Name: "evacuation-planning", Version: "0.1.0",
		Description: "Creates evacuation plan",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		if len(step.PreviousResults) < 2 {
			return nil, fmt.Errorf("expected 2 previous results, got %d", len(step.PreviousResults))
		}

		time.Sleep(500 * time.Millisecond)

		plan := &wildfire.EvacuationPlan{
			EvacuationZones: []*wildfire.EvacuationZone{
				{ZoneName: "Zone-A", Priority: 1, Population: 1200, PrimaryRoute: "Highway 395 South"},
				{ZoneName: "Zone-B", Priority: 2, Population: 450, PrimaryRoute: "Interstate 80 West"},
			},
			ShelterLocations:         []string{"Community Center - Reno", "High School Gym - Carson City"},
			EstimatedEvacuees:        1650,
			EstimatedCompletionHours: 24.0,
		}
		return proto.Marshal(plan)
	})

	return agent, nil
}

func TestWildfireWorkflowEndToEnd(t *testing.T) {
	env := newRuntimeEnv(t)
	ctx := context.Background()

	// Start all three agents in-process.
	riskAgent, err := createRiskAgent(env.bus)
	if err != nil {
		t.Fatalf("create risk agent: %v", err)
	}
	if err := riskAgent.Start(ctx); err != nil {
		t.Fatalf("start risk agent: %v", err)
	}
	defer func() { _ = riskAgent.Stop(ctx) }()

	resourceAgent, err := createResourceAgent(env.bus)
	if err != nil {
		t.Fatalf("create resource agent: %v", err)
	}
	if err := resourceAgent.Start(ctx); err != nil {
		t.Fatalf("start resource agent: %v", err)
	}
	defer func() { _ = resourceAgent.Stop(ctx) }()

	evacAgent, err := createEvacuationAgent(env.bus)
	if err != nil {
		t.Fatalf("create evacuation agent: %v", err)
	}
	if err := evacAgent.Start(ctx); err != nil {
		t.Fatalf("start evacuation agent: %v", err)
	}
	defer func() { _ = evacAgent.Stop(ctx) }()

	// Wait for all agents to register.
	time.Sleep(500 * time.Millisecond)

	// Build wildfire incident payload.
	incident := &wildfire.WildfireIncident{
		IncidentId:           uuid.Must(uuid.NewV7()).String(),
		Location:             "Sierra Nevada, CA",
		Latitude:             38.5,
		Longitude:            -120.0,
		Severity:             wildfire.Severity_SEVERITY_HIGH,
		AffectedAreaHectares: 150.0,
		WindSpeedKmh:         35.0,
		WindDirection:        "NE",
		ReportedAt:           time.Now().Unix(),
	}
	incidentData, err := proto.Marshal(incident)
	if err != nil {
		t.Fatalf("marshal incident: %v", err)
	}

	// Capture workflow ID from the first step dispatch on the DIRECT stream.
	wfIDCh := make(chan string, 1)
	var wfIDOnce sync.Once
	_, err = env.bus.Subscribe(ctx, "agent.direct.>", func(_ context.Context, evt *eventbus.Event) error {
		if evt.WorkflowID != "" {
			wfIDOnce.Do(func() { wfIDCh <- evt.WorkflowID })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent.direct: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Publish workflow.start.
	definition := &protocolv1.WorkflowDefinition{
		Name:      "wildfire-emergency-response",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "risk-estimation", Capability: "risk-estimation", TimeoutSeconds: 60, Input: incidentData},
			{Name: "resource-allocation", Capability: "resource-allocation", TimeoutSeconds: 60, Input: incidentData},
			{Name: "evacuation-planning", Capability: "evacuation-planning", TimeoutSeconds: 60, Input: incidentData},
		},
	}
	startPayload := &protocolv1.WorkflowStartPayload{Definition: definition}
	data, err := proto.Marshal(startPayload)
	if err != nil {
		t.Fatalf("marshal workflow start: %v", err)
	}

	eventID := uuid.Must(uuid.NewV7()).String()
	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:        eventID,
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish workflow.start: %v", err)
	}

	// Wait for the workflow ID from the first step dispatch.
	var wfID string
	select {
	case wfID = <-wfIDCh:
		t.Logf("Captured workflow ID: %s", wfID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first step dispatch")
	}

	// Now subscribe to per-workflow completion events.
	completeCh := make(chan *protocolv1.WorkflowCompletePayload, 1)
	failedCh := make(chan *protocolv1.WorkflowFailedPayload, 1)

	wfStream := fmt.Sprintf("WF-%s", wfID)
	completeSubject := fmt.Sprintf("workflow.%s.workflow.complete", wfID)
	_, err = env.bus.SubscribeWithStream(ctx, wfStream, completeSubject, func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.WorkflowCompletePayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			completeCh <- &payload
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.complete: %v", err)
	}

	failedSubject := fmt.Sprintf("workflow.%s.workflow.failed", wfID)
	_, err = env.bus.SubscribeWithStream(ctx, wfStream, failedSubject, func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.WorkflowFailedPayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			failedCh <- &payload
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.failed: %v", err)
	}

	// Wait for workflow completion (up to 30s).
	select {
	case complete := <-completeCh:
		t.Logf("Workflow %s completed successfully", complete.WorkflowId)

		if len(complete.Results) != 3 {
			t.Fatalf("expected 3 step results, got %d", len(complete.Results))
		}

		for i, r := range complete.Results {
			if r.Status != protocolv1.StepStatus_STEP_STATUS_SUCCESS {
				t.Errorf("step %d: expected SUCCESS, got %v", i, r.Status)
			}
			if len(r.Result) == 0 {
				t.Errorf("step %d: result payload is empty", i)
			}
		}

		// Verify step 0: risk assessment.
		var risk wildfire.RiskAssessment
		if err := proto.Unmarshal(complete.Results[0].Result, &risk); err != nil {
			t.Fatalf("unmarshal risk assessment: %v", err)
		}
		if risk.RiskScore == 0 {
			t.Error("risk score should be non-zero")
		}
		if risk.ThreatLevel == wildfire.ThreatLevel_THREAT_LEVEL_UNSPECIFIED {
			t.Error("threat level should be set")
		}

		// Verify step 1: resource plan.
		var resources wildfire.ResourcePlan
		if err := proto.Unmarshal(complete.Results[1].Result, &resources); err != nil {
			t.Fatalf("unmarshal resource plan: %v", err)
		}
		if resources.AssignedFireTrucks == 0 {
			t.Error("fire trucks should be assigned")
		}

		// Verify step 2: evacuation plan.
		var evac wildfire.EvacuationPlan
		if err := proto.Unmarshal(complete.Results[2].Result, &evac); err != nil {
			t.Fatalf("unmarshal evacuation plan: %v", err)
		}
		if len(evac.EvacuationZones) == 0 {
			t.Error("evacuation zones should not be empty")
		}
		if evac.EstimatedEvacuees == 0 {
			t.Error("estimated evacuees should be non-zero")
		}

	case failed := <-failedCh:
		errMsg := "unknown"
		if failed.Error != nil {
			errMsg = failed.Error.Message
		}
		t.Fatalf("workflow FAILED at step %d: %s", failed.FailedStep, errMsg)

	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for workflow completion")
	}
}

// TestWildfireWithHumanApproval verifies a 4-step workflow where step 2 is a
// human approval gate: risk-estimation → resource-allocation → human approve → evacuation-planning.
func TestWildfireWithHumanApproval(t *testing.T) {
	env := newRuntimeEnv(t)
	ctx := context.Background()

	// Start risk and resource agents.
	riskAgent, err := createRiskAgent(env.bus)
	if err != nil {
		t.Fatalf("create risk agent: %v", err)
	}
	if err := riskAgent.Start(ctx); err != nil {
		t.Fatalf("start risk agent: %v", err)
	}
	defer func() { _ = riskAgent.Stop(ctx) }()

	resourceAgent, err := createResourceAgent(env.bus)
	if err != nil {
		t.Fatalf("create resource agent: %v", err)
	}
	if err := resourceAgent.Start(ctx); err != nil {
		t.Fatalf("start resource agent: %v", err)
	}
	defer func() { _ = resourceAgent.Stop(ctx) }()

	// Create evacuation agent that expects 3 previous results (risk, resource, human approval).
	evacAgent, err := sdk.New("evacuation-hitl", "wildfire-agent", "0.1.0",
		sdk.WithEventBus(env.bus),
	)
	if err != nil {
		t.Fatalf("create evac agent: %v", err)
	}
	evacAgent.Handle(sdk.Capability{
		Name: "evacuation-planning", Version: "0.1.0",
		Description: "Creates evacuation plan",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		if len(step.PreviousResults) < 3 {
			return nil, fmt.Errorf("expected 3 previous results, got %d", len(step.PreviousResults))
		}
		plan := &wildfire.EvacuationPlan{
			EvacuationZones: []*wildfire.EvacuationZone{
				{ZoneName: "Zone-A", Priority: 1, Population: 1200, PrimaryRoute: "Highway 395 South"},
			},
			ShelterLocations:         []string{"Community Center - Reno"},
			EstimatedEvacuees:        1200,
			EstimatedCompletionHours: 12.0,
		}
		return proto.Marshal(plan)
	})
	if err := evacAgent.Start(ctx); err != nil {
		t.Fatalf("start evac agent: %v", err)
	}
	defer func() { _ = evacAgent.Stop(ctx) }()

	// Wait for all agents to register.
	time.Sleep(500 * time.Millisecond)

	// Track human.decision.request to get decision details.
	decisionCh := make(chan *protocolv1.HumanDecisionRequestPayload, 1)
	var decisionOnce sync.Once
	_, err = env.bus.Subscribe(ctx, "human.decision.request", func(_ context.Context, evt *eventbus.Event) error {
		req := &protocolv1.HumanDecisionRequestPayload{}
		if err := proto.Unmarshal(evt.Payload, req); err == nil {
			decisionOnce.Do(func() { decisionCh <- req })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe human.decision.request: %v", err)
	}

	// Capture workflow ID.
	wfIDCh := make(chan string, 1)
	var wfIDOnce sync.Once
	_, err = env.bus.Subscribe(ctx, "agent.direct.>", func(_ context.Context, evt *eventbus.Event) error {
		if evt.WorkflowID != "" {
			wfIDOnce.Do(func() { wfIDCh <- evt.WorkflowID })
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe agent.direct: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Build wildfire incident.
	incident := &wildfire.WildfireIncident{
		IncidentId:           uuid.Must(uuid.NewV7()).String(),
		Location:             "Sierra Nevada, CA",
		Severity:             wildfire.Severity_SEVERITY_HIGH,
		AffectedAreaHectares: 150.0,
		WindSpeedKmh:         35.0,
	}
	incidentData, _ := proto.Marshal(incident)

	// 4-step workflow: risk → resource → human approval → evacuation.
	definition := &protocolv1.WorkflowDefinition{
		Name:      "wildfire-emergency-with-approval",
		Initiator: "test",
		Steps: []*protocolv1.StepDefinition{
			{Name: "risk-estimation", Capability: "risk-estimation", TimeoutSeconds: 60, Input: incidentData},
			{Name: "resource-allocation", Capability: "resource-allocation", TimeoutSeconds: 60, Input: incidentData},
			{Name: "approve-evacuation", HumanApproval: true, Prompt: "Approve evacuation plan?", ResourceIds: []string{"zone-a"}},
			{Name: "evacuation-planning", Capability: "evacuation-planning", TimeoutSeconds: 60, Input: incidentData},
		},
	}
	startPayload := &protocolv1.WorkflowStartPayload{Definition: definition}
	data, _ := proto.Marshal(startPayload)

	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		t.Fatalf("publish workflow.start: %v", err)
	}

	// Wait for workflow ID from first step dispatch.
	var wfID string
	select {
	case wfID = <-wfIDCh:
		t.Logf("Captured workflow ID: %s", wfID)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first step dispatch")
	}

	// Wait for human.decision.request (step 2).
	var decisionReq *protocolv1.HumanDecisionRequestPayload
	select {
	case decisionReq = <-decisionCh:
		t.Logf("Received human decision request: %s", decisionReq.GetDecisionId())
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for human.decision.request")
	}

	// Verify the decision request has context.
	if decisionReq.GetPrompt() != "Approve evacuation plan?" {
		t.Errorf("unexpected prompt: %s", decisionReq.GetPrompt())
	}
	if len(decisionReq.GetPreviousResults()) < 2 {
		t.Errorf("expected at least 2 previous results, got %d", len(decisionReq.GetPreviousResults()))
	}
	if decisionReq.GetWorkflowId() != wfID {
		t.Errorf("workflow ID mismatch: %s != %s", decisionReq.GetWorkflowId(), wfID)
	}

	// Subscribe to per-workflow completion.
	completeCh := make(chan *protocolv1.WorkflowCompletePayload, 1)
	wfStream := fmt.Sprintf("WF-%s", wfID)
	completeSubject := fmt.Sprintf("workflow.%s.workflow.complete", wfID)
	_, err = env.bus.SubscribeWithStream(ctx, wfStream, completeSubject, func(_ context.Context, evt *eventbus.Event) error {
		var payload protocolv1.WorkflowCompletePayload
		if err := proto.Unmarshal(evt.Payload, &payload); err == nil {
			completeCh <- &payload
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe workflow.complete: %v", err)
	}

	// Simulate operator approval by publishing human.decision.response.
	approvalPayload := &protocolv1.HumanDecisionResponsePayload{
		DecisionId:  decisionReq.GetDecisionId(),
		WorkflowId:  wfID,
		Action:      protocolv1.DecisionAction_DECISION_ACTION_APPROVE,
		OperatorId:  "operator-jane",
		Comment:     "Evacuation approved. Wind conditions confirm urgency.",
		RespondedAt: time.Now().UnixNano(),
	}
	approvalData, _ := proto.Marshal(approvalPayload)

	if err := env.bus.Publish(ctx, &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       "human.decision.response",
		SourceNode: nodeID,
		WorkflowID: wfID,
		Timestamp:  time.Now().UnixNano(),
		Payload:    approvalData,
	}); err != nil {
		t.Fatalf("publish human.decision.response: %v", err)
	}

	// Wait for workflow completion.
	select {
	case complete := <-completeCh:
		t.Logf("Workflow %s completed with human approval", complete.WorkflowId)

		if len(complete.Results) != 4 {
			t.Fatalf("expected 4 step results, got %d", len(complete.Results))
		}

		for i, r := range complete.Results {
			if r.Status != protocolv1.StepStatus_STEP_STATUS_SUCCESS {
				t.Errorf("step %d: expected SUCCESS, got %v", i, r.Status)
			}
		}

		// Step 2 (human approval) should have agent_id "human-operator".
		if complete.Results[2].AgentId != "human-operator" {
			t.Errorf("step 2 agent_id: expected 'human-operator', got %q", complete.Results[2].AgentId)
		}

		// Step 3 (evacuation) should have a valid result.
		var evac wildfire.EvacuationPlan
		if err := proto.Unmarshal(complete.Results[3].Result, &evac); err != nil {
			t.Fatalf("unmarshal evacuation plan: %v", err)
		}
		if len(evac.EvacuationZones) == 0 {
			t.Error("evacuation zones should not be empty")
		}

	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for workflow completion")
	}
}

func TestAgentGracefulShutdown(t *testing.T) {
	env := newRuntimeEnv(t)
	ctx := context.Background()

	// Create the risk agent with a longer sleep handler (1s).
	agent, err := sdk.New("risk-shutdown-test", "wildfire-agent", "0.1.0",
		sdk.WithEventBus(env.bus),
	)
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}

	handlerCompleted := make(chan struct{}, 1)
	agent.Handle(sdk.Capability{
		Name: "risk-estimation", Version: "0.1.0",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var incident wildfire.WildfireIncident
		if err := proto.Unmarshal(step.Input, &incident); err != nil {
			return nil, err
		}
		// Simulate long processing — shutdown should wait for this.
		time.Sleep(1 * time.Second)
		handlerCompleted <- struct{}{}

		assessment := &wildfire.RiskAssessment{
			RiskScore:   0.8,
			ThreatLevel: wildfire.ThreatLevel_THREAT_LEVEL_SEVERE,
		}
		return proto.Marshal(assessment)
	})

	if err := agent.Start(ctx); err != nil {
		t.Fatalf("agent.Start: %v", err)
	}

	// Wait for registration.
	time.Sleep(300 * time.Millisecond)

	// Subscribe to step results on a per-workflow stream.
	wfID := uuid.Must(uuid.NewV7()).String()

	resultCh := make(chan *protocolv1.WorkflowStepResultPayload, 1)
	wfStream := fmt.Sprintf("WF-%s", wfID)
	resultSubject := fmt.Sprintf("workflow.%s.workflow.step.result", wfID)
	_, err = env.bus.SubscribeWithStream(ctx, wfStream, resultSubject, func(_ context.Context, evt *eventbus.Event) error {
		var p protocolv1.WorkflowStepResultPayload
		if err := proto.Unmarshal(evt.Payload, &p); err == nil {
			resultCh <- &p
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe step result: %v", err)
	}

	// Subscribe to unregister events.
	unregCh := make(chan struct{}, 1)
	_, err = env.bus.Subscribe(ctx, "agent.unregister", func(_ context.Context, evt *eventbus.Event) error {
		var p protocolv1.AgentUnregisterPayload
		if err := proto.Unmarshal(evt.Payload, &p); err == nil {
			if p.AgentId == agent.ID() {
				unregCh <- struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe unregister: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Dispatch a step to the agent.
	incident := &wildfire.WildfireIncident{
		IncidentId: "shutdown-test",
		Severity:   wildfire.Severity_SEVERITY_HIGH,
	}
	incidentData, _ := proto.Marshal(incident)

	publishDirectStep(t, env.bus, agent.ID(), wfID, 0, "risk-estimation", incidentData, nil)

	// Wait 500ms (mid-handler sleep) then stop the agent.
	time.Sleep(500 * time.Millisecond)

	// Stop should wait for the in-flight handler to complete.
	if err := agent.Stop(ctx); err != nil {
		t.Fatalf("agent.Stop: %v", err)
	}

	// Verify: (a) handler completed its work before shutdown.
	select {
	case <-handlerCompleted:
		// Handler finished — good.
	case <-time.After(3 * time.Second):
		t.Error("handler did not complete before shutdown")
	}

	// Verify: (b) step result was published.
	select {
	case r := <-resultCh:
		if r.Status != protocolv1.StepStatus_STEP_STATUS_SUCCESS {
			t.Errorf("expected SUCCESS, got %v", r.Status)
		}
	case <-time.After(3 * time.Second):
		t.Error("step result not published before shutdown")
	}

	// Verify: (c) unregister event was published.
	select {
	case <-unregCh:
		// Unregister event received — good.
	case <-time.After(3 * time.Second):
		t.Error("unregister event not published during shutdown")
	}
}
