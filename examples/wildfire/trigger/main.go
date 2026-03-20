package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	wildfire "github.com/baran-network/baran-os/examples/wildfire/proto/gen"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	withApproval := flag.Bool("with-approval", false, "Add a human approval step before evacuation")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	timeout := 60 * time.Second
	if *withApproval {
		timeout = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	bus, err := natseventbus.New(ctx, *natsURL)
	if err != nil {
		logger.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer bus.Close()

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
		logger.Error("failed to marshal incident", "error", err)
		os.Exit(1)
	}

	// Define workflow steps.
	steps := []*protocolv1.StepDefinition{
		{Name: "risk-estimation", Capability: "risk-estimation", TimeoutSeconds: 60, Input: incidentData},
		{Name: "resource-allocation", Capability: "resource-allocation", TimeoutSeconds: 60, Input: incidentData},
	}
	if *withApproval {
		steps = append(steps, &protocolv1.StepDefinition{
			Name:          "approve-evacuation",
			HumanApproval: true,
			Prompt:        "Approve evacuation of Zone A affecting 5,000 residents?",
			ResourceIds:   []string{"zone-a"},
		})
		fmt.Println("Human approval step enabled — open http://localhost:8080/ui/ to approve")
	}
	steps = append(steps, &protocolv1.StepDefinition{
		Name: "evacuation-planning", Capability: "evacuation-planning", TimeoutSeconds: 60, Input: incidentData,
	})

	definition := &protocolv1.WorkflowDefinition{
		Name:      "wildfire-emergency-response",
		Initiator: "trigger",
		Steps:     steps,
	}

	startPayload := &protocolv1.WorkflowStartPayload{Definition: definition}
	data, err := proto.Marshal(startPayload)
	if err != nil {
		logger.Error("failed to marshal workflow start", "error", err)
		os.Exit(1)
	}

	// Subscribe to workflow completion/failure events before publishing start.
	completeCh := make(chan *protocolv1.WorkflowCompletePayload, 1)
	failedCh := make(chan *protocolv1.WorkflowFailedPayload, 1)

	// We subscribe to "workflow.>" to catch all workflow events since we don't know the ID yet.
	sub, err := bus.Subscribe(ctx, "workflow.>", func(ctx context.Context, event *eventbus.Event) error {
		switch {
		case containsSuffix(event.Type, "workflow.complete"):
			var payload protocolv1.WorkflowCompletePayload
			if err := proto.Unmarshal(event.Payload, &payload); err == nil {
				completeCh <- &payload
			}
		case containsSuffix(event.Type, "workflow.failed"):
			var payload protocolv1.WorkflowFailedPayload
			if err := proto.Unmarshal(event.Payload, &payload); err == nil {
				failedCh <- &payload
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("failed to subscribe", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	// Allow subscription to initialize.
	time.Sleep(200 * time.Millisecond)

	// Publish workflow.start.
	eventID := uuid.Must(uuid.NewV7()).String()
	if err := bus.Publish(ctx, &eventbus.Event{
		ID:        eventID,
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		logger.Error("failed to publish workflow.start", "error", err)
		os.Exit(1)
	}

	fmt.Println("Workflow started. Waiting for completion...")

	// Wait for result.
	select {
	case complete := <-completeCh:
		fmt.Printf("\nWorkflow completed successfully!\n")
		fmt.Printf("Workflow ID: %s\n", complete.WorkflowId)
		fmt.Printf("Steps completed: %d\n\n", len(complete.Results))
		for _, r := range complete.Results {
			printStepResult(r)
		}
	case failed := <-failedCh:
		fmt.Printf("\nWorkflow FAILED\n")
		fmt.Printf("Workflow ID: %s\n", failed.WorkflowId)
		if failed.Error != nil {
			fmt.Printf("Error: %s — %s (step %d)\n", failed.Error.Code, failed.Error.Message, failed.FailedStep)
		}
		os.Exit(1)
	case <-ctx.Done():
		fmt.Println("Timeout waiting for workflow completion")
		os.Exit(1)
	}
}

func printStepResult(r *protocolv1.StepResult) {
	fmt.Printf("--- Step %d (agent: %s) ---\n", r.StepIndex, r.AgentId)
	fmt.Printf("  Status: %s\n", r.Status.String())

	switch r.StepIndex {
	case 0:
		var risk wildfire.RiskAssessment
		if err := proto.Unmarshal(r.Result, &risk); err == nil {
			fmt.Printf("  Risk Score: %.2f\n", risk.RiskScore)
			fmt.Printf("  Threat Level: %s\n", risk.ThreatLevel.String())
			fmt.Printf("  Spread Rate: %.1f ha/hr\n", risk.SpreadRateHectaresPerHour)
			fmt.Printf("  Affected Zones: %v\n", risk.AffectedZones)
		}
	case 1:
		var plan wildfire.ResourcePlan
		if err := proto.Unmarshal(r.Result, &plan); err == nil {
			fmt.Printf("  Fire Trucks: %d\n", plan.AssignedFireTrucks)
			fmt.Printf("  Crews: %d\n", plan.AssignedCrews)
			fmt.Printf("  Aircraft: %d\n", plan.AssignedAircraft)
			fmt.Printf("  Staging Area: %s\n", plan.StagingArea)
			fmt.Printf("  Response Time: %d min\n", plan.EstimatedResponseTimeMinutes)
		}
	case 2:
		var evac wildfire.EvacuationPlan
		if err := proto.Unmarshal(r.Result, &evac); err == nil {
			fmt.Printf("  Evacuees: %d\n", evac.EstimatedEvacuees)
			fmt.Printf("  Shelters: %v\n", evac.ShelterLocations)
			for _, z := range evac.EvacuationZones {
				fmt.Printf("  Zone %q (priority %d): pop %d, route: %s\n",
					z.ZoneName, z.Priority, z.Population, z.PrimaryRoute)
			}
		}
	}
	fmt.Println()
}

func containsSuffix(eventType, suffix string) bool {
	return len(eventType) >= len(suffix) && eventType[len(eventType)-len(suffix):] == suffix
}
