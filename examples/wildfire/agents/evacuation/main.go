package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	wildfire "github.com/baran-network/baran-os/examples/wildfire/proto/gen"
	"github.com/baran-network/baran-os/sdk"
	"google.golang.org/protobuf/proto"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	agent, err := sdk.New("evacuation-planning", "wildfire-agent", "0.1.0",
		sdk.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create agent", "error", err)
		os.Exit(1)
	}

	agent.Handle(sdk.Capability{
		Name:        "evacuation-planning",
		Version:     "0.1.0",
		Description: "Creates evacuation plan based on risk and available resources",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var incident wildfire.WildfireIncident
		if err := proto.Unmarshal(step.Input, &incident); err != nil {
			return nil, err
		}

		if len(step.PreviousResults) < 2 {
			return nil, fmt.Errorf("expected 2 previous results, got %d", len(step.PreviousResults))
		}

		var risk wildfire.RiskAssessment
		if err := proto.Unmarshal(step.PreviousResults[0].Result, &risk); err != nil {
			return nil, fmt.Errorf("unmarshal risk assessment: %w", err)
		}

		var resources wildfire.ResourcePlan
		if err := proto.Unmarshal(step.PreviousResults[1].Result, &resources); err != nil {
			return nil, fmt.Errorf("unmarshal resource plan: %w", err)
		}

		logger.Info("received inputs",
			"incident_id", incident.IncidentId,
			"threat_level", risk.ThreatLevel.String(),
			"fire_trucks", resources.AssignedFireTrucks,
			"staging_area", resources.StagingArea,
		)

		// Simulate processing (2s to make in-flight behavior visible during manual testing).
		time.Sleep(2 * time.Second)

		plan := &wildfire.EvacuationPlan{
			EvacuationZones: []*wildfire.EvacuationZone{
				{
					ZoneName:       "Zone-A (Residential)",
					Priority:       1,
					Population:     1200,
					PrimaryRoute:   "Highway 395 South",
					AlternateRoute: "Mountain Road 12 East",
				},
				{
					ZoneName:       "Zone-B (Commercial)",
					Priority:       2,
					Population:     450,
					PrimaryRoute:   "Interstate 80 West",
					AlternateRoute: "County Road 7 North",
				},
				{
					ZoneName:       "Zone-C (Rural)",
					Priority:       3,
					Population:     180,
					PrimaryRoute:   "Forest Service Road 3",
					AlternateRoute: "Emergency Helicopter LZ",
				},
			},
			ShelterLocations:         []string{"Community Center - Reno", "High School Gym - Carson City", "Fairgrounds - Minden"},
			EstimatedEvacuees:        1830,
			EstimatedCompletionHours: risk.EstimatedDurationHours / 2,
		}

		result, err := proto.Marshal(plan)
		if err != nil {
			return nil, err
		}

		logger.Info("evacuation plan completed",
			"zones", len(plan.EvacuationZones),
			"evacuees", plan.EstimatedEvacuees,
			"completion_hours", plan.EstimatedCompletionHours,
		)
		return result, nil
	})

	logger.Info("starting evacuation-planning agent")
	if err := agent.Run(context.Background()); err != nil {
		logger.Error("agent error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutting down")
}
