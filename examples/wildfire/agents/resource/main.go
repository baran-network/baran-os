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

	agent, err := sdk.New("resource-allocation", "wildfire-agent", "0.1.0",
		sdk.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create agent", "error", err)
		os.Exit(1)
	}

	agent.Handle(sdk.Capability{
		Name:        "resource-allocation",
		Version:     "0.1.0",
		Description: "Allocates emergency resources based on risk assessment",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var incident wildfire.WildfireIncident
		if err := proto.Unmarshal(step.Input, &incident); err != nil {
			return nil, err
		}

		if len(step.PreviousResults) == 0 {
			return nil, fmt.Errorf("no previous results: expected risk assessment")
		}

		var risk wildfire.RiskAssessment
		if err := proto.Unmarshal(step.PreviousResults[0].Result, &risk); err != nil {
			return nil, fmt.Errorf("unmarshal risk assessment: %w", err)
		}

		logger.Info("received inputs",
			"incident_id", incident.IncidentId,
			"threat_level", risk.ThreatLevel.String(),
			"risk_score", risk.RiskScore,
		)

		// Simulate processing (2s to make in-flight behavior visible during manual testing).
		time.Sleep(2 * time.Second)

		// Scale resources based on threat level.
		var trucks, crews, aircraft, ambulances int32
		var responseMinutes int32

		switch risk.ThreatLevel {
		case wildfire.ThreatLevel_THREAT_LEVEL_EXTREME:
			trucks, crews, aircraft, ambulances = 20, 15, 6, 10
			responseMinutes = 15
		case wildfire.ThreatLevel_THREAT_LEVEL_SEVERE:
			trucks, crews, aircraft, ambulances = 12, 8, 3, 6
			responseMinutes = 25
		case wildfire.ThreatLevel_THREAT_LEVEL_MODERATE:
			trucks, crews, aircraft, ambulances = 6, 4, 1, 3
			responseMinutes = 40
		default:
			trucks, crews, aircraft, ambulances = 3, 2, 0, 1
			responseMinutes = 60
		}

		plan := &wildfire.ResourcePlan{
			AssignedFireTrucks:           trucks,
			AssignedCrews:                crews,
			AssignedAircraft:             aircraft,
			AssignedAmbulances:           ambulances,
			StagingArea:                  "Base Camp Alpha - Highway 395",
			EstimatedResponseTimeMinutes: responseMinutes,
		}

		result, err := proto.Marshal(plan)
		if err != nil {
			return nil, err
		}

		logger.Info("resource plan completed",
			"fire_trucks", trucks,
			"crews", crews,
			"aircraft", aircraft,
		)
		return result, nil
	})

	logger.Info("starting resource-allocation agent")
	if err := agent.Run(context.Background()); err != nil {
		logger.Error("agent error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutting down")
}
