package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	wildfire "github.com/baran-network/baran-os/examples/wildfire/proto/gen"
	"github.com/baran-network/baran-os/sdk"
	"google.golang.org/protobuf/proto"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	agent, err := sdk.New("risk-estimation", "wildfire-agent", "0.1.0",
		sdk.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create agent", "error", err)
		os.Exit(1)
	}

	agent.Handle(sdk.Capability{
		Name:        "risk-estimation",
		Version:     "0.1.0",
		Description: "Estimates wildfire risk based on incident data",
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var incident wildfire.WildfireIncident
		if err := proto.Unmarshal(step.Input, &incident); err != nil {
			return nil, err
		}
		logger.Info("received incident",
			"incident_id", incident.IncidentId,
			"location", incident.Location,
			"severity", incident.Severity.String(),
		)

		// Simulate processing (2s to make in-flight behavior visible during manual testing).
		time.Sleep(2 * time.Second)

		// Compute risk based on severity.
		var riskScore float64
		var threatLevel wildfire.ThreatLevel
		var spreadRate float64
		var duration float64

		switch incident.Severity {
		case wildfire.Severity_SEVERITY_CRITICAL:
			riskScore = 0.95
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_EXTREME
			spreadRate = 50.0
			duration = 72.0
		case wildfire.Severity_SEVERITY_HIGH:
			riskScore = 0.8
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_SEVERE
			spreadRate = 30.0
			duration = 48.0
		case wildfire.Severity_SEVERITY_MEDIUM:
			riskScore = 0.5
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_MODERATE
			spreadRate = 15.0
			duration = 24.0
		default:
			riskScore = 0.2
			threatLevel = wildfire.ThreatLevel_THREAT_LEVEL_MINIMAL
			spreadRate = 5.0
			duration = 12.0
		}

		assessment := &wildfire.RiskAssessment{
			RiskScore:                 riskScore,
			SpreadRateHectaresPerHour: spreadRate,
			ThreatLevel:               threatLevel,
			AffectedZones:             []string{"Zone-A", "Zone-B", "Zone-C"},
			EstimatedDurationHours:    duration,
		}

		result, err := proto.Marshal(assessment)
		if err != nil {
			return nil, err
		}

		logger.Info("risk assessment completed",
			"risk_score", riskScore,
			"threat_level", threatLevel.String(),
		)
		return result, nil
	})

	logger.Info("starting risk-estimation agent")
	if err := agent.Run(context.Background()); err != nil {
		logger.Error("agent error", "error", err)
		os.Exit(1)
	}
	logger.Info("shutting down")
}
