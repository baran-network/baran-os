// hello-baran uppercaser agent — native Go agent demonstrating capability
// dispatch via the Baran SDK.
//
// Capability: text.uppercase
//
// Note on the Phase 9 capability taxonomy (spec 015):
// `text` is NOT one of the 8 standard top-level categories
// (nlp, code, vision, data, decision, communication, orchestration, security
// — see core/taxonomy/catalog_data.go), and `text.uppercase` is not a standard
// catalog entry. Per core/taxonomy/catalog.go Validate() rule 3, a name with
// ≥2 dot-separated segments whose first segment is not a standard category is
// accepted as a valid vendor capability. Therefore NO alias registration is
// required at startup — the SDK can register `text.uppercase` directly.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"

	"github.com/baran-network/baran-os/sdk"
)

type textPayload struct {
	Text string `json:"text"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	natsURL := os.Getenv("BARAN_NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	agent, err := sdk.New("uppercaser", "hello-baran-agent", "0.1.0",
		sdk.WithLogger(logger),
		sdk.WithNATSURL(natsURL),
	)
	if err != nil {
		logger.Error("failed to create agent", "error", err)
		os.Exit(1)
	}

	agent.Handle(sdk.Capability{
		Name:        "text.uppercase",
		Version:     "0.1.0",
		Description: "Returns the input text upper-cased",
		InputTypes:  []string{"application/json"},
		OutputTypes: []string{"application/json"},
	}, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
		var in textPayload
		if err := json.Unmarshal(step.Input, &in); err != nil {
			return nil, err
		}
		out := textPayload{Text: strings.ToUpper(in.Text)}
		logger.Info("uppercase", "in", in.Text, "out", out.Text)
		return json.Marshal(out)
	})

	logger.Info("starting uppercaser agent", "nats_url", natsURL)
	if err := agent.Run(context.Background()); err != nil {
		logger.Error("agent error", "error", err)
		os.Exit(1)
	}
}
