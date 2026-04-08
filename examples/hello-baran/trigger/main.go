// hello-baran trigger — one-shot seed of the hello-baran workflow (FR-011).
//
// Loads the declarative workflow definition from a JSON file (default:
// /etc/hello-baran/hello.json), converts it to a protocol WorkflowStartPayload,
// and publishes a single workflow.start event via the Go SDK's NATS EventBus.
// Exits 0 on success, non-zero on emit failure. Does NOT retry.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type stepDef struct {
	Name           string          `json:"name"`
	Capability     string          `json:"capability"`
	TimeoutSeconds uint32          `json:"timeout_seconds"`
	Input          json.RawMessage `json:"input"`
}

type workflowDef struct {
	Name      string    `json:"name"`
	Initiator string    `json:"initiator"`
	Steps     []stepDef `json:"steps"`
}

func main() {
	wfPath := flag.String("workflow", "/etc/hello-baran/hello.json", "Workflow JSON file path")
	natsURL := flag.String("nats-url", envOr("BARAN_NATS_URL", "nats://localhost:4222"), "NATS server URL")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	raw, err := os.ReadFile(*wfPath)
	if err != nil {
		logger.Error("read workflow file", "path", *wfPath, "error", err)
		os.Exit(1)
	}

	var wf workflowDef
	if err := json.Unmarshal(raw, &wf); err != nil {
		logger.Error("parse workflow file", "error", err)
		os.Exit(1)
	}

	steps := make([]*protocolv1.StepDefinition, 0, len(wf.Steps))
	for _, s := range wf.Steps {
		// Pass the raw JSON object as the step's domain-typed input bytes.
		// Both hello-baran agents accept JSON {"text": ...} payloads.
		steps = append(steps, &protocolv1.StepDefinition{
			Name:           s.Name,
			Capability:     s.Capability,
			TimeoutSeconds: s.TimeoutSeconds,
			Input:          []byte(s.Input),
		})
	}

	def := &protocolv1.WorkflowDefinition{
		Name:      wf.Name,
		Initiator: wf.Initiator,
		Steps:     steps,
	}
	startPayload, err := proto.Marshal(&protocolv1.WorkflowStartPayload{Definition: def})
	if err != nil {
		logger.Error("marshal WorkflowStartPayload", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bus, err := natseventbus.New(ctx, *natsURL)
	if err != nil {
		logger.Error("connect to NATS", "url", *natsURL, "error", err)
		os.Exit(1)
	}
	defer bus.Close()

	workflowID := uuid.Must(uuid.NewV7()).String()
	if err := bus.Publish(ctx, &eventbus.Event{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   startPayload,
	}); err != nil {
		logger.Error("publish workflow.start", "error", err)
		os.Exit(1)
	}

	fmt.Printf("seeded hello-baran workflow (workflow_id=%s, steps=%d)\n", workflowID, len(steps))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
