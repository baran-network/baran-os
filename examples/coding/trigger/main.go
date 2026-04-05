// Coding workflow trigger — submits a coding task to the Baran workflow engine
// and displays the aggregated result from the 3-step pipeline:
//
//	Step 1: code.analysis  → task analyst agent
//	Step 2: code.generation → code generator agent
//	Step 3: code.review    → code reviewer agent
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/gofrs/uuid/v5"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Domain payload types mirroring models.py.

type codingTask struct {
	Description string `json:"description"`
	Language    string `json:"language"`
}

type taskAnalysis struct {
	Requirements []string `json:"requirements"`
	Constraints  []string `json:"constraints"`
	Approach     string   `json:"approach"`
	Language     string   `json:"language"`
	Complexity   string   `json:"complexity"`
}

type generatedCode struct {
	Code        string   `json:"code"`
	Explanation string   `json:"explanation"`
	Language    string   `json:"language"`
	ToolsUsed   []string `json:"tools_used"`
}

type reviewFeedback struct {
	Verdict      string   `json:"verdict"`
	Issues       []string `json:"issues"`
	Suggestions  []string `json:"suggestions"`
	QualityScore int      `json:"quality_score"`
}

func main() {
	natsURL := flag.String("nats-url", "nats://localhost:4222", "NATS server URL")
	language := flag.String("language", "python", "Target programming language")
	timeout := flag.Duration("timeout", 120*time.Second, "Workflow timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Usage: go run trigger/main.go [flags] \"<coding task description>\"")
		fmt.Fprintln(os.Stderr, "Example: go run trigger/main.go \"Implement a palindrome checker with tests\"")
		os.Exit(1)
	}
	description := strings.Join(args, " ")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	bus, err := natseventbus.New(ctx, *natsURL)
	if err != nil {
		logger.Error("failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer bus.Close()

	// Build the initial CodingTask payload (JSON bytes, per R6).
	task := codingTask{Description: description, Language: *language}
	taskBytes, err := json.Marshal(task)
	if err != nil {
		logger.Error("failed to marshal coding task", "error", err)
		os.Exit(1)
	}

	// Define 3-step workflow using taxonomy capabilities.
	definition := &protocolv1.WorkflowDefinition{
		Name:      "coding-workflow",
		Initiator: "trigger",
		Steps: []*protocolv1.StepDefinition{
			{
				Name:           "analysis",
				Capability:     "code.analysis",
				TimeoutSeconds: 60,
				Input:          taskBytes,
			},
			{
				Name:           "generation",
				Capability:     "code.generation",
				TimeoutSeconds: 90,
			},
			{
				Name:           "review",
				Capability:     "code.review",
				TimeoutSeconds: 60,
			},
		},
	}

	startPayload := &protocolv1.WorkflowStartPayload{Definition: definition}
	data, err := proto.Marshal(startPayload)
	if err != nil {
		logger.Error("failed to marshal workflow start payload", "error", err)
		os.Exit(1)
	}

	// Subscribe to workflow completion/failure before publishing start.
	completeCh := make(chan *protocolv1.WorkflowCompletePayload, 1)
	failedCh := make(chan *protocolv1.WorkflowFailedPayload, 1)

	nc, err := nats.Connect(*natsURL)
	if err != nil {
		logger.Error("failed to connect to NATS for subscriptions", "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("workflow.*.workflow.>", func(msg *nats.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data, &pbEvent); err != nil {
			return
		}
		switch {
		case hasSuffix(pbEvent.Type, "workflow.complete"):
			var payload protocolv1.WorkflowCompletePayload
			if err := proto.Unmarshal(pbEvent.Payload, &payload); err == nil {
				select {
				case completeCh <- &payload:
				default:
				}
			}
		case hasSuffix(pbEvent.Type, "workflow.failed"):
			var payload protocolv1.WorkflowFailedPayload
			if err := proto.Unmarshal(pbEvent.Payload, &payload); err == nil {
				select {
				case failedCh <- &payload:
				default:
				}
			}
		}
	})
	if err != nil {
		logger.Error("failed to subscribe to workflow events", "error", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	// Allow subscription to initialize before publishing.
	time.Sleep(200 * time.Millisecond)

	startedAt := time.Now()
	eventID, _ := uuid.NewV7()
	if err := bus.Publish(ctx, &eventbus.Event{
		ID:        eventID.String(),
		Type:      "workflow.start",
		Timestamp: time.Now().UnixNano(),
		Payload:   data,
	}); err != nil {
		logger.Error("failed to publish workflow.start", "error", err)
		os.Exit(1)
	}

	fmt.Printf("=== Coding Workflow Started ===\n")
	fmt.Printf("Task: %s\n", description)
	fmt.Printf("Language: %s\n", *language)
	fmt.Println("Waiting for completion...")
	fmt.Println()

	select {
	case complete := <-completeCh:
		duration := time.Since(startedAt)
		printResult(complete, duration)
	case failed := <-failedCh:
		fmt.Printf("Workflow FAILED\n")
		fmt.Printf("Workflow ID: %s\n", failed.WorkflowId)
		if failed.Error != nil {
			fmt.Printf("Error: [%s] %s (step %d)\n", failed.Error.Code, failed.Error.Message, failed.FailedStep)
		}
		os.Exit(1)
	case <-ctx.Done():
		fmt.Println("Timeout: workflow did not complete within", *timeout)
		os.Exit(1)
	}
}

func printResult(complete *protocolv1.WorkflowCompletePayload, duration time.Duration) {
	fmt.Printf("=== Coding Workflow Result ===\n\n")
	fmt.Printf("Workflow ID: %s\n", complete.WorkflowId)
	fmt.Printf("Duration: %.1fs\n\n", duration.Seconds())

	for _, r := range complete.Results {
		switch r.StepIndex {
		case 0:
			printAnalysis(r.Result)
		case 1:
			printCode(r.Result)
		case 2:
			printReview(r.Result)
		}
	}
}

func printAnalysis(data []byte) {
	fmt.Println("--- Analysis ---")
	var a taskAnalysis
	if err := json.Unmarshal(data, &a); err != nil {
		fmt.Printf("  (raw) %s\n\n", string(data))
		return
	}
	fmt.Printf("Requirements: %v\n", a.Requirements)
	fmt.Printf("Constraints:  %v\n", a.Constraints)
	fmt.Printf("Approach:     %s\n", a.Approach)
	fmt.Printf("Complexity:   %s\n\n", a.Complexity)
}

func printCode(data []byte) {
	fmt.Println("--- Generated Code ---")
	var g generatedCode
	if err := json.Unmarshal(data, &g); err != nil {
		fmt.Printf("  (raw) %s\n\n", string(data))
		return
	}
	fmt.Printf("Language: %s\n", g.Language)
	if len(g.ToolsUsed) > 0 {
		fmt.Printf("Tools used: %v\n", g.ToolsUsed)
	}
	fmt.Printf("Explanation: %s\n\n", g.Explanation)
	fmt.Println(g.Code)
	fmt.Println()
}

func printReview(data []byte) {
	fmt.Println("--- Review ---")
	var rv reviewFeedback
	if err := json.Unmarshal(data, &rv); err != nil {
		fmt.Printf("  (raw) %s\n\n", string(data))
		return
	}
	fmt.Printf("Verdict: %s\n", rv.Verdict)
	fmt.Printf("Quality: %d/10\n", rv.QualityScore)
	if len(rv.Issues) > 0 {
		fmt.Printf("Issues: %v\n", rv.Issues)
	}
	if len(rv.Suggestions) > 0 {
		fmt.Printf("Suggestions: %v\n", rv.Suggestions)
	}
	fmt.Println()
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
