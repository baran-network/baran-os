# Building Agents

This guide covers the Go SDK for building Baran OS agents. The SDK handles connection, registration, capability announcement, health pings, step dispatch, idempotency, and graceful shutdown — you focus on domain logic.

## Minimal Example

A complete agent in under 20 lines:

```go
package main

import (
    "context"
    "github.com/baran-network/baran-os/sdk"
)

func main() {
    agent := sdk.New("my-agent", "analyzer", "1.0.0")

    agent.Handle(sdk.Capability{
        Name:    "analyze-data",
        Version: "1.0.0",
    }, func(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
        // Your logic here — call an LLM, run heuristics, read sensors
        return []byte(`{"result": "analysis complete"}`), nil
    })

    agent.Run(context.Background())
}
```

Run it:

```bash
go run main.go
```

The agent connects to the runtime on `localhost:4222`, registers with capability `analyze-data`, responds to health pings, and waits for workflow steps.

## Creating an Agent

```go
agent := sdk.New(name, agentType, version string, opts ...Option)
```

| Parameter | Description |
|-----------|-------------|
| `name` | Human-readable agent name (required, non-empty) |
| `agentType` | Agent type classification (e.g., "analyzer", "sensor") |
| `version` | Semantic version string |

The SDK generates a UUID v7 for the agent automatically.

### Options

```go
// Connect to a different NATS server (default: nats://localhost:4222)
sdk.WithNATSURL("nats://remote-host:4222")

// Attach metadata labels to the agent registration
sdk.WithLabels(map[string]string{"team": "platform", "env": "prod"})

// Custom structured logger
sdk.WithLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

// Max time to wait for in-flight handlers during shutdown (default: 10s)
sdk.WithShutdownTimeout(30 * time.Second)

// Size of the idempotency LRU cache (default: 10000)
sdk.WithIdempotencyCacheSize(50000)

// Provide a custom EventBus implementation
sdk.WithEventBus(myBus)
```

## Defining Capabilities

A capability declares what an agent can do. Workflow steps are matched to agents by capability name.

```go
type Capability struct {
    Name        string            // Unique capability identifier
    Version     string            // Semantic version
    Description string            // Human-readable description
    Parameters  map[string]string // Optional metadata
}
```

Register a handler for each capability:

```go
agent.Handle(sdk.Capability{
    Name:        "risk-estimation",
    Version:     "1.0.0",
    Description: "Estimates risk level from incident data",
}, handler)
```

An agent can handle multiple capabilities:

```go
agent.Handle(sdk.Capability{Name: "analyze", Version: "1.0.0"}, analyzeHandler)
agent.Handle(sdk.Capability{Name: "summarize", Version: "1.0.0"}, summarizeHandler)
```

`Handle` returns the agent pointer for method chaining. All `Handle` calls must happen before `Run()`.

## Step Handlers

A step handler receives a workflow step and returns a result:

```go
type StepHandler func(ctx context.Context, step *StepContext) ([]byte, error)
```

### StepContext

```go
type StepContext struct {
    WorkflowID      string       // UUID v7 of the parent workflow
    StepIndex       uint32       // 0-based position in the workflow
    StepName        string       // Human-readable step name
    Capability      string       // The capability this step requires
    Input           []byte       // Serialized input (domain-specific protobuf)
    PreviousResults []StepResult // Results from all prior steps
}
```

### Accessing Previous Results

Each step receives the results of all prior steps in the workflow. This is how agents build on each other's work:

```go
func myHandler(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
    // Access the original workflow input
    input := step.Input

    // Access result from step 0
    if len(step.PreviousResults) > 0 {
        prevResult := step.PreviousResults[0]
        if prevResult.Status == "SUCCESS" {
            // prevResult.Result contains the serialized output from step 0
        }
    }

    // Return your result
    return myResult, nil
}
```

### StepResult

```go
type StepResult struct {
    StepIndex uint32 // Which step (0-based)
    Status    string // "SUCCESS" or "FAILURE"
    Result    []byte // Serialized result from that step
}
```

### Error Handling

Return an error from your handler to fail the step (and the workflow):

```go
func myHandler(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
    result, err := doWork(step.Input)
    if err != nil {
        return nil, fmt.Errorf("processing failed: %w", err)
    }
    return result, nil
}
```

The SDK catches panics in handlers and converts them to errors.

### Payload Serialization

Inputs and results are `[]byte` — the SDK doesn't prescribe a serialization format. Protobuf is standard in the Baran ecosystem:

```go
func myHandler(ctx context.Context, step *sdk.StepContext) ([]byte, error) {
    // Deserialize input
    var incident pb.WildfireIncident
    if err := proto.Unmarshal(step.Input, &incident); err != nil {
        return nil, err
    }

    // Do work
    assessment := &pb.RiskAssessment{
        RiskScore:   0.8,
        ThreatLevel: "SEVERE",
    }

    // Serialize result
    return proto.Marshal(assessment)
}
```

## Agent Lifecycle

### Run (blocking)

```go
agent.Run(context.Background())
```

`Run` starts the agent and blocks until context cancellation or SIGINT/SIGTERM:

1. Connects to the EventBus (NATS)
2. Publishes `agent.register` with all capabilities
3. Subscribes to health pings and workflow steps
4. Blocks until shutdown signal
5. Performs graceful shutdown

### Start/Stop (non-blocking)

For more control:

```go
if err := agent.Start(ctx); err != nil {
    log.Fatal(err)
}

// ... do other work ...

if err := agent.Stop(ctx); err != nil {
    log.Fatal(err)
}
```

### Graceful Shutdown

When the agent shuts down (via context cancellation, SIGINT, or SIGTERM):

1. Stops accepting new workflow steps
2. Waits for in-flight handlers to complete (up to `ShutdownTimeout`, default 10s)
3. Publishes `agent.unregister`
4. Closes the EventBus connection

### Idempotency

The SDK maintains an in-memory LRU cache of processed event IDs (default size: 10,000). If the same event arrives twice (at-least-once delivery), the SDK skips the duplicate.

The cache is not persistent — if the agent restarts, it may reprocess events that arrived before the restart. Design your handlers to be idempotent when possible.

### Health Pings

The runtime sends `agent.health.ping` events periodically (default every 10 seconds). The SDK responds automatically with `agent.health.pong` — no handler code needed.

If the agent misses 3 pings, it's marked UNHEALTHY. After 6 missed pings, it's marked DEAD and deregistered.
