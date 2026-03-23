# Human-in-the-Loop Decisions

Baran OS supports workflow steps that require human approval before proceeding. This enables scenarios where AI agents prepare recommendations but a human operator makes the final call — approving evacuations, authorizing resource allocation, or validating critical outputs.

## How It Works

Any workflow step can be marked as requiring human approval by setting `human_approval: true` in the step definition. When the workflow engine reaches that step:

1. The workflow transitions to `WAITING_HUMAN` status
2. A `human.decision.request` event is published with the step prompt, workflow input, and all previous step results
3. The workflow pauses until an operator approves or rejects
4. On **approve**: a synthetic step result is created (agent_id = `"human-operator"`) and the workflow resumes to the next step
5. On **reject**: the workflow fails with the operator's comment as the error message
6. On **timeout**: the workflow fails with `DECISION_TIMEOUT` (default: 24 hours)

## Defining a Human Approval Step

In your workflow definition, add a step with `human_approval: true`:

```go
steps := []workflow.StepDefinition{
    {
        Name:       "risk-estimation",
        Capability: "estimate-risk",
    },
    {
        Name:           "approve-evacuation",
        HumanApproval:  true,
        Prompt:         "Review the risk assessment and approve the evacuation plan",
        ResourceIDs:    []string{"zone-north", "zone-south"},
        TimeoutSeconds: 3600, // 1 hour (default is 24h)
    },
    {
        Name:       "execute-evacuation",
        Capability: "execute-plan",
    },
}
```

**Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `human_approval` | yes | Set to `true` to require human decision |
| `prompt` | yes | The question presented to the operator |
| `resource_ids` | no | Resource identifiers for conflict detection |
| `timeout_seconds` | no | Decision timeout in seconds (default: 86400 = 24h) |

Human approval steps do not need a `capability` field — they are not dispatched to agents.

## Operator Web UI

The runtime includes a built-in operator dashboard at `http://localhost:8080/ui/`.

### Pending Decisions

The dashboard shows all pending decisions as cards, sorted newest first. Each card displays:

- **Step name** and status badge (`PENDING` or `CONFLICT`)
- **Prompt** — the question the operator needs to answer
- **Context** — workflow ID, step index, timestamp
- **Resource tags** — if the step declares resource IDs
- **Conflict info** — links to other decisions competing for the same resources

### Approving or Rejecting

Each decision card has:
- A **comment** text field for optional notes
- An **Approve** button (green) — resumes the workflow
- A **Reject** button (red) — fails the workflow with the comment as reason

### Real-Time Updates

The UI uses Server-Sent Events (SSE) to receive live updates:
- New decisions appear immediately
- Conflict notifications update affected cards
- Resolved decisions disappear from the list

## REST API

The decision API is available alongside the health endpoint on port 8080.

### List Pending Decisions

```
GET /api/decisions
```

```json
{
  "decisions": [
    {
      "decision_id": "019...",
      "workflow_id": "019...",
      "step_index": 1,
      "step_name": "approve-evacuation",
      "prompt": "Review the risk assessment and approve the evacuation plan",
      "resource_ids": ["zone-north", "zone-south"],
      "requested_at": "2026-03-20T10:30:00Z",
      "conflict_ids": ["019..."]
    }
  ]
}
```

### Get a Single Decision

```
GET /api/decisions/{id}
```

Returns a single decision object (same fields as above). Returns 404 if not found.

### Respond to a Decision

```
POST /api/decisions/{id}/respond
```

```json
{
  "action": "approve",
  "operator_id": "carlos",
  "comment": "Risk assessment looks correct, proceed with evacuation"
}
```

**Fields:**

| Field | Required | Values |
|-------|----------|--------|
| `action` | yes | `"approve"` or `"reject"` |
| `operator_id` | yes | Identifier of the operator |
| `comment` | no | Reason or notes |

**Responses:**
- `200` — decision accepted
- `400` — invalid action or missing operator_id
- `409` — decision already resolved

### SSE Stream

```
GET /api/decisions/stream
```

Returns a `text/event-stream` with three event types:

| Event | Description |
|-------|-------------|
| `decision.new` | A new decision request was published |
| `decision.conflict` | A conflict was detected between decisions |
| `decision.resolved` | A decision was approved or rejected |

## Conflict Detection

When multiple workflows request human decisions that affect overlapping resources, the Decision Coordinator detects the conflict automatically.

**How it works:**

1. Each decision can declare `resource_ids` — identifiers for the resources it affects
2. When a new decision request arrives, the coordinator checks all pending decisions for overlapping resource IDs
3. If overlap is found, both decisions are annotated with each other's IDs in `conflict_ids`
4. A `decision.conflict` event is published on the COORDINATION stream
5. The operator UI highlights conflicting decisions with an orange border

This helps operators coordinate when, for example, two workflows both want to allocate the same emergency resources.

## Recovery

The Decision Coordinator is resilient to runtime restarts. On startup, it scans the `workflow-state` KV bucket for any workflows in `WAITING_HUMAN` status and:

- Repopulates its in-memory pending decision index
- Re-publishes `human.decision.request` events (idempotent via decision ID)
- Operators can continue reviewing decisions without interruption
