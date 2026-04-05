# Capability Taxonomy

Baran OS ships with a **standard capability taxonomy** — 48 well-known capabilities organized across 8 categories. Agents that register standard capabilities get automatic metadata (category, action, input/output types), hierarchical discovery, and cross-vendor alias matching.

## Naming Convention

Capabilities follow a two-part dot-notation: `{category}.{action}`

- `nlp.summarization` — standard capability
- `code.generation` — standard capability
- `acme.wildfire.risk_assessment` — vendor capability (3+ segments, custom first segment)

## Standard vs Vendor Capabilities

| Type | Pattern | Validation | Schema Required |
|------|---------|-----------|-----------------|
| Standard | `{std_category}.{action}` | Must exist in catalog | Auto-filled from catalog |
| Vendor | `{org}.{action}` or `{org}.{sub}.{action}` | First segment must NOT be a standard category; ≥2 segments | `input_types` + `output_types` required |

```go
// Standard — category/action/types auto-filled from catalog
sdk.WithCapability(sdk.Capability{
    Name:    "nlp.summarization",
    Version: "1.0.0",
})

// Vendor — schema required
sdk.WithCapability(sdk.Capability{
    Name:        "acme.wildfire.risk_assessment",
    Version:     "1.0.0",
    Description: "Assess wildfire risk for a geographic area",
    InputTypes:  []string{"application/json"},
    OutputTypes: []string{"application/json"},
})
```

## Hierarchical Discovery

The registry supports glob patterns for discovery:

```go
// All NLP agents
matches, _ := registry.FindByCapability(ctx, "nlp.*", "")

// Exact capability
matches, _ := registry.FindByCapability(ctx, "nlp.summarization", "1.0.0")

// All vendor capabilities for an org
matches, _ := registry.FindByCapability(ctx, "acme.wildfire.*", "")
```

## Vendor Namespace Rules

1. **First segment must not be a standard category**: `acme.risk_eval` is valid; `nlp.custom_thing` is not (because `nlp` is a standard category).
2. **At least two dot-separated segments**: `custom_thing` is invalid; `acme.custom_thing` is valid.
3. **`input_types` and `output_types` are required** — vendors must declare their schemas explicitly.

## Standard Catalog (v1.0) — 48 Capabilities

### nlp (8)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `nlp.summarization` | Summarize text content | text/plain, application/json | text/plain |
| `nlp.translation` | Translate between languages | text/plain | text/plain |
| `nlp.sentiment` | Analyze text sentiment | text/plain | application/json |
| `nlp.extraction` | Extract entities from text | text/plain | application/json |
| `nlp.classification` | Classify text into categories | text/plain | application/json |
| `nlp.generation` | Generate text from prompts | text/plain, application/json | text/plain |
| `nlp.embedding` | Generate vector embeddings | text/plain | application/json |
| `nlp.qa` | Answer questions from context | application/json | application/json |

### code (7)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `code.generation` | Generate source code | text/plain, application/json | text/plain |
| `code.review` | Review code for issues | text/plain | application/json |
| `code.refactoring` | Refactor existing code | text/plain | text/plain |
| `code.testing` | Generate test cases | text/plain | text/plain |
| `code.documentation` | Generate documentation | text/plain | text/plain |
| `code.completion` | Complete partial code | text/plain | text/plain |
| `code.analysis` | Static analysis and metrics | text/plain | application/json |

### vision (6)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `vision.ocr` | Extract text from images | image/png, image/jpeg | text/plain |
| `vision.classification` | Classify images | image/png, image/jpeg | application/json |
| `vision.detection` | Detect objects in images | image/png, image/jpeg | application/json |
| `vision.segmentation` | Segment image regions | image/png, image/jpeg | application/json |
| `vision.generation` | Generate images from prompts | text/plain | image/png |
| `vision.captioning` | Generate image captions | image/png, image/jpeg | text/plain |

### data (6)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `data.extraction` | Extract structured data from sources | application/json, text/plain | application/json |
| `data.transformation` | Transform data between formats | application/json | application/json |
| `data.validation` | Validate data against rules | application/json | application/json |
| `data.enrichment` | Enrich data with external sources | application/json | application/json |
| `data.aggregation` | Aggregate and summarize datasets | application/json | application/json |
| `data.search` | Search across data sources | application/json | application/json |

### decision (6)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `decision.approval` | Request human or automated approval | application/json | application/json |
| `decision.routing` | Route requests to appropriate handler | application/json | application/json |
| `decision.classification` | Classify inputs for decision paths | application/json | application/json |
| `decision.scoring` | Score items against criteria | application/json | application/json |
| `decision.recommendation` | Generate recommendations | application/json | application/json |
| `decision.escalation` | Escalate to higher authority | application/json | application/json |

### communication (5)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `communication.notification` | Send notifications | application/json | application/json |
| `communication.email` | Send and manage email | application/json | application/json |
| `communication.messaging` | Send messages via channels | application/json | application/json |
| `communication.scheduling` | Schedule meetings and events | application/json | application/json |
| `communication.translation` | Translate communications | text/plain | text/plain |

### orchestration (5)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `orchestration.workflow` | Manage sub-workflows | application/json | application/json |
| `orchestration.delegation` | Delegate tasks to other agents | application/json | application/json |
| `orchestration.monitoring` | Monitor agent and system status | application/json | application/json |
| `orchestration.retry` | Retry failed operations | application/json | application/json |
| `orchestration.aggregation` | Aggregate results from multiple agents | application/json | application/json |

### security (5)

| Capability | Description | Input Types | Output Types |
|-----------|-------------|-------------|-------------|
| `security.scanning` | Scan for vulnerabilities | text/plain, application/json | application/json |
| `security.validation` | Validate credentials and tokens | application/json | application/json |
| `security.audit` | Audit trail and compliance | application/json | application/json |
| `security.encryption` | Encrypt and decrypt data | application/octet-stream | application/octet-stream |
| `security.authentication` | Authenticate users and agents | application/json | application/json |

## Capability Aliases

Aliases map equivalent capabilities across vendor namespaces, enabling federation discovery without requiring identical naming.

```go
// Create alias — agents with either name become mutually discoverable
aliasRegistry.AddAlias(ctx, "acme.risk_eval", "betacorp.risk_assessment")

// Querying either name returns agents registered with either capability
matches, _ := registry.FindByCapability(ctx, "acme.risk_eval", "")
// Returns agents with "acme.risk_eval" AND "betacorp.risk_assessment"
```

Aliases are stored in the `capability-aliases` JetStream KV bucket and propagated to federated nodes automatically during federation handshake via `federation.alias.sync` events.

**Circular alias detection**: The resolver tracks visited nodes with a seen-set and stops at depth 5, returning partial results with a warning if the graph is deeper.
