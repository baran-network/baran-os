package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/federation"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/taxonomy"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

const (
	operatorRingSize        = 1000
	operatorSSEBufSize      = 128
	operatorKeepalive       = 15 * time.Second
	operatorThroughputWindow = 60 * time.Second
)

// operatorStaticStreams are the JetStream streams subscribed to for the global SSE event feed.
var operatorStaticStreams = []string{
	"AGENTS", "HEALTH", "DIRECT", "DISCOVERY",
	"HUMAN", "COORDINATION", "FEDERATION", "SIMULATION",
}

// workflowLister is a minimal interface for listing all workflow states.
// Satisfied by *workflow.KVWorkflowStateStore.
type workflowLister interface {
	ListAll(ctx context.Context) ([]workflow.WorkflowState, error)
}

// operatorEventJSON is the JSON shape for events sent over GET /api/events/stream.
type operatorEventJSON struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	SourceAgent   string            `json:"source_agent"`
	SourceNode    string            `json:"source_node"`
	TargetAgent   string            `json:"target_agent,omitempty"`
	WorkflowID    string            `json:"workflow_id,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Timestamp     int64             `json:"timestamp"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	IsSimulated   bool              `json:"is_simulated"`
	Stream        string            `json:"stream"`
	Sequence      uint64            `json:"sequence"`
}

// OperatorHandler implements the read-only operator REST API and global SSE event stream.
type OperatorHandler struct {
	agentReg    registry.AgentRegistry
	workflows   workflowLister
	catalog     taxonomy.Catalog
	coordinator *workflow.DecisionCoordinator
	fed         *federation.FederationGateway
	js          jetstream.JetStream
	nodeID      string
	logger      *slog.Logger

	// SSE fan-out: keyed by client ID.
	sseMu      sync.RWMutex
	sseClients map[string]chan operatorEventJSON

	// Ring buffer for Last-Event-ID recovery.
	ringMu  sync.Mutex
	ring    [operatorRingSize]operatorEventJSON
	ringIdx int // next write position
	ringLen int // number of valid entries (0..operatorRingSize)

	// Throughput tracking: Unix nanos of recent events (rolling 60s window).
	throughputMu sync.Mutex
	eventNanos   []int64
}

// NewOperatorHandler creates an OperatorHandler.
func NewOperatorHandler(
	agentReg registry.AgentRegistry,
	workflows workflowLister,
	catalog taxonomy.Catalog,
	coordinator *workflow.DecisionCoordinator,
	fed *federation.FederationGateway,
	js jetstream.JetStream,
	nodeID string,
	logger *slog.Logger,
) *OperatorHandler {
	return &OperatorHandler{
		agentReg:    agentReg,
		workflows:   workflows,
		catalog:     catalog,
		coordinator: coordinator,
		fed:         fed,
		js:          js,
		nodeID:      nodeID,
		logger:      logger.With("component", "operator"),
		sseClients:  make(map[string]chan operatorEventJSON),
	}
}

// RegisterRoutes registers all operator API endpoints on mux.
func (h *OperatorHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/agents", h.handleAgents)
	mux.HandleFunc("/api/agents/", h.handleAgentRoutes)
	mux.HandleFunc("/api/workflows", h.handleWorkflows)
	mux.HandleFunc("/api/workflows/", h.handleWorkflowRoutes)
	mux.HandleFunc("/api/capabilities", h.handleCapabilities)
	mux.HandleFunc("/api/stats", h.handleStats)
	mux.HandleFunc("/api/events/stream", h.handleEventStream)
	mux.HandleFunc("/api/federation/nodes", h.handleFederationNodes)
}

// federationNodeJSON is the operator-facing shape for a federated node.
type federationNodeJSON struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Address           string `json:"address"`
	Status            string `json:"status"`
	AgentCount        int    `json:"agent_count"`
	WorkflowCount     int    `json:"workflow_count"`
	Capabilities      []string `json:"capabilities"`
	CapabilitiesCount int32  `json:"capabilities_count"`
	Version           string `json:"version"`
	JoinedAt          int64  `json:"joined_at"`
	LastSeen          int64  `json:"last_seen"`
	MissedHeartbeats  int32  `json:"missed_heartbeats"`
}

func (h *OperatorHandler) handleFederationNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	out := []federationNodeJSON{}
	if h.fed != nil && h.fed.IsEnabled() {
		if nodes, err := h.fed.NodeRegistry().List(r.Context()); err == nil {
			for _, n := range nodes {
				out = append(out, federationNodeJSON{
					ID:                n.NodeID,
					Name:              n.NodeID,
					Address:           n.Address,
					Status:            strings.ToLower(n.Status.String()),
					CapabilitiesCount: n.CapabilitiesCount,
					Capabilities:      []string{},
					Version:           n.Version,
					JoinedAt:          n.JoinedAt,
					LastSeen:          n.LastSeen,
					MissedHeartbeats:  n.MissedHeartbeats,
				})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// Start subscribes to all static JetStream streams and fans events out to SSE clients.
// Returns when ctx is cancelled.
func (h *OperatorHandler) Start(ctx context.Context) {
	for _, streamName := range operatorStaticStreams {
		go h.consumeStream(ctx, streamName)
	}
}

// consumeStream runs an ordered JetStream consumer on the named stream and
// broadcasts decoded events to all SSE clients.
func (h *OperatorHandler) consumeStream(ctx context.Context, streamName string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		stream, err := h.js.Stream(ctx, streamName)
		if err != nil {
			h.logger.Warn("operator: stream not found, retrying", "stream", streamName)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}

		cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
			DeliverPolicy: jetstream.DeliverNewPolicy,
		})
		if err != nil {
			h.logger.Error("operator: create ordered consumer", "stream", streamName, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		cctx, err := cons.Consume(func(msg jetstream.Msg) {
			meta, _ := msg.Metadata()
			evt := h.decodeMessage(msg, streamName, meta)
			if evt != nil {
				h.appendRing(*evt)
				h.trackThroughput()
				h.broadcastToSSE(*evt)
			}
			_ = msg.Ack()
		})
		if err != nil {
			h.logger.Error("operator: start consume", "stream", streamName, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		<-ctx.Done()
		cctx.Stop()
		return
	}
}

// decodeMessage decodes a JetStream message into an operatorEventJSON.
func (h *OperatorHandler) decodeMessage(msg jetstream.Msg, streamName string, meta *jetstream.MsgMetadata) *operatorEventJSON {
	var pbEvent protocolv1.AgentEvent
	if err := proto.Unmarshal(msg.Data(), &pbEvent); err != nil {
		return nil
	}

	var seq uint64
	if meta != nil {
		seq = meta.Sequence.Stream
	}

	isSimulated := streamName == "SIMULATION"
	if !isSimulated && pbEvent.Metadata != nil {
		_, isSimulated = pbEvent.Metadata["simulation"]
	}

	return &operatorEventJSON{
		ID:            pbEvent.Id,
		Type:          pbEvent.Type,
		SourceAgent:   pbEvent.SourceAgent,
		SourceNode:    pbEvent.SourceNode,
		TargetAgent:   pbEvent.TargetAgent,
		WorkflowID:    pbEvent.WorkflowId,
		CorrelationID: pbEvent.CorrelationId,
		Timestamp:     pbEvent.Timestamp,
		Metadata:      pbEvent.Metadata,
		IsSimulated:   isSimulated,
		Stream:        streamName,
		Sequence:      seq,
	}
}

// appendRing adds an event to the ring buffer (thread-safe).
func (h *OperatorHandler) appendRing(evt operatorEventJSON) {
	h.ringMu.Lock()
	h.ring[h.ringIdx] = evt
	h.ringIdx = (h.ringIdx + 1) % operatorRingSize
	if h.ringLen < operatorRingSize {
		h.ringLen++
	}
	h.ringMu.Unlock()
}

// trackThroughput records the current time for throughput calculation.
func (h *OperatorHandler) trackThroughput() {
	now := time.Now().UnixNano()
	cutoff := time.Now().Add(-operatorThroughputWindow).UnixNano()

	h.throughputMu.Lock()
	h.eventNanos = append(h.eventNanos, now)
	// Trim old entries.
	i := 0
	for i < len(h.eventNanos) && h.eventNanos[i] < cutoff {
		i++
	}
	h.eventNanos = h.eventNanos[i:]
	h.throughputMu.Unlock()
}

// throughputPerSec returns the approximate events/sec over the last 60s.
func (h *OperatorHandler) throughputPerSec() float64 {
	cutoff := time.Now().Add(-operatorThroughputWindow).UnixNano()
	h.throughputMu.Lock()
	count := 0
	for _, t := range h.eventNanos {
		if t >= cutoff {
			count++
		}
	}
	h.throughputMu.Unlock()
	return float64(count) / operatorThroughputWindow.Seconds()
}

// broadcastToSSE sends an event to all connected SSE clients.
func (h *OperatorHandler) broadcastToSSE(evt operatorEventJSON) {
	h.sseMu.RLock()
	defer h.sseMu.RUnlock()
	for _, ch := range h.sseClients {
		select {
		case ch <- evt:
		default:
			// Drop for slow clients — gap will be sent on reconnect.
		}
	}
}

// findInRing returns all events after lastID (exclusive) in ring buffer order.
// Returns nil, false if lastID is not found (gap scenario).
func (h *OperatorHandler) findInRing(lastID string) ([]operatorEventJSON, bool) {
	h.ringMu.Lock()
	defer h.ringMu.Unlock()

	if h.ringLen == 0 {
		return nil, true // empty ring — no gap
	}

	// Walk ring from oldest to newest.
	start := 0
	if h.ringLen == operatorRingSize {
		start = h.ringIdx // oldest entry
	}

	found := false
	var result []operatorEventJSON
	for i := 0; i < h.ringLen; i++ {
		idx := (start + i) % operatorRingSize
		if found {
			result = append(result, h.ring[idx])
		}
		if h.ring[idx].ID == lastID {
			found = true
		}
	}

	if !found {
		return nil, false // lastID not in ring → gap
	}
	return result, true
}

// --- REST Handlers ---

// agentJSON is the JSON shape for GET /api/agents and GET /api/agents/{id}.
type agentJSON struct {
	ID            string              `json:"id"`
	Name          string              `json:"name"`
	Type          string              `json:"type"`
	Version       string              `json:"version"`
	Status        string              `json:"status"`
	Origin        string              `json:"origin"`
	Capabilities  []capabilityJSON    `json:"capabilities"`
	Labels        map[string]string   `json:"labels,omitempty"`
	LastHeartbeat string              `json:"last_heartbeat,omitempty"`
	Resources     *agentResourcesJSON `json:"resources"`
	RegisteredAt  string              `json:"registered_at,omitempty"`
	NodeID        string              `json:"node_id"`
}

// agentResourcesJSON is the JSON shape for agent resource metrics.
type agentResourcesJSON struct {
	CPUPercent    float64 `json:"cpu_percent"`
	MemoryBytes   int64   `json:"memory_bytes"`
	PendingEvents int64   `json:"pending_events"`
}

// capabilityJSON is the JSON shape for a capability in the agent response.
type capabilityJSON struct {
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Description string            `json:"description,omitempty"`
	Category    string            `json:"category,omitempty"`
	Parameters  map[string]string `json:"parameters,omitempty"`
}

func registrationToJSON(reg registry.AgentRegistration) agentJSON {
	caps := make([]capabilityJSON, len(reg.Capabilities))
	for i, c := range reg.Capabilities {
		caps[i] = capabilityJSON{
			Name:        c.Name,
			Version:     c.Version,
			Description: c.Description,
			Category:    c.Category,
			Parameters:  c.Parameters,
		}
	}

	origin := "native"
	if reg.Origin == "a2a" {
		origin = "a2a"
	}

	status := "unknown"
	switch reg.Status {
	case registry.StatusActive:
		status = "active"
	case registry.StatusUnhealthy:
		status = "unhealthy"
	case registry.StatusDead:
		status = "dead"
	case registry.StatusUnregistered:
		status = "unregistered"
	}

	a := agentJSON{
		ID:           reg.AgentID,
		Name:         reg.AgentType,
		Type:         reg.AgentType,
		Version:      reg.Version,
		Status:       status,
		Origin:       origin,
		Capabilities: caps,
		Labels:       reg.Labels,
		NodeID:       reg.NodeID,
		Resources:    nil, // populated from health pong — not stored in registry
	}
	if !reg.LastSeen.IsZero() {
		a.LastHeartbeat = reg.LastSeen.UTC().Format(time.RFC3339)
	}
	return a
}

func (h *OperatorHandler) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	all, err := h.agentReg.List(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list agents")
		return
	}

	q := r.URL.Query()
	statusFilter := strings.ToLower(q.Get("status"))
	capFilter := q.Get("capability")
	typeFilter := q.Get("type")
	nameFilter := strings.ToLower(q.Get("q"))

	var filtered []agentJSON
	for _, reg := range all {
		a := registrationToJSON(reg)
		if statusFilter != "" && a.Status != statusFilter {
			continue
		}
		if typeFilter != "" && !strings.EqualFold(reg.AgentType, typeFilter) {
			continue
		}
		if nameFilter != "" && !strings.Contains(strings.ToLower(reg.AgentType), nameFilter) {
			continue
		}
		if capFilter != "" {
			found := false
			for _, c := range reg.Capabilities {
				if c.Name == capFilter {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		filtered = append(filtered, a)
	}

	if filtered == nil {
		filtered = []agentJSON{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"agents": filtered,
		"total":  len(filtered),
	})
}

func (h *OperatorHandler) handleAgentRoutes(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/agents/")
	if id == "" || strings.Contains(id, "/") {
		writeJSONError(w, http.StatusBadRequest, "invalid agent id")
		return
	}
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	reg, _, err := h.agentReg.Get(r.Context(), id)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "agent not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(registrationToJSON(reg))
}

// workflowJSON is the JSON shape for GET /api/workflows.
type workflowJSON struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Status      string         `json:"status"`
	CurrentStep int            `json:"current_step"`
	TotalSteps  int            `json:"total_steps"`
	Initiator   string         `json:"initiator"`
	CreatedAt   string         `json:"created_at"`
	UpdatedAt   string         `json:"updated_at"`
	Error       *string        `json:"error"`
	Steps       []wfStepJSON   `json:"steps,omitempty"`
}

// wfStepJSON is the JSON shape for a workflow step.
type wfStepJSON struct {
	Index       int     `json:"index"`
	Name        string  `json:"name"`
	Capability  string  `json:"capability"`
	AssignedAgent string `json:"assigned_agent,omitempty"`
	Status      string  `json:"status"`
	StartedAt   string  `json:"started_at,omitempty"`
	CompletedAt string  `json:"completed_at,omitempty"`
	DurationMs  *int64  `json:"duration_ms,omitempty"`
}

func workflowStatusString(s workflow.WorkflowStatus) string {
	switch s {
	case workflow.StatusCreated:
		return "created"
	case workflow.StatusRunning:
		return "running"
	case workflow.StatusCompleted:
		return "completed"
	case workflow.StatusFailed:
		return "failed"
	case workflow.StatusWaitingHuman:
		return "waiting_human"
	default:
		return "unknown"
	}
}

func stateToJSON(state workflow.WorkflowState, includeSteps bool) workflowJSON {
	wf := workflowJSON{
		ID:          state.WorkflowID,
		Name:        state.Definition.Name,
		Status:      workflowStatusString(state.Status),
		CurrentStep: int(state.CurrentStep),
		TotalSteps:  len(state.Definition.Steps),
		Initiator:   state.Definition.Initiator,
		CreatedAt:   time.Unix(0, state.CreatedAt).UTC().Format(time.RFC3339),
		UpdatedAt:   time.Unix(0, state.UpdatedAt).UTC().Format(time.RFC3339),
	}

	if state.Error != nil {
		msg := state.Error.Message
		wf.Error = &msg
	}

	if includeSteps {
		steps := make([]wfStepJSON, len(state.Definition.Steps))
		for i, def := range state.Definition.Steps {
			step := wfStepJSON{
				Index:      i,
				Name:       def.Name,
				Capability: def.Capability,
				Status:     "pending",
			}
			// Match step result by index.
			for _, res := range state.StepResults {
				if int(res.StepIndex) == i {
					if res.Status == workflow.StepStatusSuccess {
						step.Status = "completed"
					} else if res.Status == workflow.StepStatusFailure {
						step.Status = "failed"
					}
					if res.CompletedAt > 0 {
						step.CompletedAt = time.Unix(0, res.CompletedAt).UTC().Format(time.RFC3339)
					}
					step.AssignedAgent = res.AgentID
					break
				}
			}
			// Mark current step as running.
			if int(state.CurrentStep) == i && (state.Status == workflow.StatusRunning || state.Status == workflow.StatusWaitingHuman) {
				if step.Status == "pending" {
					step.Status = "running"
				}
			}
			steps[i] = step
		}
		wf.Steps = steps
	}

	return wf
}

func (h *OperatorHandler) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	states, err := h.workflows.ListAll(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list workflows")
		return
	}

	q := r.URL.Query()
	statusFilter := q.Get("status")

	limit := 100
	if s := q.Get("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 1000 {
			limit = v
		}
	}
	offset := 0
	if s := q.Get("offset"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}

	var filtered []workflowJSON
	for _, state := range states {
		wf := stateToJSON(state, false)
		if statusFilter != "" && wf.Status != statusFilter {
			continue
		}
		filtered = append(filtered, wf)
	}

	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := filtered[offset:end]
	if page == nil {
		page = []workflowJSON{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"workflows": page,
		"total":     total,
		"limit":     limit,
		"offset":    offset,
	})
}

func (h *OperatorHandler) handleWorkflowRoutes(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/workflows/")
	if id == "" || strings.Contains(id, "/") {
		writeJSONError(w, http.StatusBadRequest, "invalid workflow id")
		return
	}
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	states, err := h.workflows.ListAll(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list workflows")
		return
	}
	for _, state := range states {
		if state.WorkflowID == id {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(stateToJSON(state, true))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "workflow not found"})
}

// capCatalogJSON is the JSON shape for GET /api/capabilities.
type capCatalogJSON struct {
	Name       string `json:"name"`
	Category   string `json:"category"`
	Description string `json:"description"`
	AgentCount int    `json:"agent_count"`
}

func (h *OperatorHandler) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	entries := h.catalog.Query("*.*")

	// Count agents per capability.
	agentCounts := make(map[string]int)
	if agents, err := h.agentReg.List(r.Context()); err == nil {
		for _, a := range agents {
			for _, c := range a.Capabilities {
				agentCounts[c.Name]++
			}
		}
	}

	caps := make([]capCatalogJSON, len(entries))
	for i, e := range entries {
		caps[i] = capCatalogJSON{
			Name:        e.Name,
			Category:    e.Category,
			Description: e.Description,
			AgentCount:  agentCounts[e.Name],
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"capabilities": caps,
		"total":        len(caps),
	})
}

// statsJSON is the JSON shape for GET /api/stats.
type statsJSON struct {
	TotalAgents      int     `json:"total_agents"`
	HealthyCount     int     `json:"healthy_count"`
	DegradedCount    int     `json:"degraded_count"`
	OfflineCount     int     `json:"offline_count"`
	UnknownCount     int     `json:"unknown_count"`
	EventThroughput  float64 `json:"event_throughput"`
	ActiveWorkflows  int     `json:"active_workflows"`
	PendingDecisions int     `json:"pending_decisions"`
	FederationNodes  int     `json:"federation_nodes"`
}

func (h *OperatorHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats := statsJSON{
		EventThroughput: h.throughputPerSec(),
	}

	// Agent counts.
	if agents, err := h.agentReg.List(r.Context()); err == nil {
		stats.TotalAgents = len(agents)
		for _, a := range agents {
			switch a.Status {
			case registry.StatusActive:
				stats.HealthyCount++
			case registry.StatusUnhealthy:
				stats.DegradedCount++
			case registry.StatusDead, registry.StatusUnregistered:
				stats.OfflineCount++
			default:
				stats.UnknownCount++
			}
		}
	}

	// Workflow counts.
	if states, err := h.workflows.ListAll(r.Context()); err == nil {
		for _, s := range states {
			if s.Status == workflow.StatusRunning || s.Status == workflow.StatusWaitingHuman {
				stats.ActiveWorkflows++
			}
		}
	}

	// Pending decisions.
	stats.PendingDecisions = len(h.coordinator.ListPending())

	// Federation nodes.
	if h.fed != nil && h.fed.IsEnabled() {
		if nodes, err := h.fed.NodeRegistry().List(r.Context()); err == nil {
			stats.FederationNodes = len(nodes)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

// handleEventStream serves GET /api/events/stream as an SSE endpoint.
func (h *OperatorHandler) handleEventStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSONError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	// Parse filters.
	q := r.URL.Query()
	typePrefix := q.Get("type")
	agentFilter := q.Get("agent")
	workflowFilter := q.Get("workflow_id")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher.Flush()

	// Handle Last-Event-ID reconnection.
	lastID := r.Header.Get("Last-Event-ID")
	if lastID != "" {
		if missed, found := h.findInRing(lastID); found {
			for _, evt := range missed {
				if !h.eventMatchesFilter(evt, typePrefix, agentFilter, workflowFilter) {
					continue
				}
				data, _ := json.Marshal(evt)
				_, _ = fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", evt.Type, evt.ID, data)
			}
		} else {
			// lastID not in ring — send gap marker.
			gapData, _ := json.Marshal(map[string]interface{}{
				"skipped": operatorRingSize,
				"reason":  "client behind",
			})
			_, _ = fmt.Fprintf(w, "event: gap\ndata: %s\n\n", gapData)
		}
		flusher.Flush()
	}

	// Register SSE client.
	clientID := uuid.Must(uuid.NewV7()).String()
	ch := make(chan operatorEventJSON, operatorSSEBufSize)

	h.sseMu.Lock()
	h.sseClients[clientID] = ch
	h.sseMu.Unlock()

	defer func() {
		h.sseMu.Lock()
		delete(h.sseClients, clientID)
		h.sseMu.Unlock()
	}()

	keepalive := time.NewTicker(operatorKeepalive)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case <-keepalive.C:
			_, _ = fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()

		case evt := <-ch:
			if !h.eventMatchesFilter(evt, typePrefix, agentFilter, workflowFilter) {
				continue
			}
			data, err := json.Marshal(evt)
			if err != nil {
				continue
			}
			_, _ = fmt.Fprintf(w, "event: %s\nid: %s\ndata: %s\n\n", evt.Type, evt.ID, data)
			flusher.Flush()
		}
	}
}

// eventMatchesFilter returns true if the event passes all SSE query filters.
func (h *OperatorHandler) eventMatchesFilter(evt operatorEventJSON, typePrefix, agentFilter, workflowFilter string) bool {
	if typePrefix != "" {
		// Support glob-style prefix: "workflow.*" → match "workflow.step"
		prefix := strings.TrimSuffix(typePrefix, ".*")
		prefix = strings.TrimSuffix(prefix, ".")
		if !strings.HasPrefix(evt.Type, prefix) {
			return false
		}
	}
	if agentFilter != "" && evt.SourceAgent != agentFilter && evt.TargetAgent != agentFilter {
		return false
	}
	if workflowFilter != "" && evt.WorkflowID != workflowFilter {
		return false
	}
	return true
}
