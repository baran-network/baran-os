package runtime

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/workflow"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

//go:embed ui/*
var uiAssets embed.FS

// UIHandler serves REST API endpoints and embedded static assets for human decision management.
type UIHandler struct {
	coordinator *workflow.DecisionCoordinator
	bus         eventbus.EventBus
	nodeID      string
	logger      *slog.Logger

	// SSE client management.
	sseClients map[string]chan sseEvent
	sseMu      sync.RWMutex
}

// sseEvent is a server-sent event.
type sseEvent struct {
	Event string
	Data  string
}

// NewUIHandler creates a UIHandler.
func NewUIHandler(coordinator *workflow.DecisionCoordinator, bus eventbus.EventBus, nodeID string, logger *slog.Logger) *UIHandler {
	return &UIHandler{
		coordinator: coordinator,
		bus:         bus,
		nodeID:      nodeID,
		logger:      logger.With("component", "ui"),
		sseClients:  make(map[string]chan sseEvent),
	}
}

// RegisterRoutes registers decision API and UI endpoints on the given mux.
func (h *UIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/decisions/stream", h.handleSSE)
	mux.HandleFunc("/api/decisions", h.handleDecisions)
	mux.HandleFunc("/api/decisions/", h.handleDecisionRoutes)

	// Serve embedded static assets at /ui/.
	uiFS, _ := fs.Sub(uiAssets, "ui")
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.FS(uiFS))))
}

// SubscribeEvents subscribes to NATS events and broadcasts them to SSE clients.
// Returns subscriptions that should be unsubscribed on shutdown.
func (h *UIHandler) SubscribeEvents(ctx context.Context) ([]eventbus.Subscription, error) {
	var subs []eventbus.Subscription

	sub, err := h.bus.Subscribe(ctx, "human.decision.request", h.handleSSEDecisionRequest)
	if err != nil {
		return nil, fmt.Errorf("subscribe human.decision.request for SSE: %w", err)
	}
	subs = append(subs, sub)

	sub, err = h.bus.Subscribe(ctx, "decision.conflict", h.handleSSEConflict)
	if err != nil {
		return nil, fmt.Errorf("subscribe decision.conflict for SSE: %w", err)
	}
	subs = append(subs, sub)

	sub, err = h.bus.Subscribe(ctx, "decision.resolved", h.handleSSEResolved)
	if err != nil {
		return nil, fmt.Errorf("subscribe decision.resolved for SSE: %w", err)
	}
	subs = append(subs, sub)

	return subs, nil
}

// --- REST API ---

// decisionJSON is the JSON representation of a pending decision.
type decisionJSON struct {
	DecisionID  string   `json:"decision_id"`
	WorkflowID  string   `json:"workflow_id"`
	StepIndex   uint32   `json:"step_index"`
	StepName    string   `json:"step_name"`
	Prompt      string   `json:"prompt"`
	ResourceIDs []string `json:"resource_ids,omitempty"`
	RequestedAt string   `json:"requested_at"`
	ConflictIDs []string `json:"conflict_ids,omitempty"`
}

func pendingToJSON(pd *workflow.PendingDecision) decisionJSON {
	return decisionJSON{
		DecisionID:  pd.DecisionID,
		WorkflowID:  pd.WorkflowID,
		StepIndex:   pd.StepIndex,
		StepName:    pd.StepName,
		Prompt:      pd.Prompt,
		ResourceIDs: pd.ResourceIDs,
		RequestedAt: time.Unix(0, pd.RequestedAt).UTC().Format(time.RFC3339),
		ConflictIDs: pd.ConflictIDs,
	}
}

// handleDecisions handles GET /api/decisions — returns all pending decisions.
func (h *UIHandler) handleDecisions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pending := h.coordinator.ListPending()
	items := make([]decisionJSON, len(pending))
	for i, pd := range pending {
		items[i] = pendingToJSON(pd)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"decisions": items,
	})
}

// handleDecisionRoutes dispatches /api/decisions/{id} and /api/decisions/{id}/respond.
func (h *UIHandler) handleDecisionRoutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/decisions/")
	if path == "" {
		http.Error(w, "invalid decision id", http.StatusBadRequest)
		return
	}

	// Check for /respond suffix.
	if strings.HasSuffix(path, "/respond") {
		decisionID := strings.TrimSuffix(path, "/respond")
		h.handleRespond(w, r, decisionID)
		return
	}

	// No other sub-paths — treat as GET /api/decisions/{id}.
	if strings.Contains(path, "/") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	h.handleGetDecision(w, r, path)
}

// handleGetDecision handles GET /api/decisions/{id}.
func (h *UIHandler) handleGetDecision(w http.ResponseWriter, r *http.Request, decisionID string) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pd := h.coordinator.GetPending(decisionID)
	if pd == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "decision not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(pendingToJSON(pd))
}

// respondRequest is the JSON body for POST /api/decisions/{id}/respond.
type respondRequest struct {
	Action     string `json:"action"`
	OperatorID string `json:"operator_id"`
	Comment    string `json:"comment"`
}

// handleRespond handles POST /api/decisions/{id}/respond.
func (h *UIHandler) handleRespond(w http.ResponseWriter, r *http.Request, decisionID string) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req respondRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
		return
	}

	// Validate action.
	var action protocolv1.DecisionAction
	switch strings.ToLower(req.Action) {
	case "approve":
		action = protocolv1.DecisionAction_DECISION_ACTION_APPROVE
	case "reject":
		action = protocolv1.DecisionAction_DECISION_ACTION_REJECT
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid action: must be 'approve' or 'reject'"})
		return
	}

	if req.OperatorID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "operator_id is required"})
		return
	}

	// Verify the decision exists.
	pd := h.coordinator.GetPending(decisionID)
	if pd == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "decision already resolved"})
		return
	}

	// Publish human.decision.response event.
	payload := &protocolv1.HumanDecisionResponsePayload{
		DecisionId:  decisionID,
		WorkflowId:  pd.WorkflowID,
		Action:      action,
		OperatorId:  req.OperatorID,
		Comment:     req.Comment,
		RespondedAt: time.Now().UnixNano(),
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to marshal response"})
		return
	}

	if err := h.bus.Publish(r.Context(), &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       "human.decision.response",
		SourceNode: h.nodeID,
		WorkflowID: pd.WorkflowID,
		Timestamp:  time.Now().UnixNano(),
		Payload:    data,
	}); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to publish response"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "accepted",
		"decision_id": decisionID,
		"action":      req.Action,
	})
}

// --- SSE ---

// handleSSE serves the SSE endpoint at GET /api/decisions/stream.
func (h *UIHandler) handleSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	clientID := uuid.Must(uuid.NewV7()).String()
	ch := make(chan sseEvent, 32)

	h.sseMu.Lock()
	h.sseClients[clientID] = ch
	h.sseMu.Unlock()

	defer func() {
		h.sseMu.Lock()
		delete(h.sseClients, clientID)
		h.sseMu.Unlock()
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case evt := <-ch:
			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Event, evt.Data)
			flusher.Flush()
		}
	}
}

// broadcastSSE sends an event to all connected SSE clients.
func (h *UIHandler) broadcastSSE(event, data string) {
	evt := sseEvent{Event: event, Data: data}
	h.sseMu.RLock()
	defer h.sseMu.RUnlock()
	for _, ch := range h.sseClients {
		select {
		case ch <- evt:
		default:
			// Drop event for slow clients.
		}
	}
}

// handleSSEDecisionRequest is an event handler that broadcasts new decision requests to SSE clients.
func (h *UIHandler) handleSSEDecisionRequest(_ context.Context, event *eventbus.Event) error {
	var payload protocolv1.HumanDecisionRequestPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return nil
	}
	data, _ := json.Marshal(map[string]interface{}{
		"decision_id":  payload.DecisionId,
		"workflow_id":  payload.WorkflowId,
		"step_index":   payload.StepIndex,
		"step_name":    payload.StepName,
		"prompt":       payload.Prompt,
		"resource_ids": payload.ResourceIds,
		"conflict_ids": payload.ConflictIds,
		"requested_at": time.Unix(0, event.Timestamp).UTC().Format(time.RFC3339),
	})
	h.broadcastSSE("decision.new", string(data))
	return nil
}

// handleSSEConflict broadcasts conflict events to SSE clients.
func (h *UIHandler) handleSSEConflict(_ context.Context, event *eventbus.Event) error {
	var payload protocolv1.DecisionConflictPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return nil
	}
	data, _ := json.Marshal(map[string]interface{}{
		"conflict_group_id": payload.ConflictGroupId,
		"decision_ids":      payload.DecisionIds,
		"resource_ids":      payload.ResourceIds,
	})
	h.broadcastSSE("decision.conflict", string(data))
	return nil
}

// handleSSEResolved broadcasts resolved events to SSE clients.
func (h *UIHandler) handleSSEResolved(_ context.Context, event *eventbus.Event) error {
	var payload protocolv1.DecisionResolvedPayload
	if err := proto.Unmarshal(event.Payload, &payload); err != nil {
		return nil
	}
	actionStr := "approve"
	if payload.Action == protocolv1.DecisionAction_DECISION_ACTION_REJECT {
		actionStr = "reject"
	}
	data, _ := json.Marshal(map[string]interface{}{
		"decision_id":          payload.DecisionId,
		"action":               actionStr,
		"related_decision_ids": payload.RelatedDecisionIds,
	})
	h.broadcastSSE("decision.resolved", string(data))
	return nil
}
