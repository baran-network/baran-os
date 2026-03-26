package sidecar

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
)

// handleSubscribeSSE opens a Server-Sent Events stream for a registered agent.
// Events targeting this agent are translated from protobuf to JSON and delivered.
func (s *Server) handleSubscribeSSE(w http.ResponseWriter, r *http.Request) {
	agentID := r.PathValue("id")
	if agentID == "" {
		writeError(w, http.StatusBadRequest, "MISSING_FIELD", "agent ID is required")
		return
	}

	ma, ok := s.manager.Get(agentID)
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "agent not found")
		return
	}

	// Negotiate SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "streaming not supported")
		return
	}

	clientID, _ := uuid.NewV7()
	lastEventID := r.Header.Get("Last-Event-ID")

	client := &SSEClient{
		ClientID:    clientID.String(),
		AgentID:     agentID,
		Channel:     make(chan sseEvent, 32),
		ConnectedAt: now(),
		LastEventID: lastEventID,
	}
	ma.AddSSEClient(client)
	defer func() {
		ma.RemoveSSEClient(client.ClientID)
	}()

	// Subscribe to direct events for this agent on the EventBus.
	subject := fmt.Sprintf("agent.direct.%s.>", agentID)
	sub, err := s.manager.bus.Subscribe(r.Context(), subject, func(ctx context.Context, event *eventbus.Event) error {
		// Normalize event type: strip routing prefixes (e.g. "agent.direct.<id>.")
		// so external agents receive the canonical type ("agent.health.ping").
		normalizedType := normalizeEventType(event.Type)
		jsonBytes, err := ProtoToJSON(s.registry, normalizedType, event.Payload)
		if err != nil {
			s.logger.Warn("cannot translate event to JSON", "event_type", event.Type, "error", err)
			return nil
		}
		ma.FanOutSSE(sseEvent{
			ID:        event.ID,
			EventType: normalizedType,
			Data:      jsonBytes,
		})
		return nil
	})
	if err != nil {
		s.logger.Error("subscribe SSE failed", "error", err, "agent_id", agentID)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to subscribe to events")
		return
	}
	defer sub.Unsubscribe()

	// Send connected event.
	fmt.Fprintf(w, ": connected agent_id=%s\n\n", agentID)
	flusher.Flush()

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case evt := <-client.Channel:
			fmt.Fprintf(w, "id: %s\n", evt.ID)
			fmt.Fprintf(w, "event: %s\n", evt.EventType)
			fmt.Fprintf(w, "data: %s\n\n", evt.Data)
			flusher.Flush()
			client.LastEventID = evt.ID

		case <-keepalive.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}
