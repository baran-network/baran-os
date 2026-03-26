package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// wsIncomingMessage is a message received from the external agent over WebSocket.
type wsIncomingMessage struct {
	Type      string            `json:"type"`       // "publish" | "ack"
	EventType string            `json:"event_type"` // for publish
	Payload   map[string]any    `json:"payload"`    // for publish
	Metadata  map[string]string `json:"metadata"`   // for publish
	EventID   string            `json:"event_id"`   // for ack
}

// wsOutgoingMessage is a message sent to the external agent over WebSocket.
type wsOutgoingMessage struct {
	Type      string          `json:"type"`       // "event" | "connected" | "error"
	EventID   string          `json:"event_id,omitempty"`
	EventType string          `json:"event_type,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Message   string          `json:"message,omitempty"`
}

// handleWebSocket upgrades to WebSocket for full-duplex event streaming.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
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

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // CORS handled at HTTP level
	})
	if err != nil {
		s.logger.Error("websocket upgrade failed", "error", err, "agent_id", agentID)
		return
	}
	defer conn.CloseNow()

	clientID, _ := uuid.NewV7()
	lastEventID := r.URL.Query().Get("last_event_id")

	wsClient := &WSClient{
		ClientID:    clientID.String(),
		AgentID:     agentID,
		ConnectedAt: now(),
		LastEventID: lastEventID,
		send:        make(chan wsMessage, 32),
		done:        make(chan struct{}),
	}
	ma.AddWSClient(wsClient)
	defer func() {
		ma.RemoveWSClient(wsClient.ClientID)
		wsClient.closeOnce.Do(func() { close(wsClient.done) })
	}()

	// Subscribe to direct events for this agent.
	subject := fmt.Sprintf("agent.direct.%s.>", agentID)
	sub, err := s.manager.bus.Subscribe(r.Context(), subject, func(ctx context.Context, event *eventbus.Event) error {
		// Normalize event type: strip routing prefixes (e.g. "agent.direct.<id>.")
		// so external agents receive the canonical type ("agent.health.ping").
		normalizedType := normalizeEventType(event.Type)
		jsonBytes, err := ProtoToJSON(s.registry, normalizedType, event.Payload)
		if err != nil {
			s.logger.Warn("cannot translate WS event to JSON", "event_type", event.Type, "error", err)
			return nil
		}
		ma.FanOutWS(wsMessage{
			msgType: "event",
			data: mustJSON(wsOutgoingMessage{
				Type:      "event",
				EventID:   event.ID,
				EventType: normalizedType,
				Payload:   json.RawMessage(jsonBytes),
			}),
		})
		return nil
	})
	if err != nil {
		_ = conn.Close(4008, "subscribe failed")
		return
	}
	defer sub.Unsubscribe()

	// Send connected message.
	_ = wsjson.Write(r.Context(), conn, wsOutgoingMessage{
		Type:    "connected",
		Message: fmt.Sprintf("agent_id=%s", agentID),
	})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// Write loop: deliver events to client.
	go func() {
		keepalive := time.NewTicker(15 * time.Second)
		defer keepalive.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-wsClient.done:
				return
			case msg := <-wsClient.send:
				_ = conn.Write(ctx, websocket.MessageText, msg.data)
			case <-keepalive.C:
				_ = conn.Ping(ctx)
			}
		}
	}()

	// Read loop: handle messages from client.
	for {
		var msg wsIncomingMessage
		if err := wsjson.Read(ctx, conn, &msg); err != nil {
			break
		}

		ma.mu.Lock()
		ma.LastActivity = now()
		ma.mu.Unlock()

		switch msg.Type {
		case "publish":
			if err := s.wsPublish(ctx, agentID, msg); err != nil {
				_ = wsjson.Write(ctx, conn, wsOutgoingMessage{
					Type:    "error",
					Message: err.Error(),
				})
			}
		case "ack":
			// Future: advance JetStream consumer position.
		default:
			_ = wsjson.Write(ctx, conn, wsOutgoingMessage{
				Type:    "error",
				Message: fmt.Sprintf("unknown message type: %s", msg.Type),
			})
		}
	}

	conn.Close(websocket.StatusNormalClosure, "")
}

// wsPublish routes a WebSocket publish message through the EventBus.
func (s *Server) wsPublish(ctx context.Context, agentID string, msg wsIncomingMessage) error {
	if msg.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	if !s.registry.Has(msg.EventType) {
		return fmt.Errorf("unknown event type: %s", msg.EventType)
	}

	payloadJSON, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	protoBytes, err := JSONToProto(s.registry, msg.EventType, payloadJSON)
	if err != nil {
		return fmt.Errorf("translate payload: %w", err)
	}

	eventID, _ := uuid.NewV7()
	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        msg.EventType,
		SourceAgent: agentID,
		Timestamp:   time.Now().UnixNano(),
		Payload:     protoBytes,
		Metadata:    msg.Metadata,
	}
	if v, ok := msg.Metadata["target_agent"]; ok {
		event.TargetAgent = v
	}

	return s.manager.bus.Publish(ctx, event)
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
