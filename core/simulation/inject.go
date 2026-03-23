package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// syntheticSubject is the NATS subject used to publish synthetic events.
// It routes to the SIMULATION stream (matches simulation.>).
const syntheticSubject = "simulation.synthetic.event"

// InjectRequest describes a synthetic event to inject into the SIMULATION stream.
type InjectRequest struct {
	EventType     string            `json:"event_type"`
	SourceAgent   string            `json:"source_agent"`
	TargetAgent   string            `json:"target_agent"`
	WorkflowID    string            `json:"workflow_id"`
	CorrelationID string            `json:"correlation_id"`
	PayloadJSON   json.RawMessage   `json:"payload_json"`
	Metadata      map[string]string `json:"metadata"`
}

// InjectResult contains the outcome of a synthetic event injection.
type InjectResult struct {
	EventID  string `json:"event_id"`
	Stream   string `json:"stream"`
	Sequence uint64 `json:"sequence"`
}

// EventInjector publishes synthetic events to the SIMULATION stream
// with metadata markers that identify them as synthetic.
type EventInjector struct {
	js     jetstream.JetStream
	bus    eventbus.EventBus
	nodeID string
}

// NewEventInjector creates an EventInjector.
func NewEventInjector(js jetstream.JetStream, bus eventbus.EventBus, nodeID string) *EventInjector {
	return &EventInjector{js: js, bus: bus, nodeID: nodeID}
}

// Inject publishes a synthetic event to the SIMULATION stream.
// The event is wrapped in an AgentEvent protobuf envelope with synthetic metadata.
// sessionID and scenarioName are optional — set when injecting as part of a scenario.
func (ei *EventInjector) Inject(ctx context.Context, req InjectRequest, sessionID, scenarioName string) (*InjectResult, error) {
	if req.EventType == "" {
		return nil, fmt.Errorf("event_type is required")
	}

	eventID := uuid.Must(uuid.NewV7()).String()

	meta := make(map[string]string, len(req.Metadata)+3)
	for k, v := range req.Metadata {
		meta[k] = v
	}
	meta["simulation.synthetic"] = "true"
	if sessionID != "" {
		meta["simulation.session_id"] = sessionID
	}
	if scenarioName != "" {
		meta["simulation.scenario_name"] = scenarioName
	}

	pb := &protocolv1.AgentEvent{
		Id:            eventID,
		Type:          req.EventType,
		SourceNode:    ei.nodeID,
		SourceAgent:   req.SourceAgent,
		TargetAgent:   req.TargetAgent,
		WorkflowId:    req.WorkflowID,
		CorrelationId: req.CorrelationID,
		Timestamp:     time.Now().UnixNano(),
		Metadata:      meta,
		Payload:       req.PayloadJSON,
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("marshal synthetic event: %w", err)
	}

	msg := nats.NewMsg(syntheticSubject)
	msg.Data = data
	msg.Header.Set("Nats-Msg-Id", eventID)

	ack, err := ei.js.PublishMsg(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("publish synthetic event: %w", err)
	}

	return &InjectResult{
		EventID:  eventID,
		Stream:   ack.Stream,
		Sequence: ack.Sequence,
	}, nil
}

// PublishCoordination publishes a simulation coordination event via the EventBus.
func (ei *EventInjector) PublishCoordination(ctx context.Context, eventType string, sessionID string, payload proto.Message) error {
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal coordination payload: %w", err)
	}

	return ei.bus.Publish(ctx, &eventbus.Event{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       eventType,
		SourceNode: ei.nodeID,
		Timestamp:  time.Now().UnixNano(),
		Metadata: map[string]string{
			"simulation.session_id": sessionID,
		},
		Payload: data,
	})
}
