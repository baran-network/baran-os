package federation

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// EventRelayImpl implements the EventRelay interface for cross-node event forwarding.
type EventRelayImpl struct {
	nodeID        string
	agentRegistry registry.AgentRegistry
	transport     Transport
	relayTimeout  time.Duration
	logger        *slog.Logger
}

// NewEventRelay creates a new EventRelayImpl.
func NewEventRelay(
	nodeID string,
	agentRegistry registry.AgentRegistry,
	transport Transport,
	relayTimeout time.Duration,
	logger *slog.Logger,
) *EventRelayImpl {
	return &EventRelayImpl{
		nodeID:        nodeID,
		agentRegistry: agentRegistry,
		transport:     transport,
		relayTimeout:  relayTimeout,
		logger:        logger.With("component", "relay"),
	}
}

// Relay sends an event to a remote node by serializing the original event as an
// AgentEvent protobuf, wrapping it in a FederationRelayPayload, and publishing
// to the federation.relay.{targetNodeID}.{event.Type} subject via transport.
func (r *EventRelayImpl) Relay(ctx context.Context, targetNodeID string, event *eventbus.Event) error {
	// Serialize the original event as AgentEvent protobuf.
	agentEvent := &protocolv1.AgentEvent{
		Id:            event.ID,
		Type:          event.Type,
		SourceNode:    event.SourceNode,
		SourceAgent:   event.SourceAgent,
		TargetAgent:   event.TargetAgent,
		WorkflowId:    event.WorkflowID,
		CorrelationId: event.CorrelationID,
		Timestamp:     event.Timestamp,
		Metadata:      event.Metadata,
		Payload:       event.Payload,
	}

	originalBytes, err := proto.Marshal(agentEvent)
	if err != nil {
		return fmt.Errorf("marshal original event: %w", err)
	}

	relayPayload := &protocolv1.FederationRelayPayload{
		SourceNodeId:   r.nodeID,
		TargetNodeId:   targetNodeID,
		OriginalEvent:  originalBytes,
		RelayId:        uuid.Must(uuid.NewV7()).String(),
		RelayTimestamp: time.Now().UnixNano(),
	}

	data, err := proto.Marshal(relayPayload)
	if err != nil {
		return fmt.Errorf("marshal relay payload: %w", err)
	}

	subject := fmt.Sprintf("federation.relay.%s.%s", targetNodeID, event.Type)

	r.logger.Debug("relaying event",
		"target_node", targetNodeID,
		"event_type", event.Type,
		"relay_id", relayPayload.RelayId,
		"subject", subject,
	)

	if err := r.transport.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish relay to %s: %w", targetNodeID, err)
	}

	return nil
}

// IsRemoteAgent returns true if the agent is registered on a remote node,
// along with the node ID hosting it. Since remote agents are stored under
// composite keys (remote.{nodeID}.{agentID}), a direct Get by agentID won't
// find them. This method searches through all registered agents.
func (r *EventRelayImpl) IsRemoteAgent(ctx context.Context, agentID string) (bool, string, error) {
	// Try direct lookup first (works for local agents).
	reg, _, err := r.agentRegistry.Get(ctx, agentID)
	if err == nil {
		if reg.IsRemote() {
			return true, reg.NodeID, nil
		}
		return false, "", nil
	}

	// Direct lookup failed — search all entries for a remote agent with this ID.
	agents, err := r.agentRegistry.List(ctx)
	if err != nil {
		return false, "", fmt.Errorf("list agents: %w", err)
	}
	for _, a := range agents {
		if a.AgentID == agentID && a.IsRemote() {
			return true, a.NodeID, nil
		}
	}
	return false, "", nil
}

// compile-time interface check
var _ EventRelay = (*EventRelayImpl)(nil)
