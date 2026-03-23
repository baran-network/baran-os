package federation

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// CapabilitySync propagates local agent capabilities to federated peers and
// ingests remote capability announcements into the local agent registry.
type CapabilitySync struct {
	nodeID        string
	agentRegistry registry.AgentRegistry
	bus           eventbus.EventBus
	transport     Transport
	logger        *slog.Logger

	busSubs       []eventbus.Subscription
	transportSubs []TransportSubscription
}

// NewCapabilitySync creates a new CapabilitySync.
func NewCapabilitySync(
	nodeID string,
	agentRegistry registry.AgentRegistry,
	bus eventbus.EventBus,
	transport Transport,
	logger *slog.Logger,
) *CapabilitySync {
	return &CapabilitySync{
		nodeID:        nodeID,
		agentRegistry: agentRegistry,
		bus:           bus,
		transport:     transport,
		logger:        logger.With("component", "capability-sync"),
	}
}

// Start subscribes to local capability announcements and remote federation events.
// Returns the EventBus subscriptions (for tracking in the gateway).
func (s *CapabilitySync) Start(ctx context.Context) ([]eventbus.Subscription, []TransportSubscription, error) {
	// Local: subscribe to agent.capability.announce from the EventBus.
	announceSub, err := s.bus.Subscribe(ctx, "agent.capability.announce", s.handleLocalAnnounce)
	if err != nil {
		return nil, nil, err
	}
	s.busSubs = append(s.busSubs, announceSub)

	// Remote: subscribe to federation.capability.announce via transport.
	fedAnnounceSub, err := s.transport.Subscribe(ctx, "federation.capability.announce", s.handleRemoteAnnounce)
	if err != nil {
		return nil, nil, err
	}
	s.transportSubs = append(s.transportSubs, fedAnnounceSub)

	// Remote: subscribe to federation.capability.remove via transport.
	fedRemoveSub, err := s.transport.Subscribe(ctx, "federation.capability.remove", s.handleRemoteRemove)
	if err != nil {
		return nil, nil, err
	}
	s.transportSubs = append(s.transportSubs, fedRemoveSub)

	return s.busSubs, s.transportSubs, nil
}

// Stop unsubscribes all subscriptions.
func (s *CapabilitySync) Stop() {
	for _, sub := range s.busSubs {
		_ = sub.Unsubscribe()
	}
	s.busSubs = nil

	for _, sub := range s.transportSubs {
		_ = sub.Unsubscribe()
	}
	s.transportSubs = nil
}

// PurgeNode removes all remote capabilities from a dead or departed node.
func (s *CapabilitySync) PurgeNode(nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.agentRegistry.DeregisterRemotesByNode(ctx, nodeID); err != nil {
		s.logger.Warn("failed to purge remote capabilities",
			"node_id", nodeID, "error", err)
		return
	}
	s.logger.Info("purged remote capabilities for dead node", "node_id", nodeID)
}

// handleLocalAnnounce forwards a local capability announcement to federation peers.
func (s *CapabilitySync) handleLocalAnnounce(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.CapabilityAnnouncePayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		s.logger.Warn("failed to unmarshal capability announce", "error", err)
		return nil
	}

	// Empty capabilities = agent unregistered — send a remove event to peers.
	if len(payload.Capabilities) == 0 {
		removePayload := &protocolv1.FederationCapabilityRemovePayload{
			NodeId:  s.nodeID,
			AgentId: payload.AgentId,
			Reason:  "agent_unregistered",
		}
		data, err := proto.Marshal(removePayload)
		if err != nil {
			return nil
		}
		if err := s.transport.Publish(ctx, "federation.capability.remove", data); err != nil {
			s.logger.Warn("failed to publish capability remove", "agent_id", payload.AgentId, "error", err)
		}
		return nil
	}

	// Look up the agent type from the registry.
	agentReg, _, err := s.agentRegistry.Get(ctx, payload.AgentId)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			// Agent may have been removed before we processed the event.
			return nil
		}
		s.logger.Warn("failed to get agent for capability sync",
			"agent_id", payload.AgentId, "error", err)
		return nil
	}

	// Skip remote agents — only propagate local capabilities.
	if agentReg.IsRemote() {
		return nil
	}

	fedPayload := &protocolv1.FederationCapabilityPayload{
		NodeId:    s.nodeID,
		AgentId:   payload.AgentId,
		AgentType: agentReg.AgentType,
	}
	for _, c := range payload.Capabilities {
		fedPayload.Capabilities = append(fedPayload.Capabilities, &protocolv1.Capability{
			Name:        c.Name,
			Version:     c.Version,
			Description: c.Description,
			Parameters:  c.Parameters,
		})
	}

	data, err := proto.Marshal(fedPayload)
	if err != nil {
		return nil
	}

	if err := s.transport.Publish(ctx, "federation.capability.announce", data); err != nil {
		s.logger.Warn("failed to publish federation capability announce",
			"agent_id", payload.AgentId, "error", err)
	}
	return nil
}

// handleRemoteAnnounce registers a remote agent's capabilities into the local registry.
func (s *CapabilitySync) handleRemoteAnnounce(data []byte) {
	var payload protocolv1.FederationCapabilityPayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		s.logger.Warn("failed to unmarshal federation capability announce", "error", err)
		return
	}

	// Ignore our own re-broadcast.
	if payload.NodeId == s.nodeID {
		return
	}

	var caps []registry.Capability
	for _, c := range payload.Capabilities {
		caps = append(caps, registry.Capability{
			Name:        c.Name,
			Version:     c.Version,
			Description: c.Description,
			Parameters:  c.Parameters,
		})
	}

	reg := registry.AgentRegistration{
		AgentID:      payload.AgentId,
		AgentType:    payload.AgentType,
		Capabilities: caps,
		NodeID:       payload.NodeId,
		Origin:       "remote",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.agentRegistry.RegisterRemote(ctx, reg); err != nil {
		s.logger.Warn("failed to register remote capability",
			"node_id", payload.NodeId, "agent_id", payload.AgentId, "error", err)
		return
	}

	s.logger.Info("registered remote capability",
		"node_id", payload.NodeId,
		"agent_id", payload.AgentId,
		"agent_type", payload.AgentType,
	)
}

// handleRemoteRemove deregisters a specific remote agent's capabilities.
func (s *CapabilitySync) handleRemoteRemove(data []byte) {
	var payload protocolv1.FederationCapabilityRemovePayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		s.logger.Warn("failed to unmarshal federation capability remove", "error", err)
		return
	}

	if payload.NodeId == s.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// For individual agent removal, use the remote key format.
	key := "remote." + payload.NodeId + "." + payload.AgentId
	if err := s.agentRegistry.Deregister(ctx, key); err != nil {
		s.logger.Warn("failed to deregister remote capability",
			"node_id", payload.NodeId, "agent_id", payload.AgentId, "error", err)
		return
	}

	s.logger.Info("deregistered remote capability",
		"node_id", payload.NodeId,
		"agent_id", payload.AgentId,
		"reason", payload.Reason,
	)
}
