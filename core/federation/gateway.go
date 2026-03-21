package federation

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// EventRelay handles cross-node event forwarding.
type EventRelay interface {
	// Relay sends an event to a remote node.
	Relay(ctx context.Context, targetNodeID string, event *eventbus.Event) error

	// IsRemoteAgent returns true if the agent is registered on a remote node,
	// along with the node ID hosting it.
	IsRemoteAgent(ctx context.Context, agentID string) (bool, string, error)
}

// Gateway is the top-level federation coordinator.
type Gateway interface {
	// Start initializes the federation gateway and begins discovery.
	Start(ctx context.Context) ([]eventbus.Subscription, error)

	// Stop gracefully shuts down federation (announces departure, closes connections).
	Stop(ctx context.Context) error

	// NodeRegistry returns the node registry for querying federation state.
	NodeRegistry() NodeRegistry

	// Relay returns the event relay for cross-node routing.
	Relay() EventRelay

	// IsEnabled returns true if federation is configured (seeds provided).
	IsEnabled() bool
}

// GatewayConfig holds federation configuration.
type GatewayConfig struct {
	Seeds              []string
	PSK                string
	HeartbeatInterval  time.Duration
	UnhealthyThreshold int32
	DeadThreshold      int32
	RelayTimeout       time.Duration
	LeafPort           int
	CleanupTTL         time.Duration
}

// FederationGateway is the concrete implementation of Gateway.
// It coordinates node discovery, health monitoring, capability sync, and relay.
type FederationGateway struct {
	nodeID string
	config GatewayConfig
	logger *slog.Logger

	bus           eventbus.EventBus
	agentRegistry registry.AgentRegistry
	nodeRegistry  NodeRegistry
	transport     Transport

	monitor *NodeMonitor
	capSync *CapabilitySync
	relay   *EventRelayImpl

	localRouter eventbus.EventPublisher

	// relayedWorkflows tracks workflows relayed from other nodes.
	// Key: workflowID, Value: sourceNodeID. Used to forward step results
	// back to the originating node.
	relayedWorkflows sync.Map

	transportSubs []TransportSubscription
}

// NewFederationGateway creates a new federation gateway.
// localRouter is optional (nil in standalone mode); it is used to dispatch
// incoming relayed events into the local routing pipeline.
func NewFederationGateway(
	nodeID string,
	config GatewayConfig,
	bus eventbus.EventBus,
	agentRegistry registry.AgentRegistry,
	nodeRegistry NodeRegistry,
	transport Transport,
	logger *slog.Logger,
) *FederationGateway {
	return &FederationGateway{
		nodeID:        nodeID,
		config:        config,
		logger:        logger.With("component", "federation"),
		bus:           bus,
		agentRegistry: agentRegistry,
		nodeRegistry:  nodeRegistry,
		transport:     transport,
	}
}

// SetLocalRouter injects the local event router for dispatching incoming relay events.
// Must be called before Start() if relay functionality is needed.
func (g *FederationGateway) SetLocalRouter(r eventbus.EventPublisher) {
	g.localRouter = r
}

// Start initializes the federation gateway: connects transport, subscribes to
// node events, announces this node, and starts the health monitor.
func (g *FederationGateway) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	if !g.IsEnabled() {
		g.logger.Info("federation disabled (no seeds configured)")
		return nil, nil
	}

	g.logger.Info("starting federation gateway",
		"seeds", g.config.Seeds,
		"leaf_port", g.config.LeafPort,
		"heartbeat_interval", g.config.HeartbeatInterval,
	)

	// Subscribe to node registration events via transport.
	regSub, err := g.transport.Subscribe(ctx, "federation.node.register", func(data []byte) {
		g.handleNodeRegister(data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe node.register: %w", err)
	}
	g.transportSubs = append(g.transportSubs, regSub)

	unregSub, err := g.transport.Subscribe(ctx, "federation.node.unregister", func(data []byte) {
		g.handleNodeUnregister(data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe node.unregister: %w", err)
	}
	g.transportSubs = append(g.transportSubs, unregSub)

	// Start node monitor (ping/pong heartbeats).
	g.monitor = NewNodeMonitor(g.nodeID, g.nodeRegistry, g.transport, g.config, g.logger)
	g.monitor.SetOnNodeDead(g.onNodeDead)
	g.monitor.SetOnCleanupDeadNode(func(nodeID string) {
		if g.capSync != nil {
			g.capSync.PurgeNode(nodeID)
		}
	})

	// Subscribe to incoming pings so we respond with pongs.
	pingSub, err := g.transport.Subscribe(ctx, "federation.node.health.ping", func(data []byte) {
		g.monitor.handlePing(data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe node.health.ping: %w", err)
	}
	g.transportSubs = append(g.transportSubs, pingSub)

	pongSub, err := g.monitor.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start node monitor: %w", err)
	}
	g.transportSubs = append(g.transportSubs, pongSub)

	// CapabilitySync — propagate local capabilities to peers and ingest remote.
	g.capSync = NewCapabilitySync(g.nodeID, g.agentRegistry, g.bus, g.transport, g.logger)
	busSubs, capTransportSubs, err := g.capSync.Start(ctx)
	if err != nil {
		return nil, fmt.Errorf("start capability sync: %w", err)
	}
	g.transportSubs = append(g.transportSubs, capTransportSubs...)

	// EventRelay — cross-node event forwarding.
	g.relay = NewEventRelay(g.nodeID, g.agentRegistry, g.transport, g.config.RelayTimeout, g.logger)

	// Subscribe to incoming relay events targeted at this node.
	relaySub, err := g.transport.Subscribe(ctx, fmt.Sprintf("federation.relay.%s.>", g.nodeID), func(data []byte) {
		g.handleIncomingRelay(data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe relay: %w", err)
	}
	g.transportSubs = append(g.transportSubs, relaySub)

	// Self-announce: register ourselves and broadcast to peers.
	if err := g.announceNode(ctx); err != nil {
		return nil, fmt.Errorf("announce node: %w", err)
	}

	g.logger.Info("federation gateway started", "node_id", g.nodeID)
	return busSubs, nil
}

// Stop gracefully shuts down: announces departure, stops monitor, unsubscribes, closes transport.
func (g *FederationGateway) Stop(ctx context.Context) error {
	if !g.IsEnabled() {
		return nil
	}

	g.logger.Info("stopping federation gateway")

	// Announce departure.
	payload := &protocolv1.NodeUnregisterPayload{
		NodeId: g.nodeID,
		Reason: "shutdown",
	}
	data, err := proto.Marshal(payload)
	if err == nil {
		_ = g.transport.Publish(ctx, "federation.node.unregister", data)
	}

	// Stop monitor.
	if g.monitor != nil {
		g.monitor.Stop()
	}

	// Stop capability sync.
	if g.capSync != nil {
		g.capSync.Stop()
	}

	// Unsubscribe all transport subscriptions.
	for _, sub := range g.transportSubs {
		_ = sub.Unsubscribe()
	}
	g.transportSubs = nil

	// Close transport.
	if g.transport != nil {
		return g.transport.Close()
	}
	return nil
}

func (g *FederationGateway) NodeRegistry() NodeRegistry {
	return g.nodeRegistry
}

func (g *FederationGateway) Relay() EventRelay {
	return g.relay
}

func (g *FederationGateway) IsEnabled() bool {
	return len(g.config.Seeds) > 0 || g.config.PSK != ""
}

// handleIncomingRelay deserializes a relayed event and dispatches it through
// the local router so the target agent receives it transparently.
// For workflow events, it also sets up result forwarding back to the originating node.
func (g *FederationGateway) handleIncomingRelay(data []byte) {
	var relayPayload protocolv1.FederationRelayPayload
	if err := proto.Unmarshal(data, &relayPayload); err != nil {
		g.logger.Warn("failed to unmarshal relay payload", "error", err)
		return
	}

	// Ignore relay events not targeted at us (shouldn't happen with subject filter).
	if relayPayload.TargetNodeId != g.nodeID {
		return
	}

	var agentEvent protocolv1.AgentEvent
	if err := proto.Unmarshal(relayPayload.OriginalEvent, &agentEvent); err != nil {
		g.logger.Warn("failed to unmarshal relayed agent event", "error", err)
		return
	}

	// Convert protobuf AgentEvent back to eventbus.Event.
	evt := &eventbus.Event{
		ID:            agentEvent.Id,
		Type:          agentEvent.Type,
		SourceNode:    agentEvent.SourceNode,
		SourceAgent:   agentEvent.SourceAgent,
		TargetAgent:   agentEvent.TargetAgent,
		WorkflowID:    agentEvent.WorkflowId,
		CorrelationID: agentEvent.CorrelationId,
		Timestamp:     agentEvent.Timestamp,
		Metadata:      agentEvent.Metadata,
		Payload:       agentEvent.Payload,
	}

	// Mark as relayed so the router doesn't relay it again (avoid loops).
	if evt.Metadata == nil {
		evt.Metadata = make(map[string]string)
	}
	evt.Metadata["federation.relayed"] = "true"
	evt.Metadata["federation.source_node"] = relayPayload.SourceNodeId

	g.logger.Debug("received relayed event",
		"relay_id", relayPayload.RelayId,
		"source_node", relayPayload.SourceNodeId,
		"event_type", evt.Type,
		"target_agent", evt.TargetAgent,
	)

	if g.localRouter == nil {
		g.logger.Warn("no local router configured, dropping relayed event",
			"relay_id", relayPayload.RelayId)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.config.RelayTimeout)
	defer cancel()

	// For workflow events, set up result forwarding so the agent's step result
	// can be relayed back to the originating node.
	if evt.WorkflowID != "" {
		g.setupResultForwarding(ctx, evt.WorkflowID, relayPayload.SourceNodeId)
	}

	if err := g.localRouter.Route(ctx, evt); err != nil {
		g.logger.Warn("failed to route relayed event",
			"relay_id", relayPayload.RelayId,
			"event_type", evt.Type,
			"error", err)
	}
}

// setupResultForwarding ensures the per-workflow JetStream stream exists on this
// node (so the local agent can publish results) and subscribes to step results
// to relay them back to the originating node.
func (g *FederationGateway) setupResultForwarding(ctx context.Context, workflowID, sourceNodeID string) {
	// Only set up once per workflow.
	if _, loaded := g.relayedWorkflows.LoadOrStore(workflowID, sourceNodeID); loaded {
		return
	}

	// Ensure the per-workflow stream exists so the agent can publish results.
	streamCreator, ok := g.bus.(eventbus.StreamCreator)
	if !ok {
		g.logger.Warn("bus does not support stream creation, result forwarding unavailable",
			"workflow_id", workflowID)
		return
	}

	streamName := fmt.Sprintf("WF-%s", workflowID)
	subjects := []string{fmt.Sprintf("workflow.%s.>", workflowID)}
	if err := streamCreator.EnsureStream(ctx, streamName, subjects); err != nil {
		g.logger.Warn("failed to create per-workflow stream for relay",
			"workflow_id", workflowID, "error", err)
		return
	}

	// Subscribe to step results on this workflow stream.
	resultSubject := fmt.Sprintf("workflow.%s.workflow.step.result", workflowID)
	_, err := g.bus.SubscribeWithStream(ctx, streamName, resultSubject, func(_ context.Context, resultEvt *eventbus.Event) error {
		g.forwardResultToSource(workflowID, sourceNodeID, resultEvt)
		return nil
	})
	if err != nil {
		g.logger.Warn("failed to subscribe to workflow results for relay",
			"workflow_id", workflowID, "error", err)
		return
	}

	g.logger.Debug("result forwarding set up",
		"workflow_id", workflowID,
		"source_node", sourceNodeID,
	)
}

// forwardResultToSource relays a workflow step result back to the originating node.
func (g *FederationGateway) forwardResultToSource(workflowID, sourceNodeID string, resultEvt *eventbus.Event) {
	if g.relay == nil {
		return
	}

	// Strip the federation metadata before relaying back — the originating node's
	// workflow engine doesn't expect these.
	cleanEvt := *resultEvt
	meta := make(map[string]string, len(resultEvt.Metadata))
	for k, v := range resultEvt.Metadata {
		if k != "federation.relayed" && k != "federation.source_node" {
			meta[k] = v
		}
	}
	cleanEvt.Metadata = meta

	ctx, cancel := context.WithTimeout(context.Background(), g.config.RelayTimeout)
	defer cancel()

	if err := g.relay.Relay(ctx, sourceNodeID, &cleanEvt); err != nil {
		g.logger.Warn("failed to relay step result back to source",
			"workflow_id", workflowID,
			"source_node", sourceNodeID,
			"error", err,
		)
	} else {
		g.logger.Debug("relayed step result back to source",
			"workflow_id", workflowID,
			"source_node", sourceNodeID,
		)
	}
}

// announceNode registers this node in the local registry and broadcasts
// a registration event to all peers.
func (g *FederationGateway) announceNode(ctx context.Context) error {
	now := time.Now().UnixNano()
	info := NodeInfo{
		NodeID:   g.nodeID,
		Address:  fmt.Sprintf("127.0.0.1:%d", g.config.LeafPort),
		Status:   NodeStatusActive,
		JoinedAt: now,
		Version:  "0.2.0",
	}

	if _, err := g.nodeRegistry.Register(ctx, info); err != nil {
		return fmt.Errorf("register self: %w", err)
	}

	payload := &protocolv1.NodeRegisterPayload{
		NodeId:            g.nodeID,
		Address:           info.Address,
		Version:           info.Version,
		JoinedAt:          now,
		CapabilitiesCount: 0,
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal register payload: %w", err)
	}

	return g.transport.Publish(ctx, "federation.node.register", data)
}

// handleNodeRegister processes a node.register event from a peer.
// It upserts the node into the local registry and propagates to other
// known peers if not already propagated (one-hop propagation).
func (g *FederationGateway) handleNodeRegister(data []byte) {
	var payload protocolv1.NodeRegisterPayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		g.logger.Warn("failed to unmarshal node.register", "error", err)
		return
	}

	// Ignore our own registration events.
	if payload.NodeId == g.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if this is a new node (not already in our registry).
	_, _, getErr := g.nodeRegistry.Get(ctx, payload.NodeId)
	isNewNode := getErr != nil

	info := NodeInfo{
		NodeID:            payload.NodeId,
		Address:           payload.Address,
		Version:           payload.Version,
		JoinedAt:          payload.JoinedAt,
		CapabilitiesCount: payload.CapabilitiesCount,
	}

	_, err := g.nodeRegistry.Register(ctx, info)
	if err != nil {
		g.logger.Warn("failed to register peer node",
			"node_id", payload.NodeId, "error", err)
		return
	}

	g.logger.Info("peer node registered",
		"node_id", payload.NodeId,
		"address", payload.Address,
		"version", payload.Version,
	)

	// Propagate: announce all other known nodes to the new peer so it can
	// discover nodes it missed. Also re-announce ourselves to the new peer.
	if isNewNode {
		g.propagateKnownNodes(ctx, payload.NodeId)
	}
}

// propagateKnownNodes sends registration events for all known nodes
// (including ourselves) so a newly joined peer learns the full topology.
func (g *FederationGateway) propagateKnownNodes(ctx context.Context, excludeNodeID string) {
	nodes, err := g.nodeRegistry.List(ctx)
	if err != nil {
		g.logger.Warn("failed to list nodes for propagation", "error", err)
		return
	}

	for _, n := range nodes {
		if n.NodeID == excludeNodeID {
			continue
		}

		payload := &protocolv1.NodeRegisterPayload{
			NodeId:            n.NodeID,
			Address:           n.Address,
			Version:           n.Version,
			JoinedAt:          n.JoinedAt,
			CapabilitiesCount: n.CapabilitiesCount,
		}
		data, err := proto.Marshal(payload)
		if err != nil {
			continue
		}
		_ = g.transport.Publish(ctx, "federation.node.register", data)
	}
}

// handleNodeUnregister processes a node.unregister event from a peer.
// It marks the node DEAD in the registry and triggers capability purge.
func (g *FederationGateway) handleNodeUnregister(data []byte) {
	var payload protocolv1.NodeUnregisterPayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		g.logger.Warn("failed to unmarshal node.unregister", "error", err)
		return
	}

	if payload.NodeId == g.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get current revision for CAS.
	_, rev, err := g.nodeRegistry.Get(ctx, payload.NodeId)
	if err != nil {
		g.logger.Warn("failed to get node for unregister",
			"node_id", payload.NodeId, "error", err)
		return
	}

	_, err = g.nodeRegistry.UpdateStatus(ctx, payload.NodeId, NodeStatusDead, rev)
	if err != nil {
		g.logger.Warn("failed to mark node DEAD",
			"node_id", payload.NodeId, "error", err)
		return
	}

	g.logger.Info("peer node unregistered",
		"node_id", payload.NodeId,
		"reason", payload.Reason,
	)

	g.onNodeDead(payload.NodeId)
}

// onNodeDead is called when a node transitions to DEAD status.
// It triggers capability purge via CapabilitySync.
func (g *FederationGateway) onNodeDead(nodeID string) {
	g.logger.Info("node marked dead, purging capabilities", "node_id", nodeID)
	if g.capSync != nil {
		g.capSync.PurgeNode(nodeID)
	}
}

// compile-time interface check
var _ Gateway = (*FederationGateway)(nil)
