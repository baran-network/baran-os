package federation

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// NodeMonitor performs inter-node heartbeat monitoring.
// It periodically pings all known peers and tracks missed heartbeats,
// transitioning nodes through ACTIVE → UNHEALTHY → DEAD.
type NodeMonitor struct {
	nodeID    string
	registry  NodeRegistry
	transport Transport
	config    GatewayConfig
	logger    *slog.Logger

	// onNodeDead is called when a node transitions to DEAD status.
	// The gateway sets this to trigger capability purge.
	onNodeDead func(nodeID string)

	// onCleanupDeadNode is called when a DEAD node is removed after CleanupTTL.
	// The gateway sets this to purge remote agent capabilities.
	onCleanupDeadNode func(nodeID string)

	sequence atomic.Int64
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewNodeMonitor creates a new node monitor.
func NewNodeMonitor(
	nodeID string,
	registry NodeRegistry,
	transport Transport,
	config GatewayConfig,
	logger *slog.Logger,
) *NodeMonitor {
	return &NodeMonitor{
		nodeID:    nodeID,
		registry:  registry,
		transport: transport,
		config:    config,
		logger:    logger.With("component", "node-monitor"),
	}
}

// SetOnNodeDead sets the callback invoked when a node transitions to DEAD.
func (m *NodeMonitor) SetOnNodeDead(fn func(nodeID string)) {
	m.onNodeDead = fn
}

// SetOnCleanupDeadNode sets the callback invoked when a DEAD node is permanently
// removed after the CleanupTTL expires. The gateway uses this to purge remote
// capability entries for the removed node.
func (m *NodeMonitor) SetOnCleanupDeadNode(fn func(nodeID string)) {
	m.onCleanupDeadNode = fn
}

// Start begins the heartbeat monitoring loop and subscribes to pong responses.
func (m *NodeMonitor) Start(ctx context.Context) (TransportSubscription, error) {
	// Subscribe to pong responses from peers.
	sub, err := m.transport.Subscribe(ctx, "federation.node.health.pong", func(data []byte) {
		m.handlePong(data)
	})
	if err != nil {
		return nil, err
	}

	monCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel

	m.wg.Add(1)
	go m.pingLoop(monCtx)

	if m.config.CleanupTTL > 0 {
		m.wg.Add(1)
		go m.cleanupLoop(monCtx)
	}

	return sub, nil
}

// Stop halts the heartbeat monitoring loop.
func (m *NodeMonitor) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *NodeMonitor) pingLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pingAll(ctx)
		}
	}
}

func (m *NodeMonitor) pingAll(ctx context.Context) {
	nodes, err := m.registry.List(ctx)
	if err != nil {
		m.logger.Warn("failed to list nodes for heartbeat", "error", err)
		return
	}

	seq := m.sequence.Add(1)

	for _, node := range nodes {
		if node.NodeID == m.nodeID {
			continue
		}
		if node.Status == NodeStatusDead {
			continue
		}

		// Increment missed heartbeats for this node.
		newStatus, _, err := m.registry.IncrementMissedHeartbeats(ctx, node.NodeID, node.Revision)
		if err != nil {
			m.logger.Warn("failed to increment missed heartbeats",
				"node_id", node.NodeID, "error", err)
			continue
		}

		if newStatus == NodeStatusUnhealthy && node.Status == NodeStatusActive {
			m.logger.Warn("node transitioned to UNHEALTHY",
				"node_id", node.NodeID, "missed", node.MissedHeartbeats+1)
		}

		if newStatus == NodeStatusDead && node.Status != NodeStatusDead {
			m.logger.Error("node transitioned to DEAD",
				"node_id", node.NodeID, "missed", node.MissedHeartbeats+1)
			if m.onNodeDead != nil {
				m.onNodeDead(node.NodeID)
			}
		}

		// Send ping to the peer.
		payload := &protocolv1.NodeHealthPingPayload{
			NodeId:   m.nodeID,
			Sequence: seq,
		}
		data, err := proto.Marshal(payload)
		if err != nil {
			continue
		}
		_ = m.transport.Publish(ctx, "federation.node.health.ping", data)
	}
}

func (m *NodeMonitor) cleanupLoop(ctx context.Context) {
	defer m.wg.Done()
	interval := m.config.CleanupTTL / 2
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.cleanupDeadNodes(ctx)
		}
	}
}

func (m *NodeMonitor) cleanupDeadNodes(ctx context.Context) {
	nodes, err := m.registry.List(ctx)
	if err != nil {
		m.logger.Warn("cleanup: failed to list nodes", "error", err)
		return
	}

	cutoff := time.Now().Add(-m.config.CleanupTTL).UnixNano()
	for _, node := range nodes {
		if node.NodeID == m.nodeID {
			continue
		}
		if node.Status != NodeStatusDead {
			continue
		}
		if node.LastSeen > cutoff {
			continue
		}

		if err := m.registry.Deregister(ctx, node.NodeID); err != nil {
			m.logger.Warn("cleanup: failed to deregister dead node",
				"node_id", node.NodeID, "error", err)
			continue
		}

		m.logger.Info("cleanup: removed dead node", "node_id", node.NodeID)

		if m.onCleanupDeadNode != nil {
			m.onCleanupDeadNode(node.NodeID)
		}
	}
}

func (m *NodeMonitor) handlePong(data []byte) {
	var payload protocolv1.NodeHealthPongPayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		m.logger.Warn("failed to unmarshal pong", "error", err)
		return
	}

	// Ignore our own pongs.
	if payload.NodeId == m.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get current state to obtain revision for CAS.
	_, rev, err := m.registry.Get(ctx, payload.NodeId)
	if err != nil {
		m.logger.Warn("failed to get node for heartbeat record",
			"node_id", payload.NodeId, "error", err)
		return
	}

	_, err = m.registry.RecordHeartbeat(ctx, payload.NodeId, rev)
	if err != nil {
		m.logger.Warn("failed to record heartbeat",
			"node_id", payload.NodeId, "error", err)
	}
}

// handlePing processes an incoming ping and replies with a pong.
func (m *NodeMonitor) handlePing(data []byte) {
	var payload protocolv1.NodeHealthPingPayload
	if err := proto.Unmarshal(data, &payload); err != nil {
		m.logger.Warn("failed to unmarshal ping", "error", err)
		return
	}

	// Ignore our own pings.
	if payload.NodeId == m.nodeID {
		return
	}

	pong := &protocolv1.NodeHealthPongPayload{
		NodeId:   m.nodeID,
		Sequence: payload.Sequence,
		Status:   protocolv1.NodeStatus_NODE_STATUS_HEALTHY,
	}
	pongData, err := proto.Marshal(pong)
	if err != nil {
		return
	}
	_ = m.transport.Publish(context.Background(), "federation.node.health.pong", pongData)
}
