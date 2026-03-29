package federation

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/baran-network/baran-os/core/registry"
)

// AliasSync handles federation synchronization of the alias registry.
// It exchanges full snapshots on node connect and propagates individual
// alias create/delete events to peers.
type AliasSync struct {
	nodeID        string
	aliasRegistry registry.AliasRegistry
	transport     Transport
	logger        *slog.Logger

	transportSubs []TransportSubscription
}

// NewAliasSync creates a new AliasSync.
func NewAliasSync(
	nodeID string,
	aliasRegistry registry.AliasRegistry,
	transport Transport,
	logger *slog.Logger,
) *AliasSync {
	return &AliasSync{
		nodeID:        nodeID,
		aliasRegistry: aliasRegistry,
		transport:     transport,
		logger:        logger.With("component", "alias-sync"),
	}
}

// Start subscribes to federation alias events.
func (s *AliasSync) Start(ctx context.Context) ([]TransportSubscription, error) {
	// federation.alias.sync: full snapshot exchange when a peer node connects.
	syncSub, err := s.transport.Subscribe(ctx, "federation.alias.sync", s.handleAliasSync)
	if err != nil {
		return nil, err
	}
	s.transportSubs = append(s.transportSubs, syncSub)

	// federation.alias.update: propagate a single alias create or delete.
	updateSub, err := s.transport.Subscribe(ctx, "federation.alias.update", s.handleAliasUpdate)
	if err != nil {
		return nil, err
	}
	s.transportSubs = append(s.transportSubs, updateSub)

	return s.transportSubs, nil
}

// Stop unsubscribes all federation alias subscriptions.
func (s *AliasSync) Stop() {
	for _, sub := range s.transportSubs {
		_ = sub.Unsubscribe()
	}
	s.transportSubs = nil
}

// SendSnapshot publishes the local alias registry snapshot to federation peers.
// Called during federation handshake when a new node joins.
func (s *AliasSync) SendSnapshot(ctx context.Context) {
	snapshot, err := s.aliasRegistry.Snapshot(ctx)
	if err != nil {
		s.logger.Warn("failed to snapshot alias registry for federation", "error", err)
		return
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		s.logger.Warn("failed to marshal alias snapshot", "error", err)
		return
	}

	if err := s.transport.Publish(ctx, "federation.alias.sync", data); err != nil {
		s.logger.Warn("failed to publish alias snapshot", "error", err)
	}
}

// handleAliasSync applies an inbound alias registry snapshot from a remote node.
func (s *AliasSync) handleAliasSync(data []byte) {
	var snapshot registry.AliasRegistrySnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		s.logger.Warn("failed to unmarshal alias sync snapshot", "error", err)
		return
	}

	// Ignore our own snapshots re-broadcast back to us.
	if snapshot.NodeID == s.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.aliasRegistry.ApplySnapshot(ctx, &snapshot); err != nil {
		s.logger.Warn("failed to apply remote alias snapshot",
			"node_id", snapshot.NodeID, "error", err)
		return
	}

	s.logger.Info("applied remote alias snapshot",
		"node_id", snapshot.NodeID,
		"alias_count", len(snapshot.Aliases),
	)
}

// aliasUpdateMessage is the wire format for individual alias propagation.
type aliasUpdateMessage struct {
	NodeID  string               `json:"node_id"`
	Op      string               `json:"op"` // "create" or "delete"
	Mapping *registry.AliasMapping `json:"mapping,omitempty"`
	AliasID string               `json:"alias_id,omitempty"` // used for "delete" op
}

// PublishAliasCreate propagates a newly created alias to federation peers.
func (s *AliasSync) PublishAliasCreate(ctx context.Context, mapping *registry.AliasMapping) {
	msg := aliasUpdateMessage{
		NodeID:  s.nodeID,
		Op:      "create",
		Mapping: mapping,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	_ = s.transport.Publish(ctx, "federation.alias.update", data)
}

// PublishAliasDelete propagates an alias deletion to federation peers.
func (s *AliasSync) PublishAliasDelete(ctx context.Context, aliasID string) {
	msg := aliasUpdateMessage{
		NodeID:  s.nodeID,
		Op:      "delete",
		AliasID: aliasID,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	_ = s.transport.Publish(ctx, "federation.alias.update", data)
}

// handleAliasUpdate processes an individual alias create/delete event from a peer.
func (s *AliasSync) handleAliasUpdate(data []byte) {
	var msg aliasUpdateMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.logger.Warn("failed to unmarshal alias update", "error", err)
		return
	}

	if msg.NodeID == s.nodeID {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch msg.Op {
	case "create":
		if msg.Mapping == nil {
			return
		}
		snapshot := &registry.AliasRegistrySnapshot{
			NodeID:  msg.NodeID,
			Aliases: []registry.AliasMapping{*msg.Mapping},
		}
		if err := s.aliasRegistry.ApplySnapshot(ctx, snapshot); err != nil {
			s.logger.Warn("failed to apply remote alias create",
				"alias_id", msg.Mapping.ID, "error", err)
			return
		}
		s.logger.Info("applied remote alias create",
			"alias_id", msg.Mapping.ID,
			"source", msg.Mapping.Source,
			"target", msg.Mapping.Target,
		)

	case "delete":
		if msg.AliasID == "" {
			return
		}
		if err := s.aliasRegistry.RemoveAlias(ctx, msg.AliasID); err != nil {
			s.logger.Warn("failed to apply remote alias delete",
				"alias_id", msg.AliasID, "error", err)
			return
		}
		s.logger.Info("applied remote alias delete", "alias_id", msg.AliasID)

	default:
		s.logger.Warn("unknown alias update op", "op", msg.Op)
	}
}
