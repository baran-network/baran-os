package federation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const nodeRegistryBucket = "node-registry"

// Sentinel errors for node registry operations.
var (
	ErrNodeNotFound   = errors.New("node not found")
	ErrNodeCAS        = errors.New("CAS conflict: revision mismatch")
	ErrNodeValidation = errors.New("validation error")
)

// NodeLifecycleStatus represents the health state of a federated node.
type NodeLifecycleStatus int

const (
	NodeStatusActive NodeLifecycleStatus = iota
	NodeStatusUnhealthy
	NodeStatusDead
)

func (s NodeLifecycleStatus) String() string {
	switch s {
	case NodeStatusActive:
		return "ACTIVE"
	case NodeStatusUnhealthy:
		return "UNHEALTHY"
	case NodeStatusDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo represents a known federated node.
type NodeInfo struct {
	NodeID            string              `json:"node_id"`
	Address           string              `json:"address"`
	Status            NodeLifecycleStatus `json:"status"`
	CapabilitiesCount int32               `json:"capabilities_count"`
	LastSeen          int64               `json:"last_seen"`
	JoinedAt          int64               `json:"joined_at"`
	MissedHeartbeats  int32               `json:"missed_heartbeats"`
	Version           string              `json:"version"`
	Revision          uint64              `json:"-"`
}

// NodeRegistry manages federated node state in a KV bucket.
type NodeRegistry interface {
	Register(ctx context.Context, info NodeInfo) (uint64, error)
	Deregister(ctx context.Context, nodeID string) error
	Get(ctx context.Context, nodeID string) (NodeInfo, uint64, error)
	List(ctx context.Context) ([]NodeInfo, error)
	UpdateStatus(ctx context.Context, nodeID string, status NodeLifecycleStatus, revision uint64) (uint64, error)
	RecordHeartbeat(ctx context.Context, nodeID string, revision uint64) (uint64, error)
	IncrementMissedHeartbeats(ctx context.Context, nodeID string, revision uint64) (NodeLifecycleStatus, uint64, error)
}

// KVNodeRegistry implements NodeRegistry backed by JetStream KV.
type KVNodeRegistry struct {
	kv                 jetstream.KeyValue
	UnhealthyThreshold int32
	DeadThreshold      int32
}

// NewKVNodeRegistry creates a KVNodeRegistry, creating the KV bucket if needed.
func NewKVNodeRegistry(ctx context.Context, nc *nats.Conn, unhealthyThreshold, deadThreshold int32) (*KVNodeRegistry, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  nodeRegistryBucket,
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("create KV bucket %s: %w", nodeRegistryBucket, err)
	}

	return &KVNodeRegistry{
		kv:                 kv,
		UnhealthyThreshold: unhealthyThreshold,
		DeadThreshold:      deadThreshold,
	}, nil
}

func (r *KVNodeRegistry) Register(ctx context.Context, info NodeInfo) (uint64, error) {
	if info.NodeID == "" {
		return 0, fmt.Errorf("%w: node_id is required", ErrNodeValidation)
	}
	if info.Address == "" {
		return 0, fmt.Errorf("%w: address is required", ErrNodeValidation)
	}

	info.Status = NodeStatusActive
	info.LastSeen = time.Now().UnixNano()
	info.MissedHeartbeats = 0
	if info.JoinedAt == 0 {
		info.JoinedAt = time.Now().UnixNano()
	}

	data, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal node info: %w", err)
	}

	// Try create first (new node).
	rev, err := r.kv.Create(ctx, info.NodeID, data)
	if err == nil {
		return rev, nil
	}

	// If key exists, read current and update with CAS.
	entry, err := r.kv.Get(ctx, info.NodeID)
	if err != nil {
		return 0, fmt.Errorf("get for re-register: %w", err)
	}

	rev, err = r.kv.Update(ctx, info.NodeID, data, entry.Revision())
	if err != nil {
		return 0, fmt.Errorf("update for re-register: %w", err)
	}
	return rev, nil
}

func (r *KVNodeRegistry) Deregister(ctx context.Context, nodeID string) error {
	err := r.kv.Purge(ctx, nodeID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("purge %s: %w", nodeID, err)
	}
	return nil
}

func (r *KVNodeRegistry) Get(ctx context.Context, nodeID string) (NodeInfo, uint64, error) {
	entry, err := r.kv.Get(ctx, nodeID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return NodeInfo{}, 0, ErrNodeNotFound
		}
		return NodeInfo{}, 0, fmt.Errorf("get %s: %w", nodeID, err)
	}

	var info NodeInfo
	if err := json.Unmarshal(entry.Value(), &info); err != nil {
		return NodeInfo{}, 0, fmt.Errorf("unmarshal %s: %w", nodeID, err)
	}
	info.Revision = entry.Revision()

	return info, entry.Revision(), nil
}

func (r *KVNodeRegistry) List(ctx context.Context) ([]NodeInfo, error) {
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("list keys: %w", err)
	}

	var nodes []NodeInfo
	for _, key := range keys {
		info, _, err := r.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		nodes = append(nodes, info)
	}
	return nodes, nil
}

func (r *KVNodeRegistry) UpdateStatus(ctx context.Context, nodeID string, status NodeLifecycleStatus, revision uint64) (uint64, error) {
	info, _, err := r.Get(ctx, nodeID)
	if err != nil {
		return 0, err
	}

	info.Status = status
	data, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, nodeID, data, revision)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNodeCAS, err)
	}
	return rev, nil
}

func (r *KVNodeRegistry) RecordHeartbeat(ctx context.Context, nodeID string, _ uint64) (uint64, error) {
	info, currentRev, err := r.Get(ctx, nodeID)
	if err != nil {
		return 0, err
	}

	info.MissedHeartbeats = 0
	info.LastSeen = time.Now().UnixNano()
	if info.Status == NodeStatusUnhealthy {
		info.Status = NodeStatusActive
	}

	data, err := json.Marshal(info)
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, nodeID, data, currentRev)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrNodeCAS, err)
	}
	return rev, nil
}

func (r *KVNodeRegistry) IncrementMissedHeartbeats(ctx context.Context, nodeID string, _ uint64) (NodeLifecycleStatus, uint64, error) {
	info, currentRev, err := r.Get(ctx, nodeID)
	if err != nil {
		return 0, 0, err
	}

	info.MissedHeartbeats++

	if info.MissedHeartbeats >= r.DeadThreshold && info.Status == NodeStatusUnhealthy {
		info.Status = NodeStatusDead
	} else if info.MissedHeartbeats >= r.UnhealthyThreshold && info.Status == NodeStatusActive {
		info.Status = NodeStatusUnhealthy
	}

	data, err := json.Marshal(info)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, nodeID, data, currentRev)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrNodeCAS, err)
	}
	return info.Status, rev, nil
}
