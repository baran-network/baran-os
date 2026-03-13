package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const bucketName = "agent-registry"

// KVRegistry implements AgentRegistry backed by JetStream KV.
type KVRegistry struct {
	kv jetstream.KeyValue
	// Thresholds for state transitions.
	UnhealthyThreshold int32
	DeadThreshold      int32
}

// NewKVRegistry creates a KVRegistry, creating the KV bucket if needed.
func NewKVRegistry(ctx context.Context, nc *nats.Conn, unhealthyThreshold, deadThreshold int32) (*KVRegistry, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  bucketName,
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("create KV bucket: %w", err)
	}

	return &KVRegistry{
		kv:                 kv,
		UnhealthyThreshold: unhealthyThreshold,
		DeadThreshold:      deadThreshold,
	}, nil
}

func (r *KVRegistry) Register(ctx context.Context, reg AgentRegistration) (uint64, error) {
	if reg.AgentID == "" {
		return 0, fmt.Errorf("%w: agent_id is required", ErrValidation)
	}
	if reg.AgentType == "" {
		return 0, fmt.Errorf("%w: agent_type is required", ErrValidation)
	}
	if len(reg.Capabilities) == 0 {
		return 0, fmt.Errorf("%w: at least one capability is required", ErrValidation)
	}

	reg.Status = StatusActive
	reg.LastSeen = time.Now()
	reg.MissedHeartbeats = 0

	data, err := json.Marshal(reg)
	if err != nil {
		return 0, fmt.Errorf("marshal registration: %w", err)
	}

	// Try create first (new agent).
	rev, err := r.kv.Create(ctx, reg.AgentID, data)
	if err == nil {
		return rev, nil
	}

	// If key exists, read current and update with CAS.
	entry, err := r.kv.Get(ctx, reg.AgentID)
	if err != nil {
		return 0, fmt.Errorf("get for re-register: %w", err)
	}

	rev, err = r.kv.Update(ctx, reg.AgentID, data, entry.Revision())
	if err != nil {
		return 0, fmt.Errorf("update for re-register: %w", err)
	}
	return rev, nil
}

func (r *KVRegistry) Deregister(ctx context.Context, agentID string) error {
	err := r.kv.Purge(ctx, agentID)
	if err != nil {
		// Purge on nonexistent key — treat as idempotent.
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return fmt.Errorf("purge %s: %w", agentID, err)
	}
	return nil
}

func (r *KVRegistry) Get(ctx context.Context, agentID string) (AgentRegistration, uint64, error) {
	entry, err := r.kv.Get(ctx, agentID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return AgentRegistration{}, 0, ErrNotFound
		}
		return AgentRegistration{}, 0, fmt.Errorf("get %s: %w", agentID, err)
	}

	var reg AgentRegistration
	if err := json.Unmarshal(entry.Value(), &reg); err != nil {
		return AgentRegistration{}, 0, fmt.Errorf("unmarshal %s: %w", agentID, err)
	}
	reg.Revision = entry.Revision()

	return reg, entry.Revision(), nil
}

func (r *KVRegistry) List(ctx context.Context) ([]AgentRegistration, error) {
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("list keys: %w", err)
	}

	var regs []AgentRegistration
	for _, key := range keys {
		reg, _, err := r.Get(ctx, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		regs = append(regs, reg)
	}
	return regs, nil
}

func (r *KVRegistry) UpdateStatus(ctx context.Context, agentID string, status AgentLifecycleStatus, revision uint64) (uint64, error) {
	reg, _, err := r.Get(ctx, agentID)
	if err != nil {
		return 0, err
	}

	reg.Status = status
	data, err := json.Marshal(reg)
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, agentID, data, revision)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrCASConflict, err)
	}
	return rev, nil
}

func (r *KVRegistry) RecordHeartbeat(ctx context.Context, agentID string, revision uint64) (uint64, error) {
	reg, _, err := r.Get(ctx, agentID)
	if err != nil {
		return 0, err
	}

	reg.MissedHeartbeats = 0
	reg.LastSeen = time.Now()
	if reg.Status == StatusUnhealthy {
		reg.Status = StatusActive
	}

	data, err := json.Marshal(reg)
	if err != nil {
		return 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, agentID, data, revision)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrCASConflict, err)
	}
	return rev, nil
}

func (r *KVRegistry) IncrementMissedHeartbeats(ctx context.Context, agentID string, revision uint64) (AgentLifecycleStatus, uint64, error) {
	reg, _, err := r.Get(ctx, agentID)
	if err != nil {
		return 0, 0, err
	}

	reg.MissedHeartbeats++

	if reg.MissedHeartbeats >= r.DeadThreshold && reg.Status == StatusUnhealthy {
		reg.Status = StatusDead
	} else if reg.MissedHeartbeats >= r.UnhealthyThreshold && reg.Status == StatusActive {
		reg.Status = StatusUnhealthy
	}

	data, err := json.Marshal(reg)
	if err != nil {
		return 0, 0, fmt.Errorf("marshal: %w", err)
	}

	rev, err := r.kv.Update(ctx, agentID, data, revision)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrCASConflict, err)
	}
	return reg.Status, rev, nil
}
