package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const workflowBucket = "workflow-state"

// WorkflowStateStore manages persistent workflow state with optimistic concurrency.
// No NATS imports allowed through this interface.
type WorkflowStateStore interface {
	// Create initializes a new workflow state. Fails if the workflow already exists.
	Create(ctx context.Context, id string, state WorkflowState) error

	// Get retrieves a workflow state and its current KV revision.
	Get(ctx context.Context, id string) (WorkflowState, uint64, error)

	// Update performs a CAS update. Returns ErrCASConflict if revision mismatches.
	Update(ctx context.Context, id string, state WorkflowState, revision uint64) error
}

// KVWorkflowStateStore implements WorkflowStateStore backed by JetStream KV.
type KVWorkflowStateStore struct {
	kv jetstream.KeyValue
}

// NewKVWorkflowStateStore creates a KVWorkflowStateStore, creating the KV bucket if needed.
func NewKVWorkflowStateStore(ctx context.Context, nc *nats.Conn) (*KVWorkflowStateStore, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  workflowBucket,
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("create KV bucket: %w", err)
	}

	return &KVWorkflowStateStore{kv: kv}, nil
}

func (s *KVWorkflowStateStore) Create(ctx context.Context, id string, state WorkflowState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal workflow state: %w", err)
	}

	_, err = s.kv.Create(ctx, id, data)
	if err != nil {
		return fmt.Errorf("create workflow %s: %w", id, err)
	}
	return nil
}

func (s *KVWorkflowStateStore) Get(ctx context.Context, id string) (WorkflowState, uint64, error) {
	entry, err := s.kv.Get(ctx, id)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return WorkflowState{}, 0, ErrWorkflowNotFound
		}
		return WorkflowState{}, 0, fmt.Errorf("get workflow %s: %w", id, err)
	}

	var state WorkflowState
	if err := json.Unmarshal(entry.Value(), &state); err != nil {
		return WorkflowState{}, 0, fmt.Errorf("unmarshal workflow %s: %w", id, err)
	}

	return state, entry.Revision(), nil
}

// ListAll returns all workflow states in the KV store.
// Used during startup for recovery of pending human decisions.
func (s *KVWorkflowStateStore) ListAll(ctx context.Context) ([]WorkflowState, error) {
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		// If there are no keys, NATS returns ErrNoKeysFound.
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("list workflow keys: %w", err)
	}

	var states []WorkflowState
	for _, key := range keys {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue
		}
		var state WorkflowState
		if err := json.Unmarshal(entry.Value(), &state); err != nil {
			continue
		}
		states = append(states, state)
	}
	return states, nil
}

func (s *KVWorkflowStateStore) Update(ctx context.Context, id string, state WorkflowState, revision uint64) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal workflow state: %w", err)
	}

	_, err = s.kv.Update(ctx, id, data, revision)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCASConflict, err)
	}
	return nil
}
