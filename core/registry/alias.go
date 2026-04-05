package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const aliasBucketName = "capability-aliases"
const maxAliasDepth = 5

// AliasMapping defines equivalence between two capability names.
type AliasMapping struct {
	ID        string `json:"id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	NodeID    string `json:"node_id"`
	CreatedAt int64  `json:"created_at"`
}

// AliasRegistrySnapshot is the full set of aliases for federation sync.
type AliasRegistrySnapshot struct {
	NodeID  string         `json:"node_id"`
	Aliases []AliasMapping `json:"aliases"`
}

// AliasRegistry manages capability name equivalences for cross-vendor
// and cross-ecosystem discovery.
type AliasRegistry interface {
	// AddAlias creates a bidirectional alias between two capability names.
	// Returns error if source == target or if adding would create a cycle.
	AddAlias(ctx context.Context, source, target string) (*AliasMapping, error)

	// RemoveAlias deletes an alias by ID.
	RemoveAlias(ctx context.Context, aliasID string) error

	// Resolve returns all equivalent capability names for the given name,
	// following alias chains up to maxDepth (default 5).
	// Always includes the input name in results.
	Resolve(ctx context.Context, capabilityName string) ([]string, error)

	// ListAliases returns all aliases, optionally filtered by node.
	ListAliases(ctx context.Context, nodeFilter string) ([]AliasMapping, error)

	// Snapshot returns all aliases for federation sync.
	Snapshot(ctx context.Context) (*AliasRegistrySnapshot, error)

	// ApplySnapshot merges remote aliases from a federated node.
	// Conflict resolution: last-write-wins by created_at timestamp.
	ApplySnapshot(ctx context.Context, snapshot *AliasRegistrySnapshot) error
}

// KVAliasRegistry implements AliasRegistry backed by JetStream KV.
type KVAliasRegistry struct {
	kv     jetstream.KeyValue
	nodeID string
	logger *slog.Logger
}

// NewKVAliasRegistry creates or opens the capability-aliases KV bucket.
func NewKVAliasRegistry(ctx context.Context, nc *nats.Conn, nodeID string, logger *slog.Logger) (*KVAliasRegistry, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  aliasBucketName,
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("create alias KV bucket: %w", err)
	}
	return &KVAliasRegistry{kv: kv, nodeID: nodeID, logger: logger}, nil
}

// AddAlias creates a bidirectional equivalence between source and target.
func (r *KVAliasRegistry) AddAlias(ctx context.Context, source, target string) (*AliasMapping, error) {
	if source == "" || target == "" {
		return nil, fmt.Errorf("source and target capability names are required")
	}
	if source == target {
		return nil, fmt.Errorf("source and target must be different capability names")
	}

	// Cycle detection: resolve target and check if source is reachable.
	resolved, err := r.Resolve(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("resolve target for cycle check: %w", err)
	}
	for _, name := range resolved {
		if name == source {
			return nil, fmt.Errorf("adding alias %q → %q would create a cycle", source, target)
		}
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate alias ID: %w", err)
	}

	mapping := &AliasMapping{
		ID:        id.String(),
		Source:    source,
		Target:    target,
		NodeID:    r.nodeID,
		CreatedAt: time.Now().UnixNano(),
	}

	data, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("marshal alias: %w", err)
	}

	if _, err := r.kv.Create(ctx, mapping.ID, data); err != nil {
		return nil, fmt.Errorf("store alias: %w", err)
	}

	return mapping, nil
}

// RemoveAlias deletes an alias by ID. Idempotent.
func (r *KVAliasRegistry) RemoveAlias(ctx context.Context, aliasID string) error {
	err := r.kv.Purge(ctx, aliasID)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("purge alias %s: %w", aliasID, err)
	}
	return nil
}

// Resolve returns all equivalent capability names for the given name,
// following bidirectional alias chains up to maxAliasDepth.
// Always includes the input name as the first element.
func (r *KVAliasRegistry) Resolve(ctx context.Context, capabilityName string) ([]string, error) {
	seen := map[string]struct{}{capabilityName: {}}
	result := []string{capabilityName}

	if err := r.resolveFrom(ctx, capabilityName, seen, &result, 0); err != nil {
		// Return partial results with the error — callers log the warning.
		return result, err
	}

	return result, nil
}

func (r *KVAliasRegistry) resolveFrom(ctx context.Context, name string, seen map[string]struct{}, result *[]string, depth int) error {
	if depth >= maxAliasDepth {
		if r.logger != nil {
			r.logger.Warn("max alias resolution depth exceeded",
				"capability", name,
				"depth", depth,
			)
		}
		return nil
	}

	all, err := r.listAll(ctx)
	if err != nil {
		return err
	}

	for _, alias := range all {
		var next string
		switch {
		case alias.Source == name:
			next = alias.Target
		case alias.Target == name:
			next = alias.Source
		default:
			continue
		}

		if _, already := seen[next]; already {
			continue
		}
		seen[next] = struct{}{}
		*result = append(*result, next)

		if err := r.resolveFrom(ctx, next, seen, result, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// ListAliases returns all aliases, optionally filtered by nodeFilter.
// Pass empty string to return all aliases.
func (r *KVAliasRegistry) ListAliases(ctx context.Context, nodeFilter string) ([]AliasMapping, error) {
	all, err := r.listAll(ctx)
	if err != nil {
		return nil, err
	}
	if nodeFilter == "" {
		return all, nil
	}
	var filtered []AliasMapping
	for _, a := range all {
		if a.NodeID == nodeFilter {
			filtered = append(filtered, a)
		}
	}
	return filtered, nil
}

// Snapshot returns all aliases for federation sync.
func (r *KVAliasRegistry) Snapshot(ctx context.Context) (*AliasRegistrySnapshot, error) {
	all, err := r.listAll(ctx)
	if err != nil {
		return nil, err
	}
	return &AliasRegistrySnapshot{
		NodeID:  r.nodeID,
		Aliases: all,
	}, nil
}

// ApplySnapshot merges remote aliases. Conflict resolution: last-write-wins by CreatedAt.
func (r *KVAliasRegistry) ApplySnapshot(ctx context.Context, snapshot *AliasRegistrySnapshot) error {
	if snapshot == nil {
		return nil
	}
	for _, alias := range snapshot.Aliases {
		entry, getErr := r.kv.Get(ctx, alias.ID)

		if getErr != nil && !errors.Is(getErr, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("get alias %s for merge: %w", alias.ID, getErr)
		}

		data, err := json.Marshal(alias)
		if err != nil {
			continue // best-effort; skip malformed entries
		}

		if errors.Is(getErr, jetstream.ErrKeyNotFound) {
			// New alias from remote — create it.
			_, _ = r.kv.Create(ctx, alias.ID, data)
		} else {
			// Key exists — last-write-wins by timestamp.
			var cur AliasMapping
			if json.Unmarshal(entry.Value(), &cur) == nil && cur.CreatedAt >= alias.CreatedAt {
				continue // local is newer or equal; skip
			}
			_, _ = r.kv.Update(ctx, alias.ID, data, entry.Revision())
		}
	}
	return nil
}

// listAll returns all non-deleted alias entries from KV.
func (r *KVAliasRegistry) listAll(ctx context.Context) ([]AliasMapping, error) {
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("list alias keys: %w", err)
	}

	var result []AliasMapping
	for _, key := range keys {
		entry, err := r.kv.Get(ctx, key)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				continue
			}
			return nil, fmt.Errorf("get alias %s: %w", key, err)
		}
		var m AliasMapping
		if err := json.Unmarshal(entry.Value(), &m); err != nil {
			continue // skip corrupted entries
		}
		result = append(result, m)
	}
	return result, nil
}
