package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/taxonomy"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const bucketName = "agent-registry"
const catalogBucketName = "capability-catalog"

// KVRegistry implements AgentRegistry backed by JetStream KV.
type KVRegistry struct {
	kv        jetstream.KeyValue
	catalogKV jetstream.KeyValue // non-nil when taxonomy catalog is active
	catalog   taxonomy.Catalog
	// Thresholds for state transitions.
	UnhealthyThreshold int32
	DeadThreshold      int32
}

// NewKVRegistry creates a KVRegistry, creating the KV bucket if needed.
// No capability taxonomy validation is applied.
func NewKVRegistry(ctx context.Context, nc *nats.Conn, unhealthyThreshold, deadThreshold int32) (*KVRegistry, error) {
	return newKVRegistry(ctx, nc, unhealthyThreshold, deadThreshold, nil)
}

// NewKVRegistryWithCatalog creates a KVRegistry backed by the given taxonomy catalog.
// Capabilities are validated and auto-mapped at registration time.
func NewKVRegistryWithCatalog(ctx context.Context, nc *nats.Conn, unhealthyThreshold, deadThreshold int32, cat taxonomy.Catalog) (*KVRegistry, error) {
	return newKVRegistry(ctx, nc, unhealthyThreshold, deadThreshold, cat)
}

func newKVRegistry(ctx context.Context, nc *nats.Conn, unhealthyThreshold, deadThreshold int32, cat taxonomy.Catalog) (*KVRegistry, error) {
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

	r := &KVRegistry{
		kv:                 kv,
		catalog:            cat,
		UnhealthyThreshold: unhealthyThreshold,
		DeadThreshold:      deadThreshold,
	}

	// Seed capability-catalog KV bucket with standard entries on first run.
	if cat != nil {
		if err := r.seedCatalog(ctx, js, cat); err != nil {
			return nil, fmt.Errorf("seed capability catalog: %w", err)
		}
	}

	return r, nil
}

// seedCatalog creates the capability-catalog KV bucket, seeds it with standard entries,
// and stores the KV reference in r.catalogKV for later vendor capability writes.
func (r *KVRegistry) seedCatalog(ctx context.Context, js jetstream.JetStream, cat taxonomy.Catalog) error {
	catalogKV, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  catalogBucketName,
		History: 1,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("create catalog KV bucket: %w", err)
	}
	r.catalogKV = catalogKV

	for _, entry := range cat.Query("*.*") {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal catalog entry %s: %w", entry.Name, err)
		}
		// Create only if not already present (idempotent seeding).
		_, err = catalogKV.Create(ctx, entry.Name, data)
		if err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
			return fmt.Errorf("seed catalog entry %s: %w", entry.Name, err)
		}
	}
	return nil
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

	// Validate and auto-map capabilities against the taxonomy catalog.
	if r.catalog != nil {
		for i, cap := range reg.Capabilities {
			if err := r.catalog.Validate(cap.Name); err != nil {
				return 0, fmt.Errorf("%w: %v", ErrValidation, err)
			}

			isVendor := r.catalog.Lookup(cap.Name) == nil
			if isVendor {
				// Vendor capabilities require explicit input_types and output_types (schema required).
				if len(cap.InputTypes) == 0 || len(cap.OutputTypes) == 0 {
					return 0, fmt.Errorf("%w: vendor capability %q requires input_types and output_types to be specified", ErrValidation, cap.Name)
				}
			}

			// Auto-map category/action/types from catalog for standard capabilities.
			tc := taxonomy.Capability{
				Name:        cap.Name,
				Version:     cap.Version,
				Description: cap.Description,
				Parameters:  cap.Parameters,
				Category:    cap.Category,
				Action:      cap.Action,
				InputTypes:  cap.InputTypes,
				OutputTypes: cap.OutputTypes,
			}
			if err := r.catalog.AutoMap(&tc); err != nil {
				return 0, fmt.Errorf("auto-map capability %s: %w", cap.Name, err)
			}
			reg.Capabilities[i] = Capability{
				Name:        tc.Name,
				Version:     tc.Version,
				Description: tc.Description,
				Parameters:  tc.Parameters,
				Category:    tc.Category,
				Action:      tc.Action,
				InputTypes:  tc.InputTypes,
				OutputTypes: tc.OutputTypes,
			}

			// Store vendor capabilities in capability-catalog KV for discoverability.
			if isVendor && r.catalogKV != nil {
				r.storeVendorCapability(ctx, reg.Capabilities[i])
			}
		}
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

func (r *KVRegistry) RegisterRemote(ctx context.Context, reg AgentRegistration) error {
	if reg.AgentID == "" {
		return fmt.Errorf("%w: agent_id is required", ErrValidation)
	}
	if reg.NodeID == "" {
		return fmt.Errorf("%w: node_id is required for remote registration", ErrValidation)
	}

	reg.Origin = "remote"
	reg.Status = StatusActive
	reg.LastSeen = time.Now()
	reg.MissedHeartbeats = 0

	key := "remote." + reg.NodeID + "." + reg.AgentID

	data, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("marshal remote registration: %w", err)
	}

	_, err = r.kv.Create(ctx, key, data)
	if err == nil {
		return nil
	}

	entry, err := r.kv.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get for remote re-register: %w", err)
	}

	_, err = r.kv.Update(ctx, key, data, entry.Revision())
	if err != nil {
		return fmt.Errorf("update remote registration: %w", err)
	}
	return nil
}

func (r *KVRegistry) DeregisterRemotesByNode(ctx context.Context, nodeID string) error {
	keys, err := r.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return fmt.Errorf("list keys for remote deregister: %w", err)
	}

	prefix := "remote." + nodeID + "."
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if err := r.kv.Purge(ctx, key); err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return fmt.Errorf("purge remote key %s: %w", key, err)
		}
	}
	return nil
}

func (r *KVRegistry) FindByCapability(ctx context.Context, capabilityName string, versionConstraint string) ([]AgentRegistration, error) {
	all, err := r.List(ctx)
	if err != nil {
		return nil, err
	}

	// Determine if the query is a glob pattern (contains "*" or "?").
	isGlob := strings.ContainsAny(capabilityName, "*?")

	var matched []AgentRegistration
	for _, agent := range all {
		if agent.Status != StatusActive {
			continue
		}
		for _, cap := range agent.Capabilities {
			if !capabilityMatches(capabilityName, cap, isGlob) {
				continue
			}
			if versionConstraint != "" {
				// Semver prefix matching: constraint "1." matches version "1.0.0", "1.2.3", etc.
				prefix := strings.TrimSuffix(versionConstraint, "x")
				prefix = strings.TrimSuffix(prefix, "X")
				if !strings.HasPrefix(cap.Version, prefix) {
					continue
				}
			}
			matched = append(matched, agent)
			break
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].AgentID < matched[j].AgentID
	})

	return matched, nil
}

// capabilityMatches returns true if the capability satisfies the query.
// For glob patterns (containing "*" or "?"), uses path-style matching on the capability name.
// "nlp.*" matches any capability where Category == "nlp".
// For exact queries, matches the full capability name.
func capabilityMatches(query string, cap Capability, isGlob bool) bool {
	if !isGlob {
		return cap.Name == query
	}
	// Use path.Match semantics: "nlp.*" matches "nlp.summarization", "nlp.translation", etc.
	// Also support category-level glob: if Category is set, match against it.
	matched, err := pathMatch(query, cap.Name)
	if err != nil {
		return false
	}
	return matched
}

// pathMatch wraps the standard library glob matching.
func pathMatch(pattern, name string) (bool, error) {
	// path.Match from standard library — does not traverse directory separators,
	// but "." is not special, so "nlp.*" matches "nlp.summarization".
	return matchGlob(pattern, name)
}

// matchGlob performs simple glob matching: * matches any sequence of non-separator chars.
// We use '.' as the separator conceptually, but for matching we treat the full name as a path.
func matchGlob(pattern, name string) (bool, error) {
	// Use strings-based approach for dot-notation glob.
	// "nlp.*" → prefix "nlp." must match start, then anything.
	if !strings.ContainsAny(pattern, "*?") {
		return pattern == name, nil
	}
	return globMatch(pattern, name), nil
}

// storeVendorCapability persists a vendor capability entry in the capability-catalog KV bucket.
// Uses Put (upsert) so re-registrations with updated types are reflected.
func (r *KVRegistry) storeVendorCapability(ctx context.Context, cap Capability) {
	entry := taxonomy.TaxonomyEntry{
		Name:           cap.Name,
		Description:    cap.Description,
		InputTypes:     append([]string(nil), cap.InputTypes...),
		OutputTypes:    append([]string(nil), cap.OutputTypes...),
		CatalogVersion: "vendor",
	}
	// Derive category and action from dot-notation name for discoverability.
	if idx := strings.Index(cap.Name, "."); idx >= 0 {
		entry.Category = cap.Name[:idx]
		entry.Action = cap.Name[idx+1:]
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return // best-effort; registration itself has already succeeded
	}
	_, _ = r.catalogKV.Put(ctx, cap.Name, data)
}

// globMatch implements simple glob: * matches any sequence (including dots), ? matches one char.
func globMatch(pattern, s string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Skip consecutive stars.
			for len(pattern) > 0 && pattern[0] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 0 {
				return true
			}
			// Try matching the rest of the pattern at each position in s.
			for i := 0; i <= len(s); i++ {
				if globMatch(pattern, s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			pattern = pattern[1:]
			s = s[1:]
		default:
			if len(s) == 0 || pattern[0] != s[0] {
				return false
			}
			pattern = pattern[1:]
			s = s[1:]
		}
	}
	return len(s) == 0
}
