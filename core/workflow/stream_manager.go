package workflow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/router"
)

// streamInfo holds metadata about an active workflow stream.
type streamInfo struct {
	name      string
	subjects  []string
	createdAt time.Time
}

// WorkflowStreamManager is the single owner of per-workflow stream lifecycle.
// It manages creation, lookup, and cleanup of WF-{id} streams.
type WorkflowStreamManager struct {
	creator  eventbus.StreamCreator
	registry *router.StreamRegistry
	mu       sync.RWMutex
	active   map[string]streamInfo
}

// NewWorkflowStreamManager creates a WorkflowStreamManager.
// The creator is used for underlying JetStream operations.
// The registry is updated on create/cleanup for dynamic stream discovery.
func NewWorkflowStreamManager(creator eventbus.StreamCreator, registry *router.StreamRegistry) *WorkflowStreamManager {
	return &WorkflowStreamManager{
		creator:  creator,
		registry: registry,
		active:   make(map[string]streamInfo),
	}
}

// GetOrCreateStream ensures the per-workflow stream exists and returns its name.
// Idempotent: if the stream is already registered, returns immediately.
func (m *WorkflowStreamManager) GetOrCreateStream(ctx context.Context, workflowID string) (string, error) {
	streamName := fmt.Sprintf("WF-%s", workflowID)
	subjects := []string{fmt.Sprintf("workflow.%s.>", workflowID)}

	// Fast path: already tracked.
	m.mu.RLock()
	if _, ok := m.active[workflowID]; ok {
		m.mu.RUnlock()
		return streamName, nil
	}
	m.mu.RUnlock()

	// Create the JetStream stream (idempotent at NATS level).
	if err := m.creator.EnsureStream(ctx, streamName, subjects); err != nil {
		return "", fmt.Errorf("ensure workflow stream %s: %w", streamName, err)
	}

	// Register in local tracking and dynamic registry.
	m.mu.Lock()
	m.active[workflowID] = streamInfo{
		name:      streamName,
		subjects:  subjects,
		createdAt: time.Now(),
	}
	m.mu.Unlock()

	m.registry.RegisterDynamic(streamName, subjects)

	return streamName, nil
}

// Lookup returns the stream name for a workflow if it is tracked.
func (m *WorkflowStreamManager) Lookup(workflowID string) (string, bool) {
	m.mu.RLock()
	info, ok := m.active[workflowID]
	m.mu.RUnlock()
	if !ok {
		return "", false
	}
	return info.name, true
}

// Cleanup removes the workflow from the in-memory registry.
// The JetStream stream is NOT deleted — MaxAge TTL handles eventual cleanup.
func (m *WorkflowStreamManager) Cleanup(workflowID string) {
	streamName := fmt.Sprintf("WF-%s", workflowID)

	m.mu.Lock()
	delete(m.active, workflowID)
	m.mu.Unlock()

	m.registry.UnregisterDynamic(streamName)
}

// ListActive returns all currently tracked workflow IDs.
func (m *WorkflowStreamManager) ListActive() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	return ids
}
