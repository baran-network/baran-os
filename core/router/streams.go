package router

import (
	"strings"
	"sync"
	"time"
)

// StreamConfig holds the configuration for a JetStream stream.
type StreamConfig struct {
	Name     string
	Subjects []string
	MaxAge   time.Duration
}

// StreamRegistry maps event types to their target streams.
// It is the single source of truth for stream-to-event-type mapping.
// Static streams are immutable; dynamic streams can be registered/unregistered at runtime.
type StreamRegistry struct {
	streams []StreamConfig
	dynamic map[string]StreamConfig
	mu      sync.RWMutex
}

// NewStreamRegistry creates a StreamRegistry with the given stream configurations.
func NewStreamRegistry(configs ...StreamConfig) *StreamRegistry {
	return &StreamRegistry{
		streams: configs,
		dynamic: make(map[string]StreamConfig),
	}
}

// DefaultStreamRegistry returns a StreamRegistry with the system default streams.
func DefaultStreamRegistry() *StreamRegistry {
	return NewStreamRegistry(
		StreamConfig{
			Name: "AGENTS",
			Subjects: []string{
				"agent.register",
				"agent.unregister",
				"agent.error",
				// Workflow start is broadcast (no workflow_id yet — engine generates it).
				"workflow.start",
				// Workflow state queries are global (not per-workflow).
				"workflow.state.request",
				"workflow.state.response",
			},
			MaxAge: 24 * time.Hour,
		},
		StreamConfig{
			Name:     "HEALTH",
			Subjects: []string{"agent.health.ping", "agent.health.pong"},
			MaxAge:   1 * time.Hour,
		},
		StreamConfig{
			Name:     "DIRECT",
			Subjects: []string{"agent.direct.>"},
			MaxAge:   24 * time.Hour,
		},
		StreamConfig{
			Name:     "DISCOVERY",
			Subjects: []string{"agent.capability.announce", "agent.discovery.request", "agent.discovery.response"},
			MaxAge:   24 * time.Hour,
		},
		StreamConfig{
			Name:     "HUMAN",
			Subjects: []string{"human.decision.request", "human.decision.response"},
			MaxAge:   24 * time.Hour,
		},
		StreamConfig{
			Name:     "COORDINATION",
			Subjects: []string{"decision.conflict", "decision.resolved"},
			MaxAge:   24 * time.Hour,
		},
		StreamConfig{
			Name:     "FEDERATION",
			Subjects: []string{"federation.>"},
			MaxAge:   24 * time.Hour,
		},
		StreamConfig{
			Name:     "SIMULATION",
			Subjects: []string{"simulation.>"},
			MaxAge:   24 * time.Hour,
		},
	)
}

// RegisterDynamic adds a dynamic stream entry (e.g., per-workflow streams).
func (r *StreamRegistry) RegisterDynamic(name string, subjects []string) {
	r.mu.Lock()
	r.dynamic[name] = StreamConfig{Name: name, Subjects: subjects}
	r.mu.Unlock()
}

// UnregisterDynamic removes a dynamic stream entry.
func (r *StreamRegistry) UnregisterDynamic(name string) {
	r.mu.Lock()
	delete(r.dynamic, name)
	r.mu.Unlock()
}

// StreamForEventType returns the stream name for a given event type by matching
// against configured subjects. Supports exact match, ">" suffix, and "*" token wildcards.
// Checks static streams first, then dynamic entries.
func (r *StreamRegistry) StreamForEventType(eventType string) (string, bool) {
	for _, cfg := range r.streams {
		for _, subj := range cfg.Subjects {
			if matchSubject(subj, eventType) {
				return cfg.Name, true
			}
		}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, cfg := range r.dynamic {
		for _, subj := range cfg.Subjects {
			if matchSubject(subj, eventType) {
				return cfg.Name, true
			}
		}
	}

	return "", false
}

// Configs returns all stream configurations.
func (r *StreamRegistry) Configs() []StreamConfig {
	return r.streams
}

// matchSubject checks if an event type matches a NATS subject pattern.
// Supports exact match, ">" wildcard (any suffix), and "*" wildcard (one token).
func matchSubject(pattern, eventType string) bool {
	if pattern == eventType {
		return true
	}
	// Handle ">" wildcard: "agent.direct.>" matches "agent.direct.foo.bar"
	if len(pattern) > 1 && pattern[len(pattern)-1] == '>' {
		prefix := pattern[:len(pattern)-1] // includes trailing dot
		if len(eventType) >= len(prefix) && eventType[:len(prefix)] == prefix {
			return true
		}
	}
	// Handle "*" wildcard: each "*" matches exactly one dot-delimited token.
	return matchTokens(strings.Split(pattern, "."), strings.Split(eventType, "."))
}

// matchTokens returns true if all tokens match, treating "*" as a single-token wildcard.
func matchTokens(pattern, subject []string) bool {
	if len(pattern) != len(subject) {
		return false
	}
	for i, p := range pattern {
		if p != "*" && p != subject[i] {
			return false
		}
	}
	return true
}
