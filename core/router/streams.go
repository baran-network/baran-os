package router

import (
	"strings"
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
type StreamRegistry struct {
	streams []StreamConfig
}

// NewStreamRegistry creates a StreamRegistry with the given stream configurations.
func NewStreamRegistry(configs ...StreamConfig) *StreamRegistry {
	return &StreamRegistry{streams: configs}
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
	)
}

// StreamForEventType returns the stream name for a given event type by matching
// against configured subjects. Supports exact match, ">" suffix, and "*" token wildcards.
func (r *StreamRegistry) StreamForEventType(eventType string) (string, bool) {
	for _, cfg := range r.streams {
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
