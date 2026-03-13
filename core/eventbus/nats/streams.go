package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// streamConfig holds the configuration for a JetStream stream.
type streamConfig struct {
	Name     string
	Subjects []string
	MaxAge   time.Duration
}

// streamConfigs defines the system streams and their configurations.
var streamConfigs = []streamConfig{
	{
		Name:     "AGENTS",
		Subjects: []string{"agent.register", "agent.unregister", "agent.error"},
		MaxAge:   24 * time.Hour,
	},
	{
		Name:     "HEALTH",
		Subjects: []string{"agent.health.ping", "agent.health.pong"},
		MaxAge:   1 * time.Hour,
	},
}

// streamForEventType returns the stream name for a given event type.
func streamForEventType(eventType string) (string, error) {
	// Check health subjects first (more specific prefix).
	if strings.HasPrefix(eventType, "agent.health.") {
		return "HEALTH", nil
	}
	// Check agent-level subjects.
	for _, subj := range streamConfigs[0].Subjects {
		if eventType == subj {
			return "AGENTS", nil
		}
	}
	return "", fmt.Errorf("no stream mapped for event type %q", eventType)
}

// ensureStreams creates all system streams if they don't already exist.
func ensureStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range streamConfigs {
		_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:       cfg.Name,
			Subjects:   cfg.Subjects,
			Retention:  jetstream.LimitsPolicy,
			Storage:    jetstream.FileStorage,
			Discard:    jetstream.DiscardOld,
			MaxAge:     cfg.MaxAge,
			Duplicates: 2 * time.Minute,
		})
		if err != nil {
			return fmt.Errorf("create stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}
