package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/router"
	"github.com/nats-io/nats.go/jetstream"
)

// streamForEventType returns the stream name for a given event type by querying
// the provided registry. Dynamic workflow streams are registered there by WorkflowStreamManager.
func streamForEventType(reg *router.StreamRegistry, eventType string) (string, error) {
	name, ok := reg.StreamForEventType(eventType)
	if ok {
		return name, nil
	}
	return "", fmt.Errorf("no stream mapped for event type %q", eventType)
}

// ensureStreams creates all system streams if they don't already exist.
func ensureStreams(ctx context.Context, js jetstream.JetStream, reg *router.StreamRegistry) error {
	for _, cfg := range reg.Configs() {
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
