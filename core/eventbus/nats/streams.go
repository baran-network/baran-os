package nats

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ad-hok/agent-os/core/router"
	"github.com/nats-io/nats.go/jetstream"
)

// registry is the shared StreamRegistry used for event type → stream resolution.
var registry = router.DefaultStreamRegistry()

// streamForEventType returns the stream name for a given event type.
// It checks the static registry first, then handles dynamic workflow streams.
func streamForEventType(eventType string) (string, error) {
	name, ok := registry.StreamForEventType(eventType)
	if ok {
		return name, nil
	}

	// Handle dynamic workflow subjects: workflow.{workflow_id}.{event_type}
	if len(eventType) > 9 && eventType[:9] == "workflow." {
		// Extract workflow ID from "workflow.{id}.{rest}"
		rest := eventType[9:]
		dot := strings.Index(rest, ".")
		if dot > 0 {
			wfID := rest[:dot]
			return fmt.Sprintf("WF-%s", wfID), nil
		}
	}

	return "", fmt.Errorf("no stream mapped for event type %q", eventType)
}

// ensureStreams creates all system streams if they don't already exist.
func ensureStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range registry.Configs() {
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
