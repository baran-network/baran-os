package eventbus

import "context"

// Event represents a routable event in the system.
// This is a transport-agnostic representation — no NATS imports allowed here.
type Event struct {
	ID            string
	Type          string
	SourceNode    string
	SourceAgent   string
	TargetAgent   string
	WorkflowID    string
	CorrelationID string
	Timestamp     int64
	Metadata      map[string]string
	Payload       []byte
}

// EventHandler is a function that processes an incoming event.
type EventHandler func(ctx context.Context, event *Event) error

// Subscription represents an active event subscription that can be stopped.
type Subscription interface {
	Unsubscribe() error
}

// StreamCreator is an optional interface for EventBus implementations that
// support creating streams on-demand (e.g., per-workflow streams).
type StreamCreator interface {
	// EnsureStream creates a stream with the given name and subjects if it doesn't exist.
	EnsureStream(ctx context.Context, name string, subjects []string) error
}

// EventPublisher is a narrow interface for publishing events through the router.
// The workflow engine depends on this instead of importing the router package directly.
type EventPublisher interface {
	Route(ctx context.Context, event *Event) error
}

// EventBus is the transport abstraction for publishing and subscribing to events.
// Implementations MUST NOT leak transport-specific types through this interface.
type EventBus interface {
	// Publish persists an event to the bus. The event is durable before the call returns.
	Publish(ctx context.Context, event *Event) error

	// Subscribe registers a handler for events matching the given type pattern.
	// Supports exact match ("agent.register") and wildcard ("agent.>").
	Subscribe(ctx context.Context, eventType string, handler EventHandler) (Subscription, error)

	// SubscribeWithStream creates a consumer on a specific named stream with the given
	// filter subject. The stream is verified to exist before the consumer is created.
	// Use this instead of Subscribe when the target stream is known (e.g., per-workflow streams).
	SubscribeWithStream(ctx context.Context, streamName string, subject string, handler EventHandler) (Subscription, error)

	// Close drains all subscriptions and releases resources.
	Close() error
}
