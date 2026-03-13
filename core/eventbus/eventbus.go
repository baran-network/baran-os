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

// EventBus is the transport abstraction for publishing and subscribing to events.
// Implementations MUST NOT leak transport-specific types through this interface.
type EventBus interface {
	// Publish persists an event to the bus. The event is durable before the call returns.
	Publish(ctx context.Context, event *Event) error

	// Subscribe registers a handler for events matching the given type pattern.
	// Supports exact match ("agent.register") and wildcard ("agent.>").
	Subscribe(ctx context.Context, eventType string, handler EventHandler) (Subscription, error)

	// Close drains all subscriptions and releases resources.
	Close() error
}
