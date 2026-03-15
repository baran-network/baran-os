package sdk

import (
	"errors"
	"log/slog"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
)

const (
	defaultNATSURL             = "nats://localhost:4222"
	defaultShutdownTimeout     = 10 * time.Second
	defaultIdempotencyCacheSize = 10000
)

// options holds resolved configuration for an Agent.
type options struct {
	natsURL              string
	eventBus             eventbus.EventBus
	logger               *slog.Logger
	labels               map[string]string
	shutdownTimeout      time.Duration
	idempotencyCacheSize int
}

func defaultOptions() options {
	return options{
		natsURL:              defaultNATSURL,
		shutdownTimeout:      defaultShutdownTimeout,
		idempotencyCacheSize: defaultIdempotencyCacheSize,
	}
}

// Option is a functional option for configuring an Agent.
type Option func(*options)

// WithNATSURL sets the NATS server URL. Ignored if WithEventBus is also provided.
func WithNATSURL(url string) Option {
	return func(o *options) {
		if url != "" {
			o.natsURL = url
		}
	}
}

// WithEventBus provides a custom EventBus, bypassing NATS URL configuration entirely.
func WithEventBus(bus eventbus.EventBus) Option {
	return func(o *options) {
		o.eventBus = bus
	}
}

// WithLogger sets a custom structured logger for all SDK subsystems.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// WithLabels sets key-value metadata included in the agent registration payload.
func WithLabels(labels map[string]string) Option {
	return func(o *options) {
		o.labels = labels
	}
}

// WithShutdownTimeout sets the maximum wait time for in-flight handlers during Stop.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.shutdownTimeout = d
		}
	}
}

// WithIdempotencyCacheSize sets the LRU cache size for event deduplication.
func WithIdempotencyCacheSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.idempotencyCacheSize = n
		}
	}
}

// validateOptions returns an error if any option value is invalid.
func validateOptions(o *options) error {
	if o.natsURL == "" {
		return errors.New("NATS URL must not be empty")
	}
	if o.shutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be positive")
	}
	if o.idempotencyCacheSize <= 0 {
		return errors.New("idempotency cache size must be positive")
	}
	return nil
}
