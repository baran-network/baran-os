package nats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/router"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// consumerSeq generates unique consumer name suffixes to allow multiple
// subscribers to the same event type without sharing a single consumer.
var consumerSeq atomic.Uint64

// Bus implements eventbus.EventBus using NATS JetStream.
type Bus struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	streamRegistry *router.StreamRegistry
	mu             sync.Mutex
	subs           []eventbus.Subscription
}

// New creates a new NATS-backed EventBus. It connects to the given URL,
// initializes JetStream, and ensures all system streams exist.
// An optional StreamRegistry may be provided; if absent, DefaultStreamRegistry is used.
func New(ctx context.Context, url string, reg ...*router.StreamRegistry) (*Bus, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	streamReg := resolveRegistry(reg)
	if err := ensureStreams(ctx, js, streamReg); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure streams: %w", err)
	}

	return &Bus{nc: nc, js: js, streamRegistry: streamReg}, nil
}

// NewFromConn creates a Bus from an existing NATS connection (useful for tests).
// An optional StreamRegistry may be provided; if absent, DefaultStreamRegistry is used.
func NewFromConn(ctx context.Context, nc *nats.Conn, reg ...*router.StreamRegistry) (*Bus, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	streamReg := resolveRegistry(reg)
	if err := ensureStreams(ctx, js, streamReg); err != nil {
		return nil, fmt.Errorf("ensure streams: %w", err)
	}

	return &Bus{nc: nc, js: js, streamRegistry: streamReg}, nil
}

// resolveRegistry returns the provided registry or DefaultStreamRegistry if none given.
func resolveRegistry(reg []*router.StreamRegistry) *router.StreamRegistry {
	if len(reg) > 0 && reg[0] != nil {
		return reg[0]
	}
	return router.DefaultStreamRegistry()
}

// Publish serializes an Event to a protobuf AgentEvent and publishes it to the
// matching JetStream stream. The event ID is set as Nats-Msg-Id for deduplication.
// JetStream routes the message to the correct stream based on subject matching.
func (b *Bus) Publish(ctx context.Context, event *eventbus.Event) error {
	pbEvent := &protocolv1.AgentEvent{
		Id:            event.ID,
		Type:          event.Type,
		SourceNode:    event.SourceNode,
		SourceAgent:   event.SourceAgent,
		TargetAgent:   event.TargetAgent,
		WorkflowId:    event.WorkflowID,
		CorrelationId: event.CorrelationID,
		Timestamp:     event.Timestamp,
		Metadata:      event.Metadata,
		Payload:       event.Payload,
	}

	data, err := proto.Marshal(pbEvent)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := nats.NewMsg(event.Type)
	msg.Data = data
	msg.Header.Set("Nats-Msg-Id", event.ID)

	_, err = b.js.PublishMsg(ctx, msg)
	if err != nil {
		return fmt.Errorf("publish event %s: %w", event.Type, err)
	}

	return nil
}

// subscription wraps a JetStream consumer context for unsubscribing.
type subscription struct {
	cancel context.CancelFunc
}

func (s *subscription) Unsubscribe() error {
	s.cancel()
	return nil
}

// Subscribe creates a durable consumer on the stream that matches the given
// event type and calls handler for each received event.
func (b *Bus) Subscribe(ctx context.Context, eventType string, handler eventbus.EventHandler) (eventbus.Subscription, error) {
	stream, err := streamForEventType(b.streamRegistry, eventType)
	if err != nil {
		return nil, err
	}

	// Each subscription gets a unique consumer so multiple subscribers to the
	// same event type each receive every message (fan-out, not load-balanced).
	seq := consumerSeq.Add(1)
	consumerName := fmt.Sprintf("%s-%d", sanitizeConsumerName(eventType), seq)

	filterSubject := eventType

	cons, err := b.js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer %s: %w", consumerName, err)
	}

	subCtx, cancel := context.WithCancel(ctx)

	cctx, err := cons.Consume(func(msg jetstream.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data(), &pbEvent); err != nil {
			_ = msg.Nak()
			return
		}

		event := &eventbus.Event{
			ID:            pbEvent.Id,
			Type:          pbEvent.Type,
			SourceNode:    pbEvent.SourceNode,
			SourceAgent:   pbEvent.SourceAgent,
			TargetAgent:   pbEvent.TargetAgent,
			WorkflowID:    pbEvent.WorkflowId,
			CorrelationID: pbEvent.CorrelationId,
			Timestamp:     pbEvent.Timestamp,
			Metadata:      pbEvent.Metadata,
			Payload:       pbEvent.Payload,
		}

		if err := handler(subCtx, event); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("consume: %w", err)
	}

	// Wrap cancel to also stop the consume context.
	sub := &subscription{
		cancel: func() {
			cctx.Stop()
			cancel()
		},
	}

	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()

	return sub, nil
}

// SubscribeWithStream creates a consumer on a specific named stream with the given
// filter subject. Unlike Subscribe, it does not use streamForEventType() — the caller
// provides the stream name directly. If the stream does not exist, it is created
// with the filter subject, guaranteeing the consumer is created only after the stream is ready.
func (b *Bus) SubscribeWithStream(ctx context.Context, streamName string, subject string, handler eventbus.EventHandler) (eventbus.Subscription, error) {
	// Guarantee stream exists before creating the consumer.
	// Check first to avoid overwriting an existing stream's subject configuration.
	if _, err := b.js.Stream(ctx, streamName); err != nil {
		// Stream does not exist — create it scoped to the filter subject.
		if err := b.EnsureStream(ctx, streamName, []string{subject}); err != nil {
			return nil, fmt.Errorf("ensure stream %s: %w", streamName, err)
		}
	}

	seq := consumerSeq.Add(1)
	consumerName := fmt.Sprintf("%s-%d", sanitizeConsumerName(subject), seq)

	cons, err := b.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer on stream %s: %w", streamName, err)
	}

	subCtx, cancel := context.WithCancel(ctx)

	cctx, err := cons.Consume(func(msg jetstream.Msg) {
		var pbEvent protocolv1.AgentEvent
		if err := proto.Unmarshal(msg.Data(), &pbEvent); err != nil {
			_ = msg.Nak()
			return
		}

		event := &eventbus.Event{
			ID:            pbEvent.Id,
			Type:          pbEvent.Type,
			SourceNode:    pbEvent.SourceNode,
			SourceAgent:   pbEvent.SourceAgent,
			TargetAgent:   pbEvent.TargetAgent,
			WorkflowID:    pbEvent.WorkflowId,
			CorrelationID: pbEvent.CorrelationId,
			Timestamp:     pbEvent.Timestamp,
			Metadata:      pbEvent.Metadata,
			Payload:       pbEvent.Payload,
		}

		if err := handler(subCtx, event); err != nil {
			_ = msg.Nak()
			return
		}
		_ = msg.Ack()
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("consume on stream %s: %w", streamName, err)
	}

	sub := &subscription{
		cancel: func() {
			cctx.Stop()
			cancel()
		},
	}

	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()

	return sub, nil
}

// EnsureStream creates a stream with the given name and subjects if it doesn't exist.
func (b *Bus) EnsureStream(ctx context.Context, name string, subjects []string) error {
	_, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       name,
		Subjects:   subjects,
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage,
		Discard:    jetstream.DiscardOld,
		MaxAge:     24 * time.Hour,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		return fmt.Errorf("ensure stream %s: %w", name, err)
	}
	return nil
}

// JetStream returns the underlying JetStream handle.
// Used by components that need direct JetStream access (e.g., EventStore).
func (b *Bus) JetStream() jetstream.JetStream {
	return b.js
}

// Close drains and closes the NATS connection.
func (b *Bus) Close() error {
	b.mu.Lock()
	subs := b.subs
	b.subs = nil
	b.mu.Unlock()

	for _, sub := range subs {
		_ = sub.Unsubscribe()
	}

	b.nc.Close()
	return nil
}

func sanitizeConsumerName(s string) string {
	result := make([]byte, len(s))
	for i := range s {
		if s[i] == '.' || s[i] == '>' || s[i] == '*' {
			result[i] = '-'
		} else {
			result[i] = s[i]
		}
	}
	return string(result)
}
