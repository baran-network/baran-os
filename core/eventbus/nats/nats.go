package nats

import (
	"context"
	"fmt"
	"sync"

	"github.com/carlosmolina/agent-os/core/eventbus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// Bus implements eventbus.EventBus using NATS JetStream.
type Bus struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	mu   sync.Mutex
	subs []eventbus.Subscription
}

// New creates a new NATS-backed EventBus. It connects to the given URL,
// initializes JetStream, and ensures all system streams exist.
func New(ctx context.Context, url string) (*Bus, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("nats connect: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	if err := ensureStreams(ctx, js); err != nil {
		nc.Close()
		return nil, fmt.Errorf("ensure streams: %w", err)
	}

	return &Bus{nc: nc, js: js}, nil
}

// NewFromConn creates a Bus from an existing NATS connection (useful for tests).
func NewFromConn(ctx context.Context, nc *nats.Conn) (*Bus, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream init: %w", err)
	}

	if err := ensureStreams(ctx, js); err != nil {
		return nil, fmt.Errorf("ensure streams: %w", err)
	}

	return &Bus{nc: nc, js: js}, nil
}

// Publish serializes an Event to a protobuf AgentEvent and publishes it to the
// correct JetStream stream. The event ID is set as Nats-Msg-Id for deduplication.
func (b *Bus) Publish(ctx context.Context, event *eventbus.Event) error {
	stream, err := streamForEventType(event.Type)
	if err != nil {
		return err
	}

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
		return fmt.Errorf("publish to stream %s: %w", stream, err)
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
	stream, err := streamForEventType(eventType)
	if err != nil {
		return nil, err
	}

	// Use the event type as the durable consumer name (dots replaced with dashes).
	consumerName := sanitizeConsumerName(eventType)

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
