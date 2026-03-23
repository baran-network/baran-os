package simulation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/router"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// EventQuery holds parameters for querying historical events from the store.
type EventQuery struct {
	StartTime   time.Time
	EndTime     time.Time
	EventTypes  []string
	WorkflowID  string
	SourceAgent string
	Limit       int
	Offset      int
}

// StoredEvent is an event retrieved from the store, enriched with stream metadata.
type StoredEvent struct {
	Event    *eventbus.Event
	Stream   string
	Sequence uint64
}

// EventStore reads historical events from JetStream streams.
type EventStore interface {
	// Query returns events matching the given query parameters.
	Query(ctx context.Context, q EventQuery) ([]StoredEvent, error)
	// GetWorkflowEvents returns all events for a specific workflow from its WF-{id} stream.
	GetWorkflowEvents(ctx context.Context, workflowID string) ([]StoredEvent, error)
}

// JetStreamEventStore implements EventStore using JetStream ordered consumers.
type JetStreamEventStore struct {
	js       jetstream.JetStream
	registry *router.StreamRegistry
}

// NewJetStreamEventStore creates a JetStreamEventStore backed by the given JetStream handle.
func NewJetStreamEventStore(js jetstream.JetStream, registry *router.StreamRegistry) *JetStreamEventStore {
	return &JetStreamEventStore{js: js, registry: registry}
}

// Query returns events matching the given query parameters.
// It scans relevant JetStream streams using ordered consumers with DeliverByStartTime
// and applies client-side filtering by event type, workflow ID, and source agent.
func (s *JetStreamEventStore) Query(ctx context.Context, q EventQuery) ([]StoredEvent, error) {
	if q.EndTime.IsZero() {
		q.EndTime = time.Now()
	}
	if q.Limit <= 0 {
		q.Limit = 1000
	}

	// Determine which streams to scan.
	streams := s.resolveStreams(q)
	if len(streams) == 0 {
		return nil, nil
	}

	var all []StoredEvent
	for _, streamName := range streams {
		events, err := s.readFromStream(ctx, streamName, q.StartTime, q.EndTime)
		if err != nil {
			// Stream may not exist (e.g., no workflows yet); skip gracefully.
			if isStreamNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("read stream %s: %w", streamName, err)
		}
		all = append(all, events...)
	}

	// Client-side filtering.
	filtered := s.filterEvents(all, q)

	// Sort chronologically by timestamp.
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Event.Timestamp < filtered[j].Event.Timestamp
	})

	// Apply offset and limit.
	if q.Offset >= len(filtered) {
		return nil, nil
	}
	filtered = filtered[q.Offset:]
	if len(filtered) > q.Limit {
		filtered = filtered[:q.Limit]
	}

	return filtered, nil
}

// GetWorkflowEvents returns all events for a specific workflow from its WF-{id} stream.
func (s *JetStreamEventStore) GetWorkflowEvents(ctx context.Context, workflowID string) ([]StoredEvent, error) {
	streamName := "WF-" + workflowID

	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		if isStreamNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get stream %s: %w", streamName, err)
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverAllPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create ordered consumer on %s: %w", streamName, err)
	}

	return s.fetchAll(ctx, cons, streamName)
}

// resolveStreams determines which JetStream streams to scan based on the query.
func (s *JetStreamEventStore) resolveStreams(q EventQuery) []string {
	// If a workflow ID is specified without event types, read from the WF stream.
	if q.WorkflowID != "" && len(q.EventTypes) == 0 {
		return []string{"WF-" + q.WorkflowID}
	}

	// If event types are specified, resolve to their streams via the registry.
	if len(q.EventTypes) > 0 {
		seen := make(map[string]bool)
		var streams []string
		for _, et := range q.EventTypes {
			name, ok := s.registry.StreamForEventType(et)
			if ok && !seen[name] {
				seen[name] = true
				streams = append(streams, name)
			}
		}
		// Also include workflow stream if workflow ID is set.
		if q.WorkflowID != "" {
			wfStream := "WF-" + q.WorkflowID
			if !seen[wfStream] {
				streams = append(streams, wfStream)
			}
		}
		return streams
	}

	// No filters: scan all system streams.
	var streams []string
	for _, cfg := range s.registry.Configs() {
		streams = append(streams, cfg.Name)
	}
	return streams
}

// readFromStream reads events from a single stream within the given time range.
func (s *JetStreamEventStore) readFromStream(ctx context.Context, streamName string, start, end time.Time) ([]StoredEvent, error) {
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	cons, err := stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy,
		OptStartTime:  &start,
	})
	if err != nil {
		return nil, fmt.Errorf("create ordered consumer on %s: %w", streamName, err)
	}

	return s.fetchUntil(ctx, cons, streamName, end)
}

// fetchUntil reads messages from a consumer until the end time is reached or no more messages.
func (s *JetStreamEventStore) fetchUntil(ctx context.Context, cons jetstream.Consumer, streamName string, end time.Time) ([]StoredEvent, error) {
	var result []StoredEvent
	endNano := end.UnixNano()

	for {
		msgs, err := cons.Fetch(100, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				break
			}
			return result, nil
		}

		count := 0
		for msg := range msgs.Messages() {
			count++
			stored, err := decodeMessage(msg, streamName)
			if err != nil {
				continue
			}
			if stored.Event.Timestamp > endNano {
				return result, nil
			}
			result = append(result, stored)
		}

		if msgs.Error() != nil && !errors.Is(msgs.Error(), jetstream.ErrMsgIteratorClosed) {
			break
		}
		if count == 0 {
			break
		}
	}

	return result, nil
}

// fetchAll reads all messages from a consumer.
func (s *JetStreamEventStore) fetchAll(ctx context.Context, cons jetstream.Consumer, streamName string) ([]StoredEvent, error) {
	var result []StoredEvent

	for {
		msgs, err := cons.Fetch(100, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				break
			}
			return result, nil
		}

		count := 0
		for msg := range msgs.Messages() {
			count++
			stored, err := decodeMessage(msg, streamName)
			if err != nil {
				continue
			}
			result = append(result, stored)
		}

		if msgs.Error() != nil && !errors.Is(msgs.Error(), jetstream.ErrMsgIteratorClosed) {
			break
		}
		if count == 0 {
			break
		}
	}

	return result, nil
}

// filterEvents applies client-side filtering on event type, workflow ID, and source agent.
func (s *JetStreamEventStore) filterEvents(events []StoredEvent, q EventQuery) []StoredEvent {
	if len(q.EventTypes) == 0 && q.WorkflowID == "" && q.SourceAgent == "" {
		return events
	}

	typeSet := make(map[string]bool, len(q.EventTypes))
	for _, et := range q.EventTypes {
		typeSet[et] = true
	}

	var filtered []StoredEvent
	for _, e := range events {
		if len(typeSet) > 0 && !typeSet[e.Event.Type] {
			continue
		}
		if q.WorkflowID != "" && e.Event.WorkflowID != q.WorkflowID {
			continue
		}
		if q.SourceAgent != "" && e.Event.SourceAgent != q.SourceAgent {
			continue
		}
		filtered = append(filtered, e)
	}
	return filtered
}

// decodeMessage unmarshals a JetStream message into a StoredEvent.
func decodeMessage(msg jetstream.Msg, streamName string) (StoredEvent, error) {
	var pb protocolv1.AgentEvent
	if err := proto.Unmarshal(msg.Data(), &pb); err != nil {
		return StoredEvent{}, fmt.Errorf("unmarshal event: %w", err)
	}

	meta, _ := msg.Metadata()
	var seq uint64
	if meta != nil {
		seq = meta.Sequence.Stream
	}

	return StoredEvent{
		Event: &eventbus.Event{
			ID:            pb.Id,
			Type:          pb.Type,
			SourceNode:    pb.SourceNode,
			SourceAgent:   pb.SourceAgent,
			TargetAgent:   pb.TargetAgent,
			WorkflowID:    pb.WorkflowId,
			CorrelationID: pb.CorrelationId,
			Timestamp:     pb.Timestamp,
			Metadata:      pb.Metadata,
			Payload:       pb.Payload,
		},
		Stream:   streamName,
		Sequence: seq,
	}, nil
}

// isStreamNotFound checks if an error indicates a stream does not exist.
func isStreamNotFound(err error) bool {
	return errors.Is(err, jetstream.ErrStreamNotFound)
}
