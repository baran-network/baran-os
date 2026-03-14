package router

import (
	"context"
	"fmt"
	"time"

	"github.com/carlosmolina/agent-os/core/eventbus"
	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/carlosmolina/agent-os/protocol/gen/go/agentosprotocol/v1"
)

// EventRouter is the single entry point for all event routing in the runtime.
type EventRouter interface {
	// Route dispatches an event based on its envelope fields.
	Route(ctx context.Context, event *eventbus.Event) error

	// Subscribe registers a handler for broadcast events of the given type.
	Subscribe(ctx context.Context, eventType string, handler eventbus.EventHandler) (eventbus.Subscription, error)

	// SubscribeDirect registers a handler for events addressed directly to the given agent.
	SubscribeDirect(ctx context.Context, agentID string, handler eventbus.EventHandler) (eventbus.Subscription, error)

	// Close releases all resources held by the router.
	Close() error
}

// DefaultRouter implements EventRouter by composing EventBus, AgentRegistry,
// and StreamRegistry.
type DefaultRouter struct {
	bus      eventbus.EventBus
	registry registry.AgentRegistry
	streams  *StreamRegistry
}

// NewDefaultRouter creates a DefaultRouter.
func NewDefaultRouter(bus eventbus.EventBus, reg registry.AgentRegistry, streams *StreamRegistry) *DefaultRouter {
	return &DefaultRouter{
		bus:      bus,
		registry: reg,
		streams:  streams,
	}
}

// Route dispatches an event based on its envelope fields using the resolved strategy.
func (r *DefaultRouter) Route(ctx context.Context, event *eventbus.Event) error {
	strategy := ResolveStrategy(event)

	switch strategy {
	case StrategyDirect:
		return r.routeDirect(ctx, event)
	case StrategyWorkflow:
		return r.routeWorkflow(ctx, event)
	case StrategyCapability:
		return r.routeCapability(ctx, event)
	case StrategyBroadcast:
		return r.routeBroadcast(ctx, event)
	default:
		return fmt.Errorf("routing strategy %s not yet implemented", strategy)
	}
}

// Subscribe registers a handler for broadcast events of the given type.
func (r *DefaultRouter) Subscribe(ctx context.Context, eventType string, handler eventbus.EventHandler) (eventbus.Subscription, error) {
	return r.bus.Subscribe(ctx, eventType, handler)
}

// SubscribeDirect subscribes to events addressed directly to the given agent.
// It listens on the subject pattern agent.direct.{agentID}.> on the DIRECT stream.
func (r *DefaultRouter) SubscribeDirect(ctx context.Context, agentID string, handler eventbus.EventHandler) (eventbus.Subscription, error) {
	subject := fmt.Sprintf("agent.direct.%s.>", agentID)
	return r.bus.Subscribe(ctx, subject, handler)
}

// Close releases all resources held by the router.
func (r *DefaultRouter) Close() error {
	return r.bus.Close()
}

// routeDirect validates the target agent exists and publishes to its direct subject.
func (r *DefaultRouter) routeDirect(ctx context.Context, event *eventbus.Event) error {
	_, _, err := r.registry.Get(ctx, event.TargetAgent)
	if err != nil {
		return r.publishError(ctx, event.SourceAgent, "ROUTER_TARGET_NOT_FOUND",
			fmt.Sprintf("target agent %q not found in registry", event.TargetAgent))
	}

	directEvent := *event
	directEvent.Type = fmt.Sprintf("agent.direct.%s.%s", event.TargetAgent, event.Type)

	return r.bus.Publish(ctx, &directEvent)
}

// routeWorkflow creates a per-workflow stream on-demand and publishes to it.
func (r *DefaultRouter) routeWorkflow(ctx context.Context, event *eventbus.Event) error {
	streamName := fmt.Sprintf("WF-%s", event.WorkflowID)
	subject := fmt.Sprintf("workflow.%s.%s", event.WorkflowID, event.Type)

	// Create the workflow stream on-demand if the bus supports it.
	if creator, ok := r.bus.(eventbus.StreamCreator); ok {
		if err := creator.EnsureStream(ctx, streamName, []string{fmt.Sprintf("workflow.%s.>", event.WorkflowID)}); err != nil {
			return fmt.Errorf("ensure workflow stream: %w", err)
		}
	}

	wfEvent := *event
	wfEvent.Type = subject

	return r.bus.Publish(ctx, &wfEvent)
}

// routeCapability queries the registry for agents matching the requested capability
// and fans out to each via routeDirect.
func (r *DefaultRouter) routeCapability(ctx context.Context, event *eventbus.Event) error {
	capability := event.Metadata["route.capability"]

	agents, err := r.registry.List(ctx)
	if err != nil {
		return fmt.Errorf("list agents for capability routing: %w", err)
	}

	var matched []string
	for _, agent := range agents {
		if agent.Status != registry.StatusActive {
			continue
		}
		for _, cap := range agent.Capabilities {
			if cap.Name == capability {
				matched = append(matched, agent.AgentID)
				break
			}
		}
	}

	if len(matched) == 0 {
		return r.publishError(ctx, event.SourceAgent, "ROUTER_NO_CAPABILITY_MATCH",
			fmt.Sprintf("no agents found with capability %q", capability))
	}

	for _, agentID := range matched {
		directEvent := *event
		directEvent.ID = uuid.Must(uuid.NewV7()).String() // unique ID per fan-out to avoid dedup
		directEvent.TargetAgent = agentID
		// Remove route.capability to avoid infinite recursion through resolveStrategy.
		meta := make(map[string]string, len(event.Metadata))
		for k, v := range event.Metadata {
			if k != "route.capability" {
				meta[k] = v
			}
		}
		directEvent.Metadata = meta
		if err := r.routeDirect(ctx, &directEvent); err != nil {
			return err
		}
	}

	return nil
}

// routeBroadcast resolves the event type to a stream and publishes via EventBus.
func (r *DefaultRouter) routeBroadcast(ctx context.Context, event *eventbus.Event) error {
	_, ok := r.streams.StreamForEventType(event.Type)
	if !ok {
		return r.publishError(ctx, event.SourceAgent, "ROUTER_UNMAPPED_EVENT_TYPE",
			fmt.Sprintf("no stream mapped for event type %q", event.Type))
	}

	return r.bus.Publish(ctx, event)
}

// publishError emits an agent.error event with the given code and message.
func (r *DefaultRouter) publishError(ctx context.Context, agentID, code, message string) error {
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:   agentID,
		ErrorCode: code,
		Message:   message,
	}
	data, err := proto.Marshal(errPayload)
	if err != nil {
		return fmt.Errorf("marshal error payload: %w", err)
	}

	return r.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  "runtime",
		SourceAgent: "router",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
}
