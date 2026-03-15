package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
)

// subscribeHealth registers a listener for agent.health.ping events and
// automatically responds with agent.health.pong. Called during Start.
func (a *Agent) subscribeHealth(ctx context.Context) error {
	sub, err := a.bus.Subscribe(ctx, "agent.health.ping", func(ctx context.Context, event *eventbus.Event) error {
		ping, err := unmarshalHealthPingPayload(event.Payload)
		if err != nil {
			a.logger.Warn("malformed health ping", "error", err)
			return nil // ack without responding — don't crash the subscription
		}
		return a.publishHealthPong(ctx, ping.Sequence)
	})
	if err != nil {
		return fmt.Errorf("subscribe agent.health.ping: %w", err)
	}

	a.mu.Lock()
	a.subs = append(a.subs, sub)
	a.mu.Unlock()

	return nil
}

// publishHealthPong emits an agent.health.pong event in response to a ping.
func (a *Agent) publishHealthPong(ctx context.Context, sequence int64) error {
	payload := &protocolv1.HealthPongPayload{
		AgentId:  a.id,
		Sequence: sequence,
		Status:   protocolv1.AgentStatus_AGENT_STATUS_HEALTHY,
	}

	data, err := marshalPayload("agent.health.pong", payload)
	if err != nil {
		a.logger.Warn("failed to marshal health pong", "error", err)
		return nil
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return nil
	}

	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        "agent.health.pong",
		SourceAgent: a.id,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}

	if err := a.bus.Publish(ctx, event); err != nil {
		a.logger.Warn("failed to publish health pong", "error", err)
	}
	return nil
}
