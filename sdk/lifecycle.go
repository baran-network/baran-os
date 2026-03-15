package sdk

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"github.com/google/uuid"
)

// register publishes an agent.register event with the agent's metadata and
// all registered capabilities. Called during Start.
func (a *Agent) register(ctx context.Context) error {
	a.mu.RLock()
	caps := make([]*protocolv1.Capability, 0, len(a.capabilities))
	for _, entry := range a.capabilities {
		pb := &protocolv1.Capability{
			Name:        entry.cap.Name,
			Version:     entry.cap.Version,
			Description: entry.cap.Description,
			Parameters:  entry.cap.Parameters,
		}
		caps = append(caps, pb)
	}
	a.mu.RUnlock()

	payload := &protocolv1.AgentRegisterPayload{
		AgentId:      a.id,
		AgentType:    a.agentType,
		Version:      a.version,
		Capabilities: caps,
		Labels:       a.opts.labels,
	}

	data, err := marshalPayload("agent.register", payload)
	if err != nil {
		return fmt.Errorf("marshal register payload: %w", err)
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate event ID: %w", err)
	}

	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        "agent.register",
		SourceAgent: a.id,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}

	if err := a.bus.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish agent.register: %w", err)
	}

	a.logger.Info("agent registered", "capabilities", len(caps))
	return nil
}

// unregister publishes an agent.unregister event. Called during Stop.
func (a *Agent) unregister(ctx context.Context) error {
	payload := &protocolv1.AgentUnregisterPayload{
		AgentId: a.id,
		Reason:  "shutdown",
	}

	data, err := marshalPayload("agent.unregister", payload)
	if err != nil {
		return fmt.Errorf("marshal unregister payload: %w", err)
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("generate event ID: %w", err)
	}

	event := &eventbus.Event{
		ID:          eventID.String(),
		Type:        "agent.unregister",
		SourceAgent: a.id,
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}

	if err := a.bus.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish agent.unregister: %w", err)
	}

	a.logger.Info("agent unregistered")
	return nil
}
