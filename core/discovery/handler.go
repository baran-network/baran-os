package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// DiscoveryHandler subscribes to agent.discovery.request events and publishes
// agent.discovery.response events with matching agents.
type DiscoveryHandler struct {
	bus           eventbus.EventBus
	registry      registry.AgentRegistry
	nodeID        string
	aliasRegistry registry.AliasRegistry // optional; enables alias-based fallback discovery
}

// NewDiscoveryHandler creates a new DiscoveryHandler.
func NewDiscoveryHandler(bus eventbus.EventBus, reg registry.AgentRegistry, nodeID string) *DiscoveryHandler {
	return &DiscoveryHandler{
		bus:      bus,
		registry: reg,
		nodeID:   nodeID,
	}
}

// SetAliasRegistry injects an alias registry for fallback capability discovery.
// When set, queries with no direct matches will be resolved via aliases.
func (h *DiscoveryHandler) SetAliasRegistry(ar registry.AliasRegistry) {
	h.aliasRegistry = ar
}

// Start subscribes to agent.discovery.request events.
func (h *DiscoveryHandler) Start(ctx context.Context) ([]eventbus.Subscription, error) {
	sub, err := h.bus.Subscribe(ctx, "agent.discovery.request", h.handleDiscoveryRequest)
	if err != nil {
		return nil, fmt.Errorf("subscribe agent.discovery.request: %w", err)
	}
	return []eventbus.Subscription{sub}, nil
}

func (h *DiscoveryHandler) handleDiscoveryRequest(ctx context.Context, evt *eventbus.Event) error {
	var payload protocolv1.DiscoveryRequestPayload
	if err := proto.Unmarshal(evt.Payload, &payload); err != nil {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_PAYLOAD",
			fmt.Sprintf("failed to unmarshal discovery request: %v", err))
	}

	if payload.CapabilityName == "" {
		return h.publishError(ctx, evt.SourceAgent, "INVALID_DISCOVERY_REQUEST",
			"capability_name is required")
	}

	agents, err := h.registry.FindByCapability(ctx, payload.CapabilityName, payload.VersionConstraint)
	if err != nil {
		return h.publishError(ctx, evt.SourceAgent, "DISCOVERY_FAILED",
			fmt.Sprintf("registry query failed: %v", err))
	}

	// Alias fallback: when no direct match found and an alias registry is configured,
	// resolve equivalent capability names and search for agents with those names.
	if len(agents) == 0 && h.aliasRegistry != nil {
		equivalents, resolveErr := h.aliasRegistry.Resolve(ctx, payload.CapabilityName)
		if resolveErr == nil {
			seen := map[string]struct{}{payload.CapabilityName: {}}
			for _, equiv := range equivalents {
				if _, already := seen[equiv]; already {
					continue
				}
				seen[equiv] = struct{}{}
				aliasAgents, findErr := h.registry.FindByCapability(ctx, equiv, payload.VersionConstraint)
				if findErr == nil {
					agents = append(agents, aliasAgents...)
				}
			}
			// Deduplicate by AgentID.
			agents = deduplicateByAgentID(agents)
		}
	}

	matches := make([]*protocolv1.AgentCapabilityMatch, len(agents))
	for i, agent := range agents {
		caps := make([]*protocolv1.Capability, len(agent.Capabilities))
		for j, c := range agent.Capabilities {
			caps[j] = &protocolv1.Capability{
				Name:        c.Name,
				Version:     c.Version,
				Description: c.Description,
				Parameters:  c.Parameters,
			}
		}
		sourceNode := agent.NodeID
		if !agent.IsRemote() {
			sourceNode = h.nodeID
		}
		matches[i] = &protocolv1.AgentCapabilityMatch{
			AgentId:      agent.AgentID,
			AgentType:    agent.AgentType,
			Capabilities: caps,
			SourceNode:   sourceNode,
		}
	}

	responsePayload := &protocolv1.DiscoveryResponsePayload{
		Matches: matches,
	}
	data, err := proto.Marshal(responsePayload)
	if err != nil {
		return fmt.Errorf("marshal discovery response: %w", err)
	}

	return h.bus.Publish(ctx, &eventbus.Event{
		ID:            uuid.Must(uuid.NewV7()).String(),
		Type:          "agent.discovery.response",
		SourceNode:    h.nodeID,
		SourceAgent:   "runtime",
		CorrelationID: evt.CorrelationID,
		Timestamp:     time.Now().UnixNano(),
		Payload:       data,
	})
}

// deduplicateByAgentID removes duplicate agent registrations keeping the first occurrence.
func deduplicateByAgentID(agents []registry.AgentRegistration) []registry.AgentRegistration {
	seen := make(map[string]struct{}, len(agents))
	result := agents[:0]
	for _, a := range agents {
		if _, ok := seen[a.AgentID]; ok {
			continue
		}
		seen[a.AgentID] = struct{}{}
		result = append(result, a)
	}
	return result
}

func (h *DiscoveryHandler) publishError(ctx context.Context, agentID, code, message string) error {
	errPayload := &protocolv1.AgentErrorPayload{
		AgentId:   agentID,
		ErrorCode: code,
		Message:   message,
	}
	data, err := proto.Marshal(errPayload)
	if err != nil {
		return fmt.Errorf("marshal error payload: %w", err)
	}

	return h.bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.error",
		SourceNode:  h.nodeID,
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	})
}
