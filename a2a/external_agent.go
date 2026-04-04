package a2a

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/taxonomy"
)

// ExternalAgentManager onboards external A2A agents as VirtualAgents
// in the Baran registry and monitors their health.
type ExternalAgentManager struct {
	reg    registry.AgentRegistry
	cat    taxonomy.Catalog
	client *http.Client
	logger *slog.Logger
}

// NewExternalAgentManager creates an ExternalAgentManager.
func NewExternalAgentManager(reg registry.AgentRegistry, cat taxonomy.Catalog, logger *slog.Logger) *ExternalAgentManager {
	return &ExternalAgentManager{
		reg:    reg,
		cat:    cat,
		client: &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}
}

// OnboardExternalAgent fetches the external agent's Agent Card, maps each skill
// to a Baran capability name, and registers a VirtualAgent with origin="a2a".
// The resolved agentID is returned so the caller can start health polling.
func (m *ExternalAgentManager) OnboardExternalAgent(ctx context.Context, cfg ExternalAgentConfig) (string, error) {
	card, err := m.fetchAgentCard(cfg.Endpoint)
	if err != nil {
		return "", fmt.Errorf("fetch agent card from %s: %w", cfg.Endpoint, err)
	}

	caps := make([]registry.Capability, 0, len(card.Skills))
	for _, skill := range card.Skills {
		capName := m.resolveCapabilityName(cfg, skill)
		if capName == "" {
			m.logger.Warn("skipping unmappable skill", "agent", cfg.Name, "skill", skill.ID)
			continue
		}

		inputTypes := skill.InputModes
		outputTypes := skill.OutputModes
		if len(inputTypes) == 0 {
			inputTypes = []string{"application/json"}
		}
		if len(outputTypes) == 0 {
			outputTypes = []string{"application/json"}
		}

		caps = append(caps, registry.Capability{
			Name:        capName,
			Version:     "1.0.0",
			Description: skill.Description,
			Parameters:  map[string]string{"a2a_endpoint": cfg.Endpoint},
			InputTypes:  inputTypes,
			OutputTypes: outputTypes,
		})
	}

	if len(caps) == 0 {
		return "", fmt.Errorf("no mappable capabilities found in agent card from %s", cfg.Endpoint)
	}

	agentID := agentIDForExternal(cfg.Name)
	reg := registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    "a2a-virtual",
		Version:      "1.0.0",
		Capabilities: caps,
		Labels:       map[string]string{"a2a_endpoint": cfg.Endpoint, "a2a_name": cfg.Name},
		Status:       registry.StatusActive,
		NodeID:       "a2a-gateway",
		Origin:       "a2a",
	}

	if _, err := m.reg.Register(ctx, reg); err != nil {
		return "", fmt.Errorf("register a2a virtual agent %s: %w", agentID, err)
	}

	m.logger.Info("onboarded external A2A agent",
		"agent_id", agentID,
		"name", cfg.Name,
		"endpoint", cfg.Endpoint,
		"capabilities", len(caps),
	)
	return agentID, nil
}

// StartHealthPolling launches a background goroutine that re-fetches the
// Agent Card at the configured interval. On HTTP error it increments missed
// heartbeats (standard lifecycle path: 3 missed → UNHEALTHY, 6 → DEAD).
// On successful recovery it reactivates the agent. The goroutine exits when
// ctx is cancelled.
func (m *ExternalAgentManager) StartHealthPolling(ctx context.Context, agentID, endpoint string, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.checkHealth(ctx, agentID, endpoint)
			}
		}
	}()
}

func (m *ExternalAgentManager) checkHealth(ctx context.Context, agentID, endpoint string) {
	_, fetchErr := m.fetchAgentCard(endpoint)

	reg, revision, getErr := m.reg.Get(ctx, agentID)
	if getErr != nil {
		m.logger.Warn("health check: agent not found in registry", "agent_id", agentID)
		return
	}

	if fetchErr != nil {
		m.logger.Warn("external A2A agent unreachable",
			"agent_id", agentID,
			"endpoint", endpoint,
			"error", fetchErr,
		)
		_, _, _ = m.reg.IncrementMissedHeartbeats(ctx, agentID, revision)
		return
	}

	// Agent responded — recover if currently unhealthy, otherwise heartbeat.
	if reg.Status != registry.StatusActive {
		if _, err := m.reg.UpdateStatus(ctx, agentID, registry.StatusActive, revision); err != nil {
			m.logger.Warn("health check: failed to reactivate agent", "agent_id", agentID, "error", err)
		} else {
			m.logger.Info("external A2A agent recovered", "agent_id", agentID)
		}
		return
	}
	if _, err := m.reg.RecordHeartbeat(ctx, agentID, revision); err != nil {
		m.logger.Warn("health check: failed to record heartbeat", "agent_id", agentID, "error", err)
	}
}

// resolveCapabilityName maps an A2A skill to a Baran capability name.
// Priority order:
//  1. Manual mapping from config SkillsMapping
//  2. Standard catalog match (skill.ID is a known standard capability)
//  3. Auto vendor namespace: a2a.{agent_name_seg}.{skill_id_seg}
func (m *ExternalAgentManager) resolveCapabilityName(cfg ExternalAgentConfig, skill AgentSkill) string {
	// (1) Manual mapping.
	if mapped, ok := cfg.SkillsMapping[skill.ID]; ok && mapped != "" {
		return mapped
	}

	// (2) Standard catalog exact match.
	if m.cat != nil && m.cat.Lookup(skill.ID) != nil {
		return skill.ID
	}

	// (3) Auto vendor namespace: a2a.{agent_name}.{skill_id}.
	// "a2a" is not a standard category so this satisfies vendor namespace rules.
	agentSeg := sanitizeSegment(cfg.Name)
	skillSeg := sanitizeSegment(skill.ID)
	if agentSeg == "" || skillSeg == "" {
		return ""
	}
	vendorName := fmt.Sprintf("a2a.%s.%s", agentSeg, skillSeg)

	if m.cat != nil {
		if err := m.cat.Validate(vendorName); err != nil {
			m.logger.Warn("generated vendor capability name is invalid",
				"name", vendorName,
				"error", err,
			)
			return ""
		}
	}
	return vendorName
}

// fetchAgentCard retrieves the Agent Card from {endpoint}/.well-known/agent-card.json.
func (m *ExternalAgentManager) fetchAgentCard(endpoint string) (*AgentCard, error) {
	url := strings.TrimRight(endpoint, "/") + "/.well-known/agent-card.json"
	resp, err := m.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d from %s", resp.StatusCode, url)
	}

	var card AgentCard
	if err := json.NewDecoder(resp.Body).Decode(&card); err != nil {
		return nil, fmt.Errorf("decode agent card: %w", err)
	}
	return &card, nil
}

// agentIDForExternal generates a stable agent ID for an external A2A agent
// based on its configured name.
func agentIDForExternal(name string) string {
	return fmt.Sprintf("a2a-ext-%s", sanitizeSegment(name))
}

// sanitizeSegment converts a string to a valid capability name segment
// (lowercase alphanumeric and hyphens only, no leading/trailing hyphens).
func sanitizeSegment(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}
