package a2a

import (
	"fmt"

	"github.com/baran-network/baran-os/core/registry"
)

// GenerateAgentCard creates a composite A2A Agent Card from all active Baran agents.
func GenerateAgentCard(agents []registry.AgentRegistration, externalURL string, port int) AgentCard {
	var skills []AgentSkill
	inputSet := make(map[string]bool)
	outputSet := make(map[string]bool)

	for _, agent := range agents {
		for _, cap := range agent.Capabilities {
			skill := CapabilityToSkill(cap)
			skills = append(skills, skill)
			for _, m := range skill.InputModes {
				inputSet[m] = true
			}
			for _, m := range skill.OutputModes {
				outputSet[m] = true
			}
		}
	}

	if externalURL == "" {
		externalURL = fmt.Sprintf("http://localhost:%d", port)
	}

	return AgentCard{
		Name:        "Baran OS Node",
		Description: "Baran OS agent network",
		Version:     "1.0.0",
		SupportedInterfaces: []SupportedInterface{
			{
				URL:             externalURL,
				ProtocolBinding: "JSONRPC",
				ProtocolVersion: "1.0",
			},
		},
		Capabilities: AgentCapabilities{
			Streaming:         false,
			PushNotifications: false,
			ExtendedAgentCard: false,
		},
		DefaultInputModes:  setToSlice(inputSet),
		DefaultOutputModes: setToSlice(outputSet),
		Skills:             deduplicateSkills(skills),
	}
}

// deduplicateSkills removes duplicate skills by ID.
func deduplicateSkills(skills []AgentSkill) []AgentSkill {
	seen := make(map[string]bool, len(skills))
	result := make([]AgentSkill, 0, len(skills))
	for _, s := range skills {
		if !seen[s.ID] {
			seen[s.ID] = true
			result = append(result, s)
		}
	}
	return result
}

func setToSlice(m map[string]bool) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}
