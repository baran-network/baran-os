package a2a

import (
	"strings"

	"github.com/baran-network/baran-os/core/registry"
)

// CapabilityToSkill converts a Baran Capability to an A2A AgentSkill.
func CapabilityToSkill(cap registry.Capability) AgentSkill {
	skill := AgentSkill{
		ID:          cap.Name,
		Name:        humanReadableName(cap.Name),
		Description: cap.Description,
		Tags:        buildTags(cap),
		InputModes:  cap.InputTypes,
		OutputModes: cap.OutputTypes,
	}
	if len(skill.InputModes) == 0 {
		skill.InputModes = []string{"text/plain", "application/json"}
	}
	if len(skill.OutputModes) == 0 {
		skill.OutputModes = []string{"text/plain", "application/json"}
	}
	return skill
}

// humanReadableName converts a dot-notation capability name to a human-readable title.
// "nlp.summarization" → "NLP Summarization"
func humanReadableName(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		if len(p) <= 4 {
			parts[i] = strings.ToUpper(p)
		} else {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

// buildTags creates skill tags from the capability's taxonomy fields.
func buildTags(cap registry.Capability) []string {
	tags := make([]string, 0, 2)
	if cap.Category != "" {
		tags = append(tags, cap.Category)
	}
	if cap.Name != "" {
		tags = append(tags, cap.Name)
	}
	return tags
}
