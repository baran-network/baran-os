package sidecar

// RegisterRequest is the payload for POST /agents.
type RegisterRequest struct {
	AgentID      string            `json:"agent_id"`
	Name         string            `json:"name"`
	AgentType    string            `json:"agent_type"`
	Version      string            `json:"version"`
	Capabilities []CapabilitySpec  `json:"capabilities"`
	Labels       map[string]string `json:"labels"`
	CallbackURL  string            `json:"callback_url"`
}

// CapabilitySpec is the JSON representation of an agent capability.
type CapabilitySpec struct {
	Name        string            `json:"name"`
	Version     string            `json:"version"`
	Description string            `json:"description"`
	Parameters  map[string]string `json:"parameters"`
}

// PublishRequest is the payload for POST /agents/{id}/events.
type PublishRequest struct {
	EventType string            `json:"event_type"`
	Payload   map[string]any    `json:"payload"`
	Metadata  map[string]string `json:"metadata"`
}

// AckRequest is the payload for POST /agents/{id}/ack.
type AckRequest struct {
	DeliveryID string `json:"delivery_id"`
}
