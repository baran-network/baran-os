package sdk

import (
	"fmt"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
	"google.golang.org/protobuf/proto"
)

// payloadTypeNames maps event type patterns to expected payload type names.
// Used for validation at emit time (constitution: Protocol Is the Contract).
var payloadTypeNames = map[string]string{
	"agent.register":           "AgentRegisterPayload",
	"agent.unregister":         "AgentUnregisterPayload",
	"agent.error":              "AgentErrorPayload",
	"agent.health.pong":        "HealthPongPayload",
	"workflow.step.result":     "WorkflowStepResultPayload",
}

// validatePayload checks that the given proto message matches the expected type
// for the provided event type. Returns an error if there is a mismatch.
func validatePayload(eventType string, msg proto.Message) error {
	if msg == nil {
		return fmt.Errorf("payload must not be nil for event type %q", eventType)
	}

	// For workflow step results, the full event type is
	// "workflow.{id}.workflow.step.result" — match the suffix.
	key := eventType
	if len(eventType) > 9 && eventType[:9] == "workflow." {
		// workflow.{id}.workflow.step.result → "workflow.step.result"
		rest := eventType[9:]
		dot := indexOf(rest, '.')
		if dot >= 0 {
			key = "workflow." + rest[dot+1:]
		}
	}

	expected, ok := payloadTypeNames[key]
	if !ok {
		return nil // unknown event type — no validation applied
	}

	actual := string(proto.MessageName(msg).Name())
	if actual != expected {
		return fmt.Errorf("event type %q requires payload %q, got %q", eventType, expected, actual)
	}
	return nil
}

// marshalPayload marshals a proto.Message and optionally validates the type.
func marshalPayload(eventType string, msg proto.Message) ([]byte, error) {
	if err := validatePayload(eventType, msg); err != nil {
		return nil, err
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal %T: %w", msg, err)
	}
	return data, nil
}

// unmarshalStepPayload deserializes a WorkflowStepPayload from raw bytes.
func unmarshalStepPayload(data []byte) (*protocolv1.WorkflowStepPayload, error) {
	var p protocolv1.WorkflowStepPayload
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal WorkflowStepPayload: %w", err)
	}
	return &p, nil
}

// unmarshalHealthPingPayload deserializes a HealthPingPayload from raw bytes.
func unmarshalHealthPingPayload(data []byte) (*protocolv1.HealthPingPayload, error) {
	var p protocolv1.HealthPingPayload
	if err := proto.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal HealthPingPayload: %w", err)
	}
	return &p, nil
}

func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}
