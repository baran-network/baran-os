package sidecar

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var jsonMarshaler = protojson.MarshalOptions{
	EmitUnpopulated: false,
	UseProtoNames:   false, // emit camelCase field names (JSON convention)
}

var jsonUnmarshaler = protojson.UnmarshalOptions{
	DiscardUnknown: true,
}

// JSONToProto converts a JSON payload to binary protobuf bytes for the given event type.
// The registry provides the target message type.
func JSONToProto(registry *PayloadRegistry, eventType string, jsonBytes []byte) ([]byte, error) {
	msg, err := registry.New(eventType)
	if err != nil {
		return nil, err
	}
	if err := jsonUnmarshaler.Unmarshal(jsonBytes, msg); err != nil {
		return nil, fmt.Errorf("unmarshal JSON into %T: %w", msg, err)
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal protobuf %T: %w", msg, err)
	}
	return data, nil
}

// ProtoToJSON converts binary protobuf bytes to JSON for the given event type.
// The registry provides the source message type.
func ProtoToJSON(registry *PayloadRegistry, eventType string, protoBytes []byte) ([]byte, error) {
	msg, err := registry.New(eventType)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(protoBytes, msg); err != nil {
		return nil, fmt.Errorf("unmarshal protobuf into %T: %w", msg, err)
	}
	jsonBytes, err := jsonMarshaler.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON from %T: %w", msg, err)
	}
	return jsonBytes, nil
}
