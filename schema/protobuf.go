package schema

import (
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ProtobufSerde is a [Serde] that encodes Protobuf messages with Confluent
// Protobuf framing, including the message-index path.
//
// The serde derives three things from its prototype message: the schema text,
// printed from the message's file descriptor with [PrintProtoFile]; the
// messageType, which is the fully qualified message name; and the full
// message-index path from the top-level parent to a nested message.
//
// Deserialization enforces a type check: if the cache holds a messageType for
// the writer's schema ID and it differs from the local message's full name,
// deserialization fails. That check is skipped when the registry supplied no
// messageType.
type ProtobufSerde[T proto.Message] struct {
	schemaSerde[T]
	prototype      T
	messageIndexes []int
}

// NewProtobufSerde creates a serde for the message type of prototype, which
// is typically the zero value of a generated message type, for example
// &ordersv1.Order{}.
func NewProtobufSerde[T proto.Message](prototype T, cache *SchemaCache, role Role, options ...SerdeOption) *ProtobufSerde[T] {
	descriptor := prototype.ProtoReflect().Descriptor()
	var applied serdeOptions
	for _, option := range options {
		option(&applied)
	}
	serde := &ProtobufSerde[T]{
		prototype:      prototype,
		messageIndexes: messageIndexes(descriptor),
	}
	serde.schemaSerde = schemaSerde[T]{
		cache:           cache,
		role:            role,
		kind:            KindProtobuf,
		schema:          PrintProtoFile(descriptor.ParentFile()),
		messageType:     string(descriptor.FullName()),
		strategy:        applied.strategy,
		serializeBody:   serde.serializeBody,
		deserializeBody: serde.deserializeBody,
		frame:           serde.protobufFrame,
		unframe:         unframeProtobuf,
	}
	return serde
}

func (s *ProtobufSerde[T]) serializeBody(value T) ([]byte, error) {
	return proto.Marshal(value)
}

func (s *ProtobufSerde[T]) deserializeBody(schemaID int, body []byte) (T, error) {
	var zero T
	if _, err := s.cache.WriterSchema(schemaID); err != nil {
		return zero, err
	}
	writerMessageType := s.cache.WriterMessageType(schemaID)
	localMessageType := string(s.prototype.ProtoReflect().Descriptor().FullName())
	if writerMessageType != "" && writerMessageType != localMessageType {
		return zero, fmt.Errorf(
			"Protobuf messageType mismatch: writer %s, local %s", writerMessageType, localMessageType)
	}
	value := s.prototype.ProtoReflect().New().Interface().(T)
	if err := proto.Unmarshal(body, value); err != nil {
		return zero, err
	}
	return value, nil
}

func (s *ProtobufSerde[T]) protobufFrame(schemaID int, body []byte) ([]byte, error) {
	return EncodeProtobuf(schemaID, s.messageIndexes, body)
}

func unframeProtobuf(data []byte) (Frame, error) {
	frame, err := DecodeProtobuf(data)
	if err != nil {
		return Frame{}, err
	}
	return Frame{SchemaID: frame.SchemaID, Body: frame.Body}, nil
}

// messageIndexes returns the message's index path within its .proto file:
// each entry is the message's position at its nesting level, outermost first.
func messageIndexes(descriptor protoreflect.MessageDescriptor) []int {
	var reversed []int
	for current := descriptor; current != nil; {
		reversed = append(reversed, current.Index())
		parent, ok := current.Parent().(protoreflect.MessageDescriptor)
		if !ok {
			break
		}
		current = parent
	}
	indexes := make([]int, len(reversed))
	for i, index := range reversed {
		indexes[len(reversed)-1-i] = index
	}
	return indexes
}
