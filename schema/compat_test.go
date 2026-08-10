package schema

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

var allModes = []CompatibilityMode{Backward, Forward, Full}

func TestDetectsAvroTypeChanges(t *testing.T) {
	previous := `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"}]}`
	candidate := `{"type":"record","name":"Value","fields":[{"name":"id","type":"string"}]}`

	for _, mode := range allModes {
		result, err := AvroCompatibility(previous, candidate, mode)
		if err != nil {
			t.Fatal(err)
		}
		if result.Compatible {
			t.Fatalf("%s: expected incompatible", mode)
		}
	}
}

func TestDetectsJSONTypeChanges(t *testing.T) {
	previous := `{"type":"object","properties":{"id":{"type":"integer"}}}`
	candidate := `{"type":"object","properties":{"id":{"type":"string"}}}`

	for _, mode := range allModes {
		result, err := JSONCompatibility(previous, candidate, mode)
		if err != nil {
			t.Fatal(err)
		}
		if result.Compatible {
			t.Fatalf("%s: expected incompatible", mode)
		}
	}
}

func TestDetectsProtobufWireTypeChanges(t *testing.T) {
	previous := compatDescriptor(t, descriptorpb.FieldDescriptorProto_TYPE_INT64, false)
	candidate := compatDescriptor(t, descriptorpb.FieldDescriptorProto_TYPE_STRING, false)

	for _, mode := range allModes {
		if ProtobufCompatibility(previous, candidate, mode).Compatible {
			t.Fatalf("%s: expected incompatible", mode)
		}
	}
}

func TestAcceptsCompatibleEvolution(t *testing.T) {
	previousAvro := `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"}]}`
	candidateAvro := `{"type":"record","name":"Value","fields":[{"name":"id","type":"long"},` +
		`{"name":"note","type":["null","string"],"default":null}]}`
	previousJSON := `{"type":"object","properties":{"id":{"type":"integer"}}}`
	candidateJSON := `{"type":"object","properties":{"id":{"type":"integer"},"note":{"type":"string"}}}`

	for _, mode := range allModes {
		avroResult, err := AvroCompatibility(previousAvro, candidateAvro, mode)
		if err != nil {
			t.Fatal(err)
		}
		if !avroResult.Compatible {
			t.Fatalf("%s: Avro incompatibilities %v", mode, avroResult.Incompatibilities)
		}
		jsonResult, err := JSONCompatibility(previousJSON, candidateJSON, mode)
		if err != nil {
			t.Fatal(err)
		}
		if !jsonResult.Compatible {
			t.Fatalf("%s: JSON incompatibilities %v", mode, jsonResult.Incompatibilities)
		}
		protobufResult := ProtobufCompatibility(
			compatDescriptor(t, descriptorpb.FieldDescriptorProto_TYPE_INT64, false),
			compatDescriptor(t, descriptorpb.FieldDescriptorProto_TYPE_INT64, true),
			mode)
		if !protobufResult.Compatible {
			t.Fatalf("%s: Protobuf incompatibilities %v", mode, protobufResult.Incompatibilities)
		}
	}
}

func TestDetectsNarrowingFromAnUnconstrainedJSONSchema(t *testing.T) {
	result, err := JSONCompatibility(`{}`, `{"type":"string"}`, Backward)
	if err != nil {
		t.Fatal(err)
	}

	if result.Compatible {
		t.Fatal("expected incompatible")
	}
}

func compatDescriptor(t *testing.T, fieldType descriptorpb.FieldDescriptorProto_Type, extraField bool) protoreflect.FileDescriptor {
	t.Helper()
	fields := []*descriptorpb.FieldDescriptorProto{{
		Name:     proto.String("id"),
		Number:   proto.Int32(1),
		Type:     fieldType.Enum(),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		JsonName: proto.String("id"),
	}}
	if extraField {
		fields = append(fields, &descriptorpb.FieldDescriptorProto{
			Name:     proto.String("note"),
			Number:   proto.Int32(2),
			Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			JsonName: proto.String("note"),
		})
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("value.proto"),
		Package: proto.String("demo"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String("Value"),
			Field: fields,
		}},
	}
	descriptor, err := protodesc.NewFile(file, nil)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
