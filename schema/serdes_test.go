package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func offlineCache(t *testing.T) *SchemaCache {
	t.Helper()
	// The unreachable URL is intentional: if a test ever performs a lookup,
	// it fails immediately instead of reaching a real service.
	client, err := NewRegistryClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	return NewSchemaCache(client)
}

func TestAvroRoundTripsWithWriterResolution(t *testing.T) {
	schemaText := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	cache := offlineCache(t)
	serde, err := NewGenericAvroSerde(schemaText, cache, RoleValue)
	if err != nil {
		t.Fatal(err)
	}
	cache.SeedSubjectID("orders-value", 11)
	cache.SeedWriterSchema(11, serde.ReaderSchema().String())

	data, err := serde.Serialize("orders", map[string]any{"id": "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}

	record, ok := decoded.(map[string]any)
	if !ok || record["id"] != "o-1" {
		t.Fatalf("unexpected decode %v", decoded)
	}
}

func TestAvroTypedRoundTripsStruct(t *testing.T) {
	type ReflectedOrder struct {
		ID string `avro:"id"`
	}
	schemaText := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	cache := offlineCache(t)
	serde, err := NewAvroSerde[ReflectedOrder](schemaText, cache, RoleValue)
	if err != nil {
		t.Fatal(err)
	}
	cache.SeedSubjectID("orders-value", 15)
	cache.SeedWriterSchema(15, serde.ReaderSchema().String())

	data, err := serde.Serialize("orders", ReflectedOrder{ID: "o-3"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.ID != "o-3" {
		t.Fatalf("unexpected decode %+v", decoded)
	}
}

func TestAvroResolvesWriterSchemaOntoReader(t *testing.T) {
	writerText := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`
	readerText := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},` +
		`{"name":"note","type":["null","string"],"default":null}]}`
	cache := offlineCache(t)
	writerSerde, err := NewGenericAvroSerde(writerText, cache, RoleValue)
	if err != nil {
		t.Fatal(err)
	}
	readerSerde, err := NewGenericAvroSerde(readerText, cache, RoleValue)
	if err != nil {
		t.Fatal(err)
	}
	cache.SeedSubjectID("orders-value", 21)
	cache.SeedWriterSchema(21, writerSerde.ReaderSchema().String())

	data, err := writerSerde.Serialize("orders", map[string]any{"id": "o-9"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := readerSerde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}

	record := decoded.(map[string]any)
	if record["id"] != "o-9" || record["note"] != nil {
		t.Fatalf("unexpected resolved decode %v", record)
	}
}

func TestProtobufRoundTripsAndChecksMessageType(t *testing.T) {
	cache := offlineCache(t)
	cache.SeedSubjectID("messages-value", 12)
	cache.SeedWriterSchema(12, `syntax = "proto3";`)
	cache.SeedWriterMessageType(12, "google.protobuf.StringValue")
	serde := NewProtobufSerde(&wrapperspb.StringValue{}, cache, RoleValue)

	data, err := serde.Serialize("messages", wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("messages", data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.GetValue() != "hello" {
		t.Fatalf("unexpected decode %v", decoded)
	}

	cache.SeedWriterMessageType(12, "demo.Other")
	if _, err := serde.Deserialize("messages", data); err == nil ||
		!strings.Contains(err.Error(), "Protobuf messageType mismatch") {
		t.Fatalf("expected a messageType mismatch, got %v", err)
	}
}

func TestJSONRoundTripsAndValidatesWriterSchema(t *testing.T) {
	type Order struct {
		ID string `json:"id"`
	}
	schemaText := `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`
	cache := offlineCache(t)
	cache.SeedSubjectID("orders-value", 13)
	cache.SeedWriterSchema(13, schemaText)
	serde, err := NewJSONSchemaSerde[Order](schemaText, cache, RoleValue, true)
	if err != nil {
		t.Fatal(err)
	}

	data, err := serde.Serialize("orders", Order{ID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, Order{ID: "o-1"}) {
		t.Fatalf("unexpected decode %+v", decoded)
	}

	invalid := Encode(13, []byte("{}"))
	if _, err := serde.Deserialize("orders", invalid); err == nil ||
		!strings.Contains(err.Error(), "JSON Schema validation failed") {
		t.Fatalf("expected a validation failure, got %v", err)
	}
}

func TestKafkaNilsRemainNil(t *testing.T) {
	type Order struct {
		ID string `json:"id"`
	}
	serde, err := NewJSONSchemaSerde[*Order](`{"type":"object"}`, offlineCache(t), RoleValue, false)
	if err != nil {
		t.Fatal(err)
	}

	data, err := serde.Serialize("orders", nil)
	if err != nil || data != nil {
		t.Fatalf("nil value must serialize to nil bytes, got %v (%v)", data, err)
	}
	decoded, err := serde.Deserialize("orders", nil)
	if err != nil || decoded != nil {
		t.Fatalf("nil bytes must deserialize to nil, got %v (%v)", decoded, err)
	}
}

func TestSerdeCanOverrideCacheSubjectStrategy(t *testing.T) {
	type Order struct {
		ID string `json:"id"`
	}
	cache := offlineCache(t)
	cache.SeedSubjectID("record.Order", 14)
	serde, err := NewJSONSchemaSerde[Order](`{"type":"object"}`, cache, RoleValue, false,
		WithStrategy(func(topic string, role Role) string { return "record.Order" }))
	if err != nil {
		t.Fatal(err)
	}

	data, err := serde.Serialize("ignored", Order{ID: "o-2"})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if frame.SchemaID != 14 {
		t.Fatalf("unexpected schema id %d", frame.SchemaID)
	}
}

func TestUnresolvedSubjectFailsSerialization(t *testing.T) {
	serde, err := NewGenericAvroSerde(`"string"`, offlineCache(t), RoleValue)
	if err != nil {
		t.Fatal(err)
	}

	_, err = serde.Serialize("orders", "value")

	if err == nil || !strings.Contains(err.Error(),
		"schema ID for orders-value is not resolved; call RegisterSubject and prewarm first") {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestUnknownWriterSchemaSurfacesPendingError(t *testing.T) {
	serde, err := NewGenericAvroSerde(`"string"`, offlineCache(t), RoleValue)
	if err != nil {
		t.Fatal(err)
	}

	_, err = serde.Deserialize("orders", Encode(99, []byte{2, 'h', 'i'}))

	var pending *FetchPendingError
	if !errors.As(err, &pending) || pending.SchemaID != 99 {
		t.Fatalf("expected FetchPendingError for id 99, got %v", err)
	}
}

func TestProtobufPrintsFullDescriptorsAndFramesNestedIndexes(t *testing.T) {
	file := &descriptorpb.FileDescriptorProto{
		Name:    new("demo.proto"),
		Syntax:  new("proto3"),
		Package: new("demo"),
		Options: &descriptorpb.FileOptions{JavaPackage: new("demo.generated")},
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: new("Status"),
			Value: []*descriptorpb.EnumValueDescriptorProto{{
				Name: new("UNKNOWN"), Number: proto.Int32(0),
			}},
		}},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: new("Outer"),
			NestedType: []*descriptorpb.DescriptorProto{{
				Name: new("Nested"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:     new("value"),
					Number:   proto.Int32(1),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					JsonName: new("value"),
				}},
			}},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: new("choice")}},
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:       new("name"),
				Number:     proto.Int32(1),
				Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
				Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				OneofIndex: proto.Int32(0),
				JsonName:   new("name"),
			}},
		}},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: new("Demo"),
			Method: []*descriptorpb.MethodDescriptorProto{{
				Name:       new("Get"),
				InputType:  new(".demo.Outer"),
				OutputType: new(".demo.Outer"),
			}},
		}},
	}
	descriptor, err := protodesc.NewFile(file, nil)
	if err != nil {
		t.Fatal(err)
	}

	printed := PrintProtoFile(descriptor)
	for _, expected := range []string{
		`option java_package = "demo.generated";`,
		"message Nested",
		"enum Status",
		"oneof choice",
		"service Demo",
	} {
		if !strings.Contains(printed, expected) {
			t.Fatalf("printed schema misses %q:\n%s", expected, printed)
		}
	}

	nested := descriptor.Messages().ByName("Outer").Messages().ByName(protoreflect.Name("Nested"))
	prototype := dynamicpb.NewMessage(nested)
	cache := offlineCache(t)
	cache.SeedSubjectID("messages-value", 16)
	serde := NewProtobufSerde(prototype, cache, RoleValue)

	data, err := serde.Serialize("messages", prototype)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeProtobuf(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(frame.MessageIndexes, []int{0, 0}) {
		t.Fatalf("unexpected message indexes %v", frame.MessageIndexes)
	}
}
