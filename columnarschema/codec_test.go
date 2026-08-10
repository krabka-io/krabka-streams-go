package columnarschema

import (
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	// Register the well-known types the test descriptor imports.
	_ "google.golang.org/protobuf/types/known/structpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/krabka-io/krabka-streams-go/columnar"
	registry "github.com/krabka-io/krabka-streams-go/schema"
)

const readerSchema = `{"type": "record", "name": "Order", "fields": [
  {"name": "id", "type": "string"},
  {"name": "amount", "type": "long"}
]}`

func offlineCache(t *testing.T) *registry.SchemaCache {
	t.Helper()
	client, err := registry.NewRegistryClient("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	return registry.NewSchemaCache(client)
}

func avroFrame(t *testing.T, schemaID int, writer avro.Schema, record map[string]any) []byte {
	t.Helper()
	body, err := avro.Marshal(writer, record)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Encode(schemaID, body)
}

func TestDecodesFramedRecordsIntoTypedColumnsAndEncodesThemBack(t *testing.T) {
	mem := checkedTestAllocator(t)
	reader := avro.MustParse(readerSchema)
	cache := offlineCache(t)
	cache.SeedSubjectID("orders-value", 11)
	cache.SeedWriterSchema(11, reader.String())
	codec, err := NewAvroBatchCodec(readerSchema, cache, mem)
	if err != nil {
		t.Fatal(err)
	}
	frame := avroFrame(t, 11, reader, map[string]any{"id": "o-1", "amount": int64(7)})
	records := []columnar.ConsumedRecord{
		columnar.NewConsumedRecord([]byte("k"), frame, 42, 3, 100),
	}

	batch, err := codec.Decode("orders", records)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	id := batch.Column(batch.Schema().FieldIndices("id")[0]).(*array.String)
	amount := batch.Column(batch.Schema().FieldIndices("amount")[0]).(*array.Int64)
	if id.Value(0) != "o-1" || amount.Value(0) != 7 {
		t.Fatalf("unexpected payload %s %d", id.Value(0), amount.Value(0))
	}
	key := batch.Column(batch.Schema().FieldIndices("__key")[0]).(*array.Binary)
	if string(key.Value(0)) != "k" {
		t.Fatal("unexpected record key")
	}
	timestamp := batch.Column(batch.Schema().FieldIndices("__timestamp")[0]).(*array.Int64)
	offset := batch.Column(batch.Schema().FieldIndices("__offset")[0]).(*array.Int64)
	if timestamp.Value(0) != 42 || offset.Value(0) != 100 {
		t.Fatal("unexpected metadata")
	}

	produced, err := codec.Encode("orders", batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(produced) != 1 {
		t.Fatalf("unexpected produced count %d", len(produced))
	}
	if string(produced[0].Value) != string(frame) {
		t.Fatal("the encoded frame must round trip byte-for-byte")
	}
	if string(produced[0].Key) != "k" {
		t.Fatal("the record key must round trip")
	}
}

func TestResolvesEvolvedWriterSchemasOntoTheFixedReaderColumns(t *testing.T) {
	mem := checkedTestAllocator(t)
	writer := avro.MustParse(`{"type": "record", "name": "Order", "fields": [
	  {"name": "id", "type": "string"},
	  {"name": "amount", "type": "long"},
	  {"name": "note", "type": "string"}
	]}`)
	reader := avro.MustParse(readerSchema)
	cache := offlineCache(t)
	cache.SeedSubjectID("orders-value", 11)
	cache.SeedWriterSchema(11, reader.String())
	cache.SeedWriterSchema(21, writer.String())
	codec, err := NewAvroBatchCodec(readerSchema, cache, mem)
	if err != nil {
		t.Fatal(err)
	}
	frame := avroFrame(t, 21, writer, map[string]any{
		"id": "o-2", "amount": int64(9), "note": "dropped by the reader"})

	batch, err := codec.Decode("orders", []columnar.ConsumedRecord{
		columnar.NewConsumedRecord(nil, frame, 1, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	if len(batch.Schema().FieldIndices("note")) != 0 {
		t.Fatal("writer-only fields must not appear")
	}
	id := batch.Column(batch.Schema().FieldIndices("id")[0]).(*array.String)
	if id.Value(0) != "o-2" {
		t.Fatalf("unexpected id %s", id.Value(0))
	}
	if len(codec.ArrowSchema().Fields()) != 2 {
		t.Fatal("the Arrow schema must derive from the reader schema")
	}
}

func TestUnknownWriterSchemaIDsSurfaceTheRetriablePendingFetch(t *testing.T) {
	mem := checkedTestAllocator(t)
	reader := avro.MustParse(readerSchema)
	cache := offlineCache(t)
	cache.SeedSubjectID("orders-value", 11)
	cache.SeedWriterSchema(11, reader.String())
	codec, err := NewAvroBatchCodec(readerSchema, cache, mem)
	if err != nil {
		t.Fatal(err)
	}
	unknown := avroFrame(t, 99, reader, map[string]any{"id": "o-3", "amount": int64(1)})

	_, err = codec.Decode("orders", []columnar.ConsumedRecord{
		columnar.NewConsumedRecord(nil, unknown, 1, 0, 0)})

	var pending *registry.FetchPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("expected the retriable pending fetch, got %v", err)
	}
}

func everythingDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	scalar := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:     proto.String(name),
			Number:   proto.Int32(number),
			Type:     kind.Enum(),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			JsonName: proto.String(name),
		}
	}
	message := func(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
		field := scalar(name, number, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
		field.TypeName = proto.String(typeName)
		return field
	}
	repeated := func(field *descriptorpb.FieldDescriptorProto) *descriptorpb.FieldDescriptorProto {
		field.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		return field
	}
	oneof := func(field *descriptorpb.FieldDescriptorProto) *descriptorpb.FieldDescriptorProto {
		field.OneofIndex = proto.Int32(0)
		return field
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("krabka_test.proto"),
		Package: proto.String("krabka.test"),
		Syntax:  proto.String("proto3"),
		Dependency: []string{
			"google/protobuf/timestamp.proto",
			"google/protobuf/wrappers.proto",
			"google/protobuf/struct.proto",
		},
		EnumType: []*descriptorpb.EnumDescriptorProto{{
			Name: proto.String("Color"),
			Value: []*descriptorpb.EnumValueDescriptorProto{
				{Name: proto.String("RED"), Number: proto.Int32(0)},
				{Name: proto.String("BLUE"), Number: proto.Int32(1)},
			},
		}},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Child"),
				Field: []*descriptorpb.FieldDescriptorProto{
					scalar("name", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
					message("next", 2, ".krabka.test.Child"),
				},
			},
			{
				Name: proto.String("Everything"),
				NestedType: []*descriptorpb.DescriptorProto{{
					Name:    proto.String("LabelsEntry"),
					Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					Field: []*descriptorpb.FieldDescriptorProto{
						scalar("key", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
						scalar("value", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64),
					},
				}},
				OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("either")}},
				Field: []*descriptorpb.FieldDescriptorProto{
					scalar("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
					scalar("count", 2, descriptorpb.FieldDescriptorProto_TYPE_INT32),
					scalar("ucount", 3, descriptorpb.FieldDescriptorProto_TYPE_UINT32),
					scalar("big_count", 4, descriptorpb.FieldDescriptorProto_TYPE_UINT64),
					scalar("ratio", 5, descriptorpb.FieldDescriptorProto_TYPE_DOUBLE),
					scalar("flag", 6, descriptorpb.FieldDescriptorProto_TYPE_BOOL),
					scalar("payload", 7, descriptorpb.FieldDescriptorProto_TYPE_BYTES),
					func() *descriptorpb.FieldDescriptorProto {
						field := scalar("color", 8, descriptorpb.FieldDescriptorProto_TYPE_ENUM)
						field.TypeName = proto.String(".krabka.test.Color")
						return field
					}(),
					message("chld", 9, ".krabka.test.Child"),
					repeated(scalar("tags", 10, descriptorpb.FieldDescriptorProto_TYPE_STRING)),
					repeated(message("labels", 11, ".krabka.test.Everything.LabelsEntry")),
					message("stamp", 12, ".google.protobuf.Timestamp"),
					message("maybe_name", 13, ".google.protobuf.StringValue"),
					message("meta", 14, ".google.protobuf.Struct"),
					oneof(scalar("either_text", 15, descriptorpb.FieldDescriptorProto_TYPE_STRING)),
					oneof(scalar("either_num", 16, descriptorpb.FieldDescriptorProto_TYPE_INT64)),
				},
			},
		},
	}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor.Messages().ByName("Everything")
}

func TestDecodesFramedMessagesIntoTypedColumnsAndEncodesThemBack(t *testing.T) {
	mem := checkedTestAllocator(t)
	descriptor := everythingDescriptor(t)
	cache := offlineCache(t)
	cache.SeedSubjectID("events-value", 12)
	cache.SeedWriterSchema(12, `syntax = "proto3";`)
	cache.SeedWriterMessageType(12, string(descriptor.FullName()))
	prototype := dynamicpb.NewMessage(descriptor)
	serde := registry.NewProtobufSerde(prototype, cache, registry.RoleValue)
	message := dynamicpb.NewMessage(descriptor)
	message.Set(descriptor.Fields().ByName("id"), protoreflect.ValueOfString("e-1"))
	message.Set(descriptor.Fields().ByName("count"), protoreflect.ValueOfInt32(3))
	tags := message.Mutable(descriptor.Fields().ByName("tags")).List()
	tags.Append(protoreflect.ValueOfString("x"))
	frame, err := serde.Serialize("events", message)
	if err != nil {
		t.Fatal(err)
	}

	codec := NewProtobufBatchCodec(prototype, cache, mem)
	records := []columnar.ConsumedRecord{
		columnar.NewConsumedRecord([]byte("k"), frame, 42, 0, 9),
	}

	batch, err := codec.Decode("events", records)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	id := batch.Column(batch.Schema().FieldIndices("id")[0]).(*array.String)
	if id.Value(0) != "e-1" {
		t.Fatalf("unexpected id %s", id.Value(0))
	}
	timestamp := batch.Column(batch.Schema().FieldIndices("__timestamp")[0]).(*array.Int64)
	if timestamp.Value(0) != 42 {
		t.Fatal("unexpected timestamp")
	}
	if len(codec.ArrowSchema().Fields()) != descriptor.Fields().Len() {
		t.Fatal("the Arrow schema must cover every message field")
	}

	produced, err := codec.Encode("events", batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(produced) != 1 {
		t.Fatalf("unexpected produced count %d", len(produced))
	}
	if string(produced[0].Value) != string(frame) {
		t.Fatalf("the encoded frame must round trip byte-for-byte\n got % x\nwant % x", produced[0].Value, frame)
	}
}

func TestProtobufSchemaCoversWellKnownShapes(t *testing.T) {
	descriptor := everythingDescriptor(t)

	schema := ProtobufArrowSchema(descriptor)

	stamp := schema.Field(schema.FieldIndices("stamp")[0])
	if stamp.Type.ID().String() != "TIMESTAMP" {
		t.Fatalf("stamp must be a timestamp, got %s", stamp.Type)
	}
	maybeName := schema.Field(schema.FieldIndices("maybe_name")[0])
	if metadataValue(maybeName, MetadataProtoWrapper) != "google.protobuf.StringValue" {
		t.Fatal("wrapper columns must be tagged")
	}
	meta := schema.Field(schema.FieldIndices("meta")[0])
	if metadataValue(meta, MetadataJSON) != "true" ||
		metadataValue(meta, MetadataProtoMessage) != "google.protobuf.Struct" {
		t.Fatal("dynamic messages must fall back to tagged JSON")
	}
	chld := schema.Field(schema.FieldIndices("chld")[0])
	structType, ok := chld.Type.(interface{ NumFields() int })
	if !ok || structType.NumFields() != 2 {
		t.Fatalf("chld must be a two-field struct, got %s", chld.Type)
	}
	eitherText := schema.Field(schema.FieldIndices("either_text")[0])
	if metadataValue(eitherText, MetadataProtoOneof) != "either" {
		t.Fatal("oneof members must be tagged with their oneof")
	}
	if !eitherText.Nullable {
		t.Fatal("oneof members must be nullable")
	}
	if !strings.Contains(chld.Type.String(), "next: utf8") {
		t.Fatalf("recursive Child.next must fall back to JSON text, got %s", chld.Type)
	}
}
