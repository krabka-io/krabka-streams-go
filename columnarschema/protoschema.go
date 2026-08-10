package columnarschema

import (
	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const protoTimestamp = "google.protobuf.Timestamp"

var protoJSONFallback = map[string]bool{
	"google.protobuf.Struct":    true,
	"google.protobuf.Value":     true,
	"google.protobuf.ListValue": true,
}

var protoWrappers = map[string]arrow.DataType{
	"google.protobuf.DoubleValue": arrow.PrimitiveTypes.Float64,
	"google.protobuf.FloatValue":  arrow.PrimitiveTypes.Float32,
	"google.protobuf.Int64Value":  arrow.PrimitiveTypes.Int64,
	"google.protobuf.UInt64Value": arrow.PrimitiveTypes.Uint64,
	"google.protobuf.Int32Value":  arrow.PrimitiveTypes.Int32,
	"google.protobuf.UInt32Value": arrow.PrimitiveTypes.Int64,
	"google.protobuf.BoolValue":   arrow.FixedWidthTypes.Boolean,
	"google.protobuf.StringValue": arrow.BinaryTypes.String,
	"google.protobuf.BytesValue":  arrow.BinaryTypes.Binary,
}

// ProtobufArrowSchema translates a message descriptor into the Arrow schema
// its bridge produces, without touching data.
func ProtobufArrowSchema(descriptor protoreflect.MessageDescriptor) *arrow.Schema {
	return arrow.NewSchema(protoTopLevelFields(descriptor), nil)
}

// ProtobufArrowField translates one message field into an Arrow field.
func ProtobufArrowField(field protoreflect.FieldDescriptor) arrow.Field {
	return protoField(field, []string{string(field.ContainingMessage().FullName())})
}

func protoTopLevelFields(descriptor protoreflect.MessageDescriptor) []arrow.Field {
	fields := descriptor.Fields()
	result := make([]arrow.Field, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		result = append(result, protoField(fields.Get(i), []string{string(descriptor.FullName())}))
	}
	return result
}

func protoField(field protoreflect.FieldDescriptor, visiting []string) arrow.Field {
	if field.IsMap() {
		return protoMapField(field, visiting)
	}
	if field.IsList() {
		element := protoElement("item", field, visiting)
		return arrow.Field{
			Name:     string(field.Name()),
			Type:     arrow.ListOfField(element),
			Metadata: oneofMetadata(field, nil),
		}
	}
	element := protoElement(string(field.Name()), field, visiting)
	return arrow.Field{
		Name:     string(field.Name()),
		Type:     element.Type,
		Nullable: field.HasPresence(),
		Metadata: oneofMetadata(field, fieldMetadataPairs(element)),
	}
}

func protoElement(name string, field protoreflect.FieldDescriptor, visiting []string) arrow.Field {
	switch field.Kind() {
	case protoreflect.DoubleKind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64}
	case protoreflect.FloatKind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64}
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Uint64}
	case protoreflect.BoolKind:
		return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean}
	case protoreflect.StringKind:
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.String}
	case protoreflect.BytesKind:
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary}
	case protoreflect.EnumKind:
		return taggedField(name, arrow.BinaryTypes.String, false,
			map[string]string{MetadataProtoEnum: string(field.Enum().FullName())})
	default:
		return protoMessageField(name, field.Message(), visiting)
	}
}

func protoMessageField(name string, message protoreflect.MessageDescriptor, visiting []string) arrow.Field {
	fullName := string(message.FullName())
	if fullName == protoTimestamp {
		return arrow.Field{Name: name,
			Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}
	}
	if wrapped, ok := protoWrappers[fullName]; ok {
		return taggedField(name, wrapped, false, map[string]string{MetadataProtoWrapper: fullName})
	}
	if protoJSONFallback[fullName] || contains(visiting, fullName) {
		return taggedField(name, arrow.BinaryTypes.String, false, map[string]string{
			MetadataJSON:         "true",
			MetadataProtoMessage: fullName,
		})
	}
	visiting = append(visiting, fullName)
	fields := message.Fields()
	children := make([]arrow.Field, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		children = append(children, protoField(fields.Get(i), visiting))
	}
	return arrow.Field{Name: name, Type: arrow.StructOf(children...)}
}

func protoMapField(field protoreflect.FieldDescriptor, visiting []string) arrow.Field {
	key := protoElement("key", field.MapKey(), visiting)
	value := protoField(field.MapValue(), visiting)
	return arrow.Field{
		Name:     string(field.Name()),
		Type:     arrow.MapOf(key.Type, value.Type),
		Metadata: oneofMetadata(field, nil),
	}
}

func oneofMetadata(field protoreflect.FieldDescriptor, pairs map[string]string) arrow.Metadata {
	oneof := field.ContainingOneof()
	if oneof == nil || oneof.IsSynthetic() {
		return buildMetadata(pairs)
	}
	result := map[string]string{MetadataProtoOneof: string(oneof.Name())}
	for key, value := range pairs {
		result[key] = value
	}
	return buildMetadata(result)
}

func fieldMetadataPairs(field arrow.Field) map[string]string {
	result := map[string]string{}
	for i, key := range field.Metadata.Keys() {
		result[key] = field.Metadata.Values()[i]
	}
	return result
}
