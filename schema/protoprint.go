package schema

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PrintProtoFile reconstructs .proto source text from a file descriptor.
//
// The output covers the syntax declaration, package, imports, options, nested
// definitions, enums, oneofs, services, extensions, maps, and proto2 groups.
// It is the schema text the Protobuf serde registers with the registry.
func PrintProtoFile(file protoreflect.FileDescriptor) string {
	var output strings.Builder
	syntax := "proto3"
	if file.Syntax() == protoreflect.Proto2 {
		syntax = "proto2"
	}
	fmt.Fprintf(&output, "syntax = \"%s\";\n", syntax)
	if file.Package() != "" {
		fmt.Fprintf(&output, "package %s;\n", file.Package())
	}
	imports := file.Imports()
	for i := 0; i < imports.Len(); i++ {
		fmt.Fprintf(&output, "import \"%s\";\n", escapeProto(imports.Get(i).Path()))
	}
	appendProtoOptions(&output, file.Options(), "", "option ")
	enums := file.Enums()
	for i := 0; i < enums.Len(); i++ {
		appendProtoEnum(&output, enums.Get(i), "")
	}
	messages := file.Messages()
	for i := 0; i < messages.Len(); i++ {
		appendProtoMessage(&output, messages.Get(i), "")
	}
	extensions := file.Extensions()
	for i := 0; i < extensions.Len(); i++ {
		appendProtoExtension(&output, extensions.Get(i), "")
	}
	services := file.Services()
	for i := 0; i < services.Len(); i++ {
		appendProtoService(&output, services.Get(i))
	}
	return output.String()
}

func appendProtoService(output *strings.Builder, service protoreflect.ServiceDescriptor) {
	fmt.Fprintf(output, "\nservice %s {\n", service.Name())
	appendProtoOptions(output, service.Options(), "  ", "option ")
	methods := service.Methods()
	for i := 0; i < methods.Len(); i++ {
		method := methods.Get(i)
		fmt.Fprintf(output, "  rpc %s (", method.Name())
		if method.IsStreamingClient() {
			output.WriteString("stream ")
		}
		fmt.Fprintf(output, ".%s) returns (", method.Input().FullName())
		if method.IsStreamingServer() {
			output.WriteString("stream ")
		}
		fmt.Fprintf(output, ".%s)", method.Output().FullName())
		if optionCount(method.Options()) == 0 {
			output.WriteString(";\n")
		} else {
			output.WriteString(" {\n")
			appendProtoOptions(output, method.Options(), "    ", "option ")
			output.WriteString("  }\n")
		}
	}
	output.WriteString("}\n")
}

func appendProtoMessage(output *strings.Builder, message protoreflect.MessageDescriptor, indent string) {
	if message.IsMapEntry() {
		return
	}
	fmt.Fprintf(output, "\n%smessage %s {\n", indent, message.Name())
	inner := indent + "  "
	appendProtoOptions(output, message.Options(), inner, "option ")
	enums := message.Enums()
	for i := 0; i < enums.Len(); i++ {
		appendProtoEnum(output, enums.Get(i), inner)
	}
	nested := message.Messages()
	for i := 0; i < nested.Len(); i++ {
		if !isGroupType(message, nested.Get(i)) {
			appendProtoMessage(output, nested.Get(i), inner)
		}
	}
	descriptorProto := protodesc.ToDescriptorProto(message)
	for _, reserved := range descriptorProto.GetReservedRange() {
		fmt.Fprintf(output, "%sreserved %d to %d;\n", inner, reserved.GetStart(), reserved.GetEnd()-1)
	}
	if names := descriptorProto.GetReservedName(); len(names) > 0 {
		quoted := make([]string, len(names))
		for i, name := range names {
			quoted[i] = "\"" + escapeProto(name) + "\""
		}
		fmt.Fprintf(output, "%sreserved %s;\n", inner, strings.Join(quoted, ", "))
	}
	for _, extension := range descriptorProto.GetExtensionRange() {
		fmt.Fprintf(output, "%sextensions %d to %d;\n", inner, extension.GetStart(), extension.GetEnd()-1)
	}
	fields := message.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if oneof := field.ContainingOneof(); oneof == nil || oneof.IsSynthetic() {
			appendProtoField(output, field, inner)
		}
	}
	oneofs := message.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		oneof := oneofs.Get(i)
		if oneof.IsSynthetic() {
			continue
		}
		fmt.Fprintf(output, "%soneof %s {\n", inner, oneof.Name())
		appendProtoOptions(output, oneof.Options(), inner+"  ", "option ")
		oneofFields := oneof.Fields()
		for j := 0; j < oneofFields.Len(); j++ {
			appendProtoField(output, oneofFields.Get(j), inner+"  ")
		}
		fmt.Fprintf(output, "%s}\n", inner)
	}
	extensions := message.Extensions()
	for i := 0; i < extensions.Len(); i++ {
		appendProtoExtension(output, extensions.Get(i), inner)
	}
	fmt.Fprintf(output, "%s}\n", indent)
}

func isGroupType(message, nested protoreflect.MessageDescriptor) bool {
	fields := message.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if field.Kind() == protoreflect.GroupKind && field.Message() == nested {
			return true
		}
	}
	return false
}

func appendProtoEnum(output *strings.Builder, enum protoreflect.EnumDescriptor, indent string) {
	fmt.Fprintf(output, "\n%senum %s {\n", indent, enum.Name())
	appendProtoOptions(output, enum.Options(), indent+"  ", "option ")
	values := enum.Values()
	for i := 0; i < values.Len(); i++ {
		value := values.Get(i)
		fmt.Fprintf(output, "%s  %s = %d", indent, value.Name(), value.Number())
		appendInlineProtoOptions(output, value.Options())
		output.WriteString(";\n")
	}
	fmt.Fprintf(output, "%s}\n", indent)
}

func appendProtoExtension(output *strings.Builder, field protoreflect.FieldDescriptor, indent string) {
	fmt.Fprintf(output, "%sextend .%s {\n", indent, field.ContainingMessage().FullName())
	appendProtoField(output, field, indent+"  ")
	fmt.Fprintf(output, "%s}\n", indent)
}

func appendProtoField(output *strings.Builder, field protoreflect.FieldDescriptor, indent string) {
	output.WriteString(indent)
	proto3Optional := field.ContainingOneof() != nil && field.ContainingOneof().IsSynthetic()
	proto2 := field.ParentFile().Syntax() == protoreflect.Proto2
	switch {
	case proto3Optional:
		output.WriteString("optional ")
	case field.ContainingOneof() != nil:
		// oneof members have no label in proto2 or proto3
	case field.Cardinality() == protoreflect.Repeated && !field.IsMap():
		output.WriteString("repeated ")
	case proto2 && field.Cardinality() == protoreflect.Required:
		output.WriteString("required ")
	case proto2:
		output.WriteString("optional ")
	}
	if field.Kind() == protoreflect.GroupKind {
		fmt.Fprintf(output, "group %s = %d {\n", field.Message().Name(), field.Number())
		groupFields := field.Message().Fields()
		for i := 0; i < groupFields.Len(); i++ {
			appendProtoField(output, groupFields.Get(i), indent+"  ")
		}
		fmt.Fprintf(output, "%s}\n", indent)
		return
	}
	fmt.Fprintf(output, "%s %s = %d", protoTypeName(field), field.Name(), field.Number())
	appendInlineProtoOptions(output, field.Options())
	output.WriteString(";\n")
}

func protoTypeName(field protoreflect.FieldDescriptor) string {
	if field.IsMap() {
		return fmt.Sprintf("map<%s, %s>", protoTypeName(field.MapKey()), protoTypeName(field.MapValue()))
	}
	switch field.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return "." + string(field.Message().FullName())
	case protoreflect.EnumKind:
		return "." + string(field.Enum().FullName())
	default:
		return field.Kind().String()
	}
}

func optionCount(options proto.Message) int {
	if options == nil {
		return 0
	}
	count := 0
	options.ProtoReflect().Range(func(protoreflect.FieldDescriptor, protoreflect.Value) bool {
		count++
		return true
	})
	return count
}

func appendProtoOptions(output *strings.Builder, options proto.Message, indent, prefix string) {
	forEachOption(options, func(name, value string) {
		fmt.Fprintf(output, "%s%s%s = %s;\n", indent, prefix, name, value)
	})
}

func appendInlineProtoOptions(output *strings.Builder, options proto.Message) {
	var rendered []string
	forEachOption(options, func(name, value string) {
		rendered = append(rendered, name+" = "+value)
	})
	if len(rendered) > 0 {
		fmt.Fprintf(output, " [%s]", strings.Join(rendered, ", "))
	}
}

func forEachOption(options proto.Message, emit func(name, value string)) {
	if options == nil {
		return
	}
	options.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		name := string(field.Name())
		if field.IsExtension() {
			name = "(" + string(field.FullName()) + ")"
		}
		if field.IsList() {
			list := value.List()
			for i := 0; i < list.Len(); i++ {
				emit(name, protoOptionValue(field, list.Get(i)))
			}
		} else {
			emit(name, protoOptionValue(field, value))
		}
		return true
	})
}

func protoOptionValue(field protoreflect.FieldDescriptor, value protoreflect.Value) string {
	switch field.Kind() {
	case protoreflect.StringKind:
		return "\"" + escapeProto(value.String()) + "\""
	case protoreflect.BytesKind:
		return "\"" + escapeProto(string(value.Bytes())) + "\""
	case protoreflect.EnumKind:
		if enumValue := field.Enum().Values().ByNumber(value.Enum()); enumValue != nil {
			return string(enumValue.Name())
		}
		return fmt.Sprintf("%d", value.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return "{ " + strings.TrimSpace(value.Message().Interface().(interface{ String() string }).String()) + " }"
	default:
		return value.String()
	}
}

func escapeProto(value string) string {
	replacer := strings.NewReplacer(
		"\\", "\\\\",
		"\"", "\\\"",
		"\n", "\\n",
		"\r", "\\r",
		"\t", "\\t",
	)
	return replacer.Replace(value)
}
