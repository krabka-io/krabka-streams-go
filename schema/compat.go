package schema

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/hamba/avro/v2"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CompatibilityMode selects the direction of a local compatibility check.
type CompatibilityMode int

const (
	// Backward requires that the candidate schema can read data written with
	// the previous schema.
	Backward CompatibilityMode = iota

	// Forward requires that the previous schema can read data written with
	// the candidate schema.
	Forward

	// Full requires both backward and forward compatibility.
	Full
)

// String returns "BACKWARD", "FORWARD", or "FULL".
func (m CompatibilityMode) String() string {
	switch m {
	case Backward:
		return "BACKWARD"
	case Forward:
		return "FORWARD"
	default:
		return "FULL"
	}
}

// CompatibilityResult reports the outcome of a local compatibility check.
type CompatibilityResult struct {
	// Compatible is true when no incompatibility was found.
	Compatible bool

	// Incompatibilities lists every found incompatibility, prefixed with the
	// direction that failed.
	Incompatibilities []string
}

// AvroCompatibility checks a previous and candidate Avro schema without
// contacting a registry, delegating to Avro reader/writer compatibility
// rules.
func AvroCompatibility(previousSchema, candidateSchema string, mode CompatibilityMode) (CompatibilityResult, error) {
	previous, err := avro.Parse(previousSchema)
	if err != nil {
		return CompatibilityResult{}, fmt.Errorf("invalid previous Avro schema: %w", err)
	}
	candidate, err := avro.Parse(candidateSchema)
	if err != nil {
		return CompatibilityResult{}, fmt.Errorf("invalid candidate Avro schema: %w", err)
	}
	compatibility := avro.NewSchemaCompatibility()
	var errors []string
	for _, direction := range directions(mode) {
		reader, writer := previous, candidate
		if direction == Backward {
			reader, writer = candidate, previous
		}
		if err := compatibility.Compatible(reader, writer); err != nil {
			errors = append(errors, fmt.Sprintf("%s: %s", direction, err))
		}
	}
	return CompatibilityResult{Compatible: len(errors) == 0, Incompatibilities: errors}, nil
}

// JSONCompatibility checks a previous and candidate JSON Schema without
// contacting a registry. It checks type narrowing, required fields, and
// closed object properties.
func JSONCompatibility(previousSchema, candidateSchema string, mode CompatibilityMode) (CompatibilityResult, error) {
	var previous, candidate map[string]any
	if err := json.Unmarshal([]byte(previousSchema), &previous); err != nil {
		return CompatibilityResult{}, fmt.Errorf("invalid JSON Schema: %w", err)
	}
	if err := json.Unmarshal([]byte(candidateSchema), &candidate); err != nil {
		return CompatibilityResult{}, fmt.Errorf("invalid JSON Schema: %w", err)
	}
	var errors []string
	for _, direction := range directions(mode) {
		reader, writer := previous, candidate
		if direction == Backward {
			reader, writer = candidate, previous
		}
		checkJSONReader(reader, writer, "", direction, &errors)
	}
	return CompatibilityResult{Compatible: len(errors) == 0, Incompatibilities: errors}, nil
}

// ProtobufCompatibility checks a previous and candidate Protobuf file
// descriptor without contacting a registry. It compares message fields by
// wire type, cardinality, and required-field presence.
func ProtobufCompatibility(previous, candidate protoreflect.FileDescriptor, mode CompatibilityMode) CompatibilityResult {
	var errors []string
	for _, direction := range directions(mode) {
		reader, writer := previous, candidate
		if direction == Backward {
			reader, writer = candidate, previous
		}
		checkProtobufReader(reader, writer, direction, &errors)
	}
	return CompatibilityResult{Compatible: len(errors) == 0, Incompatibilities: errors}
}

func directions(mode CompatibilityMode) []CompatibilityMode {
	if mode == Full {
		return []CompatibilityMode{Backward, Forward}
	}
	return []CompatibilityMode{mode}
}

func checkJSONReader(reader, writer map[string]any, path string, direction CompatibilityMode, errors *[]string) {
	readerTypes := jsonTypes(reader)
	writerTypes := jsonTypes(writer)
	if len(readerTypes) > 0 && (len(writerTypes) == 0 || !containsAll(readerTypes, writerTypes)) {
		*errors = append(*errors, fmt.Sprintf(
			"%s: %s narrows type from %v to %v", direction, path, writerTypes, readerTypes))
		return
	}
	if !slices.Contains(readerTypes, "object") {
		if _, ok := reader["properties"]; !ok {
			return
		}
	}
	readerRequired := jsonStrings(reader["required"])
	writerRequired := jsonStrings(writer["required"])
	for _, required := range readerRequired {
		if !slices.Contains(writerRequired, required) {
			*errors = append(*errors, fmt.Sprintf(
				"%s: %s became required", direction, childPath(path, required)))
		}
	}
	readerProperties := jsonProperties(reader)
	writerProperties := jsonProperties(writer)
	for _, name := range sortedKeys(writerProperties) {
		writerProperty := writerProperties[name]
		if readerProperty, ok := readerProperties[name]; ok {
			checkJSONReader(readerProperty, writerProperty, childPath(path, name), direction, errors)
		} else if additional, ok := reader["additionalProperties"].(bool); ok && !additional {
			*errors = append(*errors, fmt.Sprintf(
				"%s: %s is no longer allowed", direction, childPath(path, name)))
		}
	}
}

func checkProtobufReader(reader, writer protoreflect.FileDescriptor, direction CompatibilityMode, errors *[]string) {
	readerMessages := protoMessages(reader)
	writerMessages := protoMessages(writer)
	for _, name := range sortedKeys(writerMessages) {
		writerMessage := writerMessages[name]
		readerMessage, ok := readerMessages[name]
		if !ok {
			*errors = append(*errors, fmt.Sprintf(
				"%s: message %s is absent from the reader", direction, name))
			continue
		}
		writerFields := writerMessage.Fields()
		for i := range writerFields.Len() {
			writerField := writerFields.Get(i)
			readerField := readerMessage.Fields().ByNumber(writerField.Number())
			if readerField != nil &&
				(protoWireType(readerField) != protoWireType(writerField) ||
					readerField.IsList() != writerField.IsList()) {
				*errors = append(*errors, fmt.Sprintf(
					"%s: %s field %d changed wire type or cardinality",
					direction, name, writerField.Number()))
			}
		}
		readerFields := readerMessage.Fields()
		for i := range readerFields.Len() {
			readerField := readerFields.Get(i)
			if readerField.Cardinality() == protoreflect.Required &&
				writerMessage.Fields().ByNumber(readerField.Number()) == nil {
				*errors = append(*errors, fmt.Sprintf(
					"%s: %s.%s is required but absent from the writer",
					direction, name, readerField.Name()))
			}
		}
	}
}

func protoMessages(file protoreflect.FileDescriptor) map[string]protoreflect.MessageDescriptor {
	result := map[string]protoreflect.MessageDescriptor{}
	messages := file.Messages()
	for i := range messages.Len() {
		addProtoMessage(messages.Get(i), result)
	}
	return result
}

func addProtoMessage(message protoreflect.MessageDescriptor, result map[string]protoreflect.MessageDescriptor) {
	result[string(message.FullName())] = message
	nested := message.Messages()
	for i := range nested.Len() {
		addProtoMessage(nested.Get(i), result)
	}
}

func protoWireType(field protoreflect.FieldDescriptor) int {
	switch field.Kind() {
	case protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.Uint32Kind,
		protoreflect.Uint64Kind, protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.BoolKind, protoreflect.EnumKind:
		return 0
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return 1
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind:
		return 2
	case protoreflect.GroupKind:
		return 3
	default:
		return 5
	}
}

func jsonTypes(schemaNode map[string]any) []string {
	switch value := schemaNode["type"].(type) {
	case string:
		return []string{value}
	case []any:
		var result []string
		for _, item := range value {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
		return result
	default:
		return nil
	}
}

func jsonStrings(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	var result []string
	for _, item := range items {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func jsonProperties(schemaNode map[string]any) map[string]map[string]any {
	properties, ok := schemaNode["properties"].(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]map[string]any, len(properties))
	for name, value := range properties {
		if property, ok := value.(map[string]any); ok {
			result[name] = property
		}
	}
	return result
}

func containsAll(values, targets []string) bool {
	for _, target := range targets {
		if !slices.Contains(values, target) {
			return false
		}
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}

func childPath(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}
