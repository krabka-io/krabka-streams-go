package columnar

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const jsonMetadata = "krabka.json"
const binaryMetadata = "krabka.binary"

// JSONRowBridge is a [RowBridge] that routes values through JSON.
//
// Column inference uses the first non-null sample and is retained by the
// bridge for every later batch; to pin the schema up front, use
// [NewJSONRowBridgeWithSchema] or [JSONRowBridgeFromJSONSchema]. Nested
// objects and arrays become serialized JSON text columns tagged with the
// krabka.json field metadata, so they survive a round trip.
//
// Scalar row types (strings, numbers, booleans, and slices) have no fields to
// spread across columns, so they are wrapped in a single column named
// "value". Column order follows first appearance across the rows.
type JSONRowBridge[T any] struct {
	fields []arrow.Field
	scalar bool
}

// NewJSONRowBridge creates a bridge that infers its Arrow schema from the
// first batch.
func NewJSONRowBridge[T any]() *JSONRowBridge[T] {
	return &JSONRowBridge[T]{scalar: isScalarType[T]()}
}

// NewJSONRowBridgeWithSchema creates a bridge with a pinned Arrow schema.
func NewJSONRowBridgeWithSchema[T any](schema *arrow.Schema) *JSONRowBridge[T] {
	return &JSONRowBridge[T]{scalar: isScalarType[T](), fields: schema.Fields()}
}

// JSONRowBridgeFromJSONSchema derives the Arrow fields from a JSON Schema's
// properties, required list, local $ref references, primitive types, arrays,
// objects, and base64 content. Required fields reject nulls before a batch
// can escape.
func JSONRowBridgeFromJSONSchema[T any](jsonSchema string) (*JSONRowBridge[T], error) {
	scalar := isScalarType[T]()
	keys, values, err := orderedJSONObject([]byte(jsonSchema))
	if err != nil {
		return nil, fmt.Errorf("invalid JSON Schema: %w", err)
	}
	root := rawObject{keys: keys, values: values}
	fields, err := jsonSchemaFields(root, scalar)
	if err != nil {
		return nil, err
	}
	return &JSONRowBridge[T]{scalar: scalar, fields: fields}, nil
}

// RowsToBatch implements [RowBridge].
func (b *JSONRowBridge[T]) RowsToBatch(rows []T, mem memory.Allocator) (arrow.Record, error) {
	objects := make([]rawObject, len(rows))
	for i, row := range rows {
		object, err := b.objectFor(row)
		if err != nil {
			return nil, err
		}
		objects[i] = object
	}
	batchFields := b.fields
	if batchFields == nil {
		var order []string
		samples := map[string][]json.RawMessage{}
		for _, object := range objects {
			for _, key := range object.keys {
				if _, ok := samples[key]; !ok {
					order = append(order, key)
				}
				samples[key] = append(samples[key], object.values[key])
			}
		}
		batchFields = make([]arrow.Field, 0, len(order))
		for _, name := range order {
			batchFields = append(batchFields, inferredField(name, samples[name]))
		}
		if len(rows) > 0 {
			b.fields = batchFields
		}
	}
	builder := array.NewRecordBuilder(mem, arrow.NewSchema(batchFields, nil))
	defer builder.Release()
	builder.Reserve(len(rows))
	for column, field := range batchFields {
		target := builder.Field(column)
		for _, object := range objects {
			if err := writeJSONValue(target, object.values[field.Name], field); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// BatchToRows implements [RowBridge].
func (b *JSONRowBridge[T]) BatchToRows(batch arrow.Record) ([]T, error) {
	rows := make([]T, 0, batch.NumRows())
	var zero T
	for row := 0; row < int(batch.NumRows()); row++ {
		var document []byte
		if b.scalar {
			value, err := readJSONValue(batch.Column(0), batch.Schema().Field(0), row)
			if err != nil {
				return nil, err
			}
			document = value
		} else {
			var object bytes.Buffer
			object.WriteByte('{')
			for column := 0; column < int(batch.NumCols()); column++ {
				field := batch.Schema().Field(column)
				value, err := readJSONValue(batch.Column(column), field, row)
				if err != nil {
					return nil, err
				}
				if column > 0 {
					object.WriteByte(',')
				}
				name, _ := json.Marshal(field.Name)
				object.Write(name)
				object.WriteByte(':')
				object.Write(value)
			}
			object.WriteByte('}')
			document = object.Bytes()
		}
		value := zero
		if err := json.Unmarshal(document, &value); err != nil {
			return nil, fmt.Errorf("cannot convert Arrow row %d to %T: %w", row, zero, err)
		}
		rows = append(rows, value)
	}
	return rows, nil
}

type rawObject struct {
	keys   []string
	values map[string]json.RawMessage
}

func (b *JSONRowBridge[T]) objectFor(row T) (rawObject, error) {
	encoded, err := json.Marshal(row)
	if err != nil {
		return rawObject{}, fmt.Errorf("cannot encode row as JSON: %w", err)
	}
	if !b.scalar {
		keys, values, err := orderedJSONObject(encoded)
		if err == nil {
			return rawObject{keys: keys, values: values}, nil
		}
	}
	return rawObject{keys: []string{"value"}, values: map[string]json.RawMessage{"value": encoded}}, nil
}

// orderedJSONObject parses a JSON object preserving key order.
func orderedJSONObject(data []byte) ([]string, map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return nil, nil, err
	}
	if delimiter, ok := first.(json.Delim); !ok || delimiter != '{' {
		return nil, nil, fmt.Errorf("not a JSON object")
	}
	var keys []string
	values := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, nil, err
		}
		key := keyToken.(string)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, nil, err
		}
		if _, ok := values[key]; !ok {
			keys = append(keys, key)
		}
		values[key] = value
	}
	return keys, values, nil
}

func jsonKind(value json.RawMessage) byte {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 {
		return 'n'
	}
	switch trimmed[0] {
	case '"':
		return 's'
	case '{':
		return 'o'
	case '[':
		return 'a'
	case 't', 'f':
		return 'b'
	case 'n':
		return 'n'
	default:
		if bytes.ContainsAny(trimmed, ".eE") {
			return 'f'
		}
		return 'i'
	}
}

// inferredField types a column from the first non-null sample: text becomes
// Utf8, integral numbers Int64, floating point Float64, booleans Bool, and
// objects or arrays JSON-tagged Utf8. All-null columns default to Utf8.
func inferredField(name string, samples []json.RawMessage) arrow.Field {
	kind := byte('n')
	for _, sample := range samples {
		if sample == nil {
			continue
		}
		if sampleKind := jsonKind(sample); sampleKind != 'n' {
			kind = sampleKind
			break
		}
	}
	field := arrow.Field{Name: name, Nullable: true, Type: arrow.BinaryTypes.String}
	switch kind {
	case 'i':
		field.Type = arrow.PrimitiveTypes.Int64
	case 'f':
		field.Type = arrow.PrimitiveTypes.Float64
	case 'b':
		field.Type = arrow.FixedWidthTypes.Boolean
	case 'o', 'a':
		field.Metadata = arrow.NewMetadata([]string{jsonMetadata}, []string{"true"})
	}
	return field
}

func writeJSONValue(builder array.Builder, value json.RawMessage, field arrow.Field) error {
	if value == nil || jsonKind(value) == 'n' {
		if !field.Nullable {
			return fmt.Errorf("required JSON field is null: %s", field.Name)
		}
		builder.AppendNull()
		return nil
	}
	var converted any
	switch {
	case fieldMetadataValue(field, jsonMetadata) == "true":
		converted = string(value)
	case fieldMetadataValue(field, binaryMetadata) == "true":
		var text string
		if err := json.Unmarshal(value, &text); err != nil {
			return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
		}
		decoded, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
		}
		converted = decoded
	default:
		switch jsonKind(value) {
		case 'i':
			var number int64
			if err := json.Unmarshal(value, &number); err != nil {
				return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
			}
			converted = number
		case 'f':
			var number float64
			if err := json.Unmarshal(value, &number); err != nil {
				return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
			}
			converted = number
		case 'b':
			converted = bytes.Equal(bytes.TrimSpace(value), []byte("true"))
		case 's':
			var text string
			if err := json.Unmarshal(value, &text); err != nil {
				return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
			}
			converted = text
		default:
			converted = string(value)
		}
	}
	if err := appendGoValue(builder, converted); err != nil {
		return fmt.Errorf("cannot write JSON field %s: %w", field.Name, err)
	}
	return nil
}

func readJSONValue(arr arrow.Array, field arrow.Field, row int) (json.RawMessage, error) {
	if arr.IsNull(row) {
		return json.RawMessage("null"), nil
	}
	value := arrowValue(arr, row)
	if fieldMetadataValue(field, jsonMetadata) == "true" {
		text, ok := value.(string)
		if !ok || !json.Valid([]byte(text)) {
			return nil, fmt.Errorf("cannot read JSON field %s", field.Name)
		}
		return json.RawMessage(text), nil
	}
	if fieldMetadataValue(field, binaryMetadata) == "true" {
		data, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("cannot read JSON field %s", field.Name)
		}
		encoded, _ := json.Marshal(base64.StdEncoding.EncodeToString(data))
		return json.RawMessage(encoded), nil
	}
	switch typed := value.(type) {
	case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64:
		encoded, err := json.Marshal(typed)
		if err != nil {
			return nil, fmt.Errorf("cannot read JSON field %s: %w", field.Name, err)
		}
		return json.RawMessage(encoded), nil
	default:
		encoded, err := json.Marshal(stringify(value))
		if err != nil {
			return nil, fmt.Errorf("cannot read JSON field %s: %w", field.Name, err)
		}
		return json.RawMessage(encoded), nil
	}
}

func isScalarType[T any]() bool {
	var zero T
	typ := reflect.TypeOf(&zero).Elem()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.Slice, reflect.Array:
		return true
	default:
		return false
	}
}

func jsonSchemaFields(root rawObject, scalar bool) ([]arrow.Field, error) {
	if scalar {
		field, err := jsonSchemaField("value", root, false, root)
		if err != nil {
			return nil, err
		}
		return []arrow.Field{field}, nil
	}
	propertiesRaw, ok := root.values["properties"]
	if !ok {
		return nil, fmt.Errorf("object JSON Schema has no properties")
	}
	keys, values, err := orderedJSONObject(propertiesRaw)
	if err != nil {
		return nil, fmt.Errorf("object JSON Schema has no properties")
	}
	required := map[string]bool{}
	if requiredRaw, ok := root.values["required"]; ok {
		var names []string
		if err := json.Unmarshal(requiredRaw, &names); err == nil {
			for _, name := range names {
				required[name] = true
			}
		}
	}
	fields := make([]arrow.Field, 0, len(keys))
	for _, name := range keys {
		declarationKeys, declarationValues, err := orderedJSONObject(values[name])
		if err != nil {
			return nil, fmt.Errorf("invalid JSON Schema property %s", name)
		}
		field, err := jsonSchemaField(name,
			rawObject{keys: declarationKeys, values: declarationValues}, !required[name], root)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

func jsonSchemaField(name string, declaration rawObject, nullable bool, root rawObject) (arrow.Field, error) {
	resolved, err := resolveJSONSchemaRef(declaration, root)
	if err != nil {
		return arrow.Field{}, err
	}
	var typeName string
	if typeRaw, ok := resolved.values["type"]; ok {
		var text string
		if json.Unmarshal(typeRaw, &text) == nil {
			typeName = text
		} else {
			var candidates []string
			if json.Unmarshal(typeRaw, &candidates) == nil {
				for _, candidate := range candidates {
					if candidate == "null" {
						nullable = true
					} else if typeName == "" {
						typeName = candidate
					}
				}
			}
		}
	}
	field := arrow.Field{Name: name, Nullable: nullable, Type: arrow.BinaryTypes.String}
	switch typeName {
	case "integer":
		field.Type = arrow.PrimitiveTypes.Int64
	case "number":
		field.Type = arrow.PrimitiveTypes.Float64
	case "boolean":
		field.Type = arrow.FixedWidthTypes.Boolean
	case "object", "array":
		field.Metadata = arrow.NewMetadata([]string{jsonMetadata}, []string{"true"})
	default:
		var contentEncoding string
		if encodingRaw, ok := resolved.values["contentEncoding"]; ok {
			_ = json.Unmarshal(encodingRaw, &contentEncoding)
		}
		if contentEncoding == "base64" {
			field.Type = arrow.BinaryTypes.Binary
			field.Metadata = arrow.NewMetadata([]string{binaryMetadata}, []string{"true"})
		}
	}
	return field, nil
}

func resolveJSONSchemaRef(declaration rawObject, root rawObject) (rawObject, error) {
	referenceRaw, ok := declaration.values["$ref"]
	if !ok {
		return declaration, nil
	}
	var reference string
	if err := json.Unmarshal(referenceRaw, &reference); err != nil || !strings.HasPrefix(reference, "#/") {
		return declaration, nil
	}
	current := declaration
	node := root
	for _, step := range strings.Split(strings.TrimPrefix(reference, "#/"), "/") {
		value, ok := node.values[step]
		if !ok {
			return current, fmt.Errorf("unresolved JSON Schema reference %s", reference)
		}
		keys, values, err := orderedJSONObject(value)
		if err != nil {
			return current, fmt.Errorf("unresolved JSON Schema reference %s", reference)
		}
		node = rawObject{keys: keys, values: values}
	}
	return node, nil
}
