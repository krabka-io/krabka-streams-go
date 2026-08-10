package columnarschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hamba/avro/v2"

	"github.com/krabka-io/krabka-streams-go/columnar"
)

// AvroRowBridge converts between Avro generic values and Arrow batches.
//
// Rows use hamba/avro's generic representation: records are map[string]any,
// enums strings, fixed values byte arrays, decimals *big.Rat, date and
// timestamp logical types time.Time, time logical types time.Duration, and
// non-nullable unions map[string]any keyed by the branch name.
//
// The Arrow schema derives from the fixed reader schema once, at
// construction. The bridge is safe for concurrent use.
type AvroRowBridge struct {
	reader *avro.RecordSchema
	fields []arrow.Field
}

// NewAvroRowBridge creates a bridge for a top-level Avro record schema.
func NewAvroRowBridge(schema avro.Schema) (*AvroRowBridge, error) {
	record, ok := schema.(*avro.RecordSchema)
	if !ok {
		return nil, fmt.Errorf("the top-level Avro schema must be a record: %s", schema)
	}
	fields, err := avroTopLevelFields(record)
	if err != nil {
		return nil, err
	}
	return &AvroRowBridge{reader: record, fields: fields}, nil
}

// ArrowSchema returns the Arrow schema of every batch the bridge produces.
func (b *AvroRowBridge) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema(b.fields, nil)
}

// RowsToBatch implements columnar.RowBridge.
func (b *AvroRowBridge) RowsToBatch(rows []any, mem memory.Allocator) (arrow.Record, error) {
	builder := array.NewRecordBuilder(mem, b.ArrowSchema())
	defer builder.Release()
	for column, field := range b.fields {
		avroField := b.reader.Fields()[column]
		target := builder.Field(column)
		for _, row := range rows {
			record, ok := row.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("Avro row is not a record: %T", row)
			}
			converted, err := avroToArrow(record[field.Name], avroField.Type(), field)
			if err != nil {
				return nil, err
			}
			if err := columnar.AppendValue(target, converted); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// BatchToRows implements columnar.RowBridge.
func (b *AvroRowBridge) BatchToRows(batch arrow.Record) ([]any, error) {
	rows := make([]any, 0, batch.NumRows())
	columns := make([]arrow.Array, len(b.fields))
	for column, field := range b.fields {
		indices := batch.Schema().FieldIndices(field.Name)
		if len(indices) == 0 {
			return nil, fmt.Errorf("Arrow batch has no column %s", field.Name)
		}
		columns[column] = batch.Column(indices[0])
	}
	for row := range int(batch.NumRows()) {
		record := make(map[string]any, len(b.fields))
		for column, field := range b.fields {
			value, err := avroFromArrow(
				columnar.Value(columns[column], row), b.reader.Fields()[column].Type(), field)
			if err != nil {
				return nil, err
			}
			record[field.Name] = value
		}
		rows = append(rows, record)
	}
	return rows, nil
}

// avroToArrow converts one hamba generic value into a value AppendValue
// accepts for the field's Arrow type.
func avroToArrow(value any, schema avro.Schema, field arrow.Field) (any, error) {
	if ref, ok := schema.(*avro.RefSchema); ok {
		schema = ref.Schema()
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		names, branches := avroUnionBranches(union)
		if metadataValue(field, MetadataAvroUnion) == "true" {
			if value == nil {
				return requireNullable(value, field)
			}
			branchName, branchValue, err := resolveUnionBranch(value, names, branches)
			if err != nil {
				return nil, fmt.Errorf("%w in field %s", err, field.Name)
			}
			child, ok := structChild(field, branchName)
			if !ok {
				return nil, fmt.Errorf("no union branch column %s in field %s", branchName, field.Name)
			}
			converted, err := avroToArrow(branchValue, branches[branchName], child)
			if err != nil {
				return nil, err
			}
			return map[string]any{branchName: converted}, nil
		}
		if value == nil {
			return requireNullable(value, field)
		}
		if unwrappedName, unwrapped, err := unwrapUnion(value, names); err == nil {
			return avroToArrow(unwrapped, branches[unwrappedName], field)
		}
		return avroToArrow(value, branches[names[0]], field)
	}
	if value == nil {
		return requireNullable(value, field)
	}
	if metadataValue(field, MetadataJSON) == "true" {
		return avroJSONText(value, schema)
	}
	switch typed := schema.(type) {
	case *avro.RecordSchema:
		record, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Avro value for field %s is not a record: %T", field.Name, value)
		}
		structType := field.Type.(*arrow.StructType)
		result := make(map[string]any, structType.NumFields())
		for index, child := range typed.Fields() {
			converted, err := avroToArrow(record[child.Name()], child.Type(), structType.Field(index))
			if err != nil {
				return nil, err
			}
			result[child.Name()] = converted
		}
		return result, nil
	case *avro.FixedSchema:
		if _, ok := typed.Logical().(*avro.DecimalLogicalSchema); ok {
			return value, nil
		}
		return fixedBytes(value), nil
	case *avro.ArraySchema:
		items, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("Avro value for field %s is not an array: %T", field.Name, value)
		}
		itemField := field.Type.(*arrow.ListType).ElemField()
		result := make([]any, len(items))
		for i, item := range items {
			converted, err := avroToArrow(item, typed.Items(), itemField)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case *avro.MapSchema:
		entries, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Avro value for field %s is not a map: %T", field.Name, value)
		}
		valueField := arrow.Field{Name: "value", Type: field.Type.(*arrow.MapType).ItemType(), Nullable: true}
		result := make(map[string]any, len(entries))
		for key, entry := range entries {
			converted, err := avroToArrow(entry, typed.Values(), valueField)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	default:
		return value, nil
	}
}

// avroFromArrow converts one Arrow value back into the hamba generic shape.
func avroFromArrow(value any, schema avro.Schema, field arrow.Field) (any, error) {
	if ref, ok := schema.(*avro.RefSchema); ok {
		schema = ref.Schema()
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		names, branches := avroUnionBranches(union)
		if value == nil {
			return nil, nil
		}
		if metadataValue(field, MetadataAvroUnion) == "true" {
			struct_, ok := value.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("Arrow union value for field %s is not a struct: %T", field.Name, value)
			}
			for _, name := range names {
				branchValue, present := struct_[name]
				if branchValue == nil || !present {
					continue
				}
				child, _ := structChild(field, name)
				return avroFromArrow(branchValue, branches[name], child)
			}
			return nil, nil
		}
		return avroFromArrow(value, branches[names[0]], field)
	}
	if value == nil {
		return nil, nil
	}
	if metadataValue(field, MetadataJSON) == "true" {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("Arrow JSON fallback value for field %s is not text: %T", field.Name, value)
		}
		return avroJSONParse(text, schema)
	}
	switch typed := schema.(type) {
	case *avro.RecordSchema:
		struct_, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Arrow value for field %s is not a struct: %T", field.Name, value)
		}
		structType := field.Type.(*arrow.StructType)
		result := make(map[string]any, len(typed.Fields()))
		for index, child := range typed.Fields() {
			converted, err := avroFromArrow(struct_[child.Name()], child.Type(), structType.Field(index))
			if err != nil {
				return nil, err
			}
			result[child.Name()] = converted
		}
		return result, nil
	case *avro.EnumSchema:
		return stringOf(value), nil
	case *avro.FixedSchema:
		if _, ok := typed.Logical().(*avro.DecimalLogicalSchema); ok {
			return value, nil
		}
		return toFixedArray(value, typed.Size())
	case *avro.ArraySchema:
		items, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("Arrow value for field %s is not a list: %T", field.Name, value)
		}
		itemField := field.Type.(*arrow.ListType).ElemField()
		result := make([]any, len(items))
		for i, item := range items {
			converted, err := avroFromArrow(item, typed.Items(), itemField)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	case *avro.MapSchema:
		entries, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("Arrow value for field %s is not a map: %T", field.Name, value)
		}
		valueField := arrow.Field{Name: "value", Type: field.Type.(*arrow.MapType).ItemType(), Nullable: true}
		result := make(map[string]any, len(entries))
		for key, entry := range entries {
			converted, err := avroFromArrow(entry, typed.Values(), valueField)
			if err != nil {
				return nil, err
			}
			result[key] = converted
		}
		return result, nil
	case *avro.PrimitiveSchema:
		return avroScalarFromArrow(value, typed)
	default:
		return nil, fmt.Errorf("cannot convert Arrow value back to Avro type %s", schema.Type())
	}
}

func avroScalarFromArrow(value any, schema *avro.PrimitiveSchema) (any, error) {
	logical := avro.LogicalType("")
	if schema.Logical() != nil {
		logical = schema.Logical().Type()
	}
	switch schema.Type() {
	case avro.Int:
		switch logical {
		case avro.Date:
			days, err := toInt64(value)
			if err != nil {
				return nil, err
			}
			return time.Unix(days*86_400, 0).UTC(), nil
		case avro.TimeMillis:
			millis, err := toInt64(value)
			if err != nil {
				return nil, err
			}
			return time.Duration(millis) * time.Millisecond, nil
		}
		number, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return int(number), nil
	case avro.Long:
		number, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		switch logical {
		case avro.TimeMicros:
			return time.Duration(number) * time.Microsecond, nil
		case avro.TimestampMillis, avro.LocalTimestampMillis:
			return time.UnixMilli(number).UTC(), nil
		case avro.TimestampMicros, avro.LocalTimestampMicros:
			return time.UnixMicro(number).UTC(), nil
		}
		return number, nil
	case avro.Float:
		if typed, ok := value.(float32); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("cannot convert %T to an Avro float", value)
	case avro.Double:
		if typed, ok := value.(float64); ok {
			return typed, nil
		}
		return nil, fmt.Errorf("cannot convert %T to an Avro double", value)
	case avro.Boolean:
		return value, nil
	case avro.String:
		return stringOf(value), nil
	case avro.Bytes:
		if logical == avro.Decimal {
			return value, nil
		}
		data, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to Avro bytes", value)
		}
		return data, nil
	default:
		return value, nil
	}
}

func toInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to an integer", value)
	}
}

func stringOf(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func requireNullable(value any, field arrow.Field) (any, error) {
	if !field.Nullable {
		return nil, fmt.Errorf("required Avro field is null: %s", field.Name)
	}
	return value, nil
}

// unwrapUnion accepts hamba's single-entry union map and returns the branch
// name and value.
func unwrapUnion(value any, names []string) (string, any, error) {
	wrapped, ok := value.(map[string]any)
	if ok && len(wrapped) == 1 {
		for key, branchValue := range wrapped {
			if slices.Contains(names, key) {
				return key, branchValue, nil
			}
		}
	}
	return "", nil, fmt.Errorf("no union branch column for value")
}

// resolveUnionBranch finds the union branch for either hamba's wrapped map
// form or an unwrapped generic value, matching the Go type to the branch.
func resolveUnionBranch(value any, names []string, branches map[string]avro.Schema) (string, any, error) {
	if name, unwrapped, err := unwrapUnion(value, names); err == nil {
		return name, unwrapped, nil
	}
	for _, name := range names {
		if genericMatchesSchema(value, branches[name]) {
			return name, value, nil
		}
	}
	return "", nil, fmt.Errorf("no union branch column for value of type %T", value)
}

func genericMatchesSchema(value any, schema avro.Schema) bool {
	if ref, ok := schema.(*avro.RefSchema); ok {
		schema = ref.Schema()
	}
	logical := avro.LogicalType("")
	if withLogical, ok := schema.(avro.LogicalTypeSchema); ok && withLogical.Logical() != nil {
		logical = withLogical.Logical().Type()
	}
	switch value.(type) {
	case bool:
		return schema.Type() == avro.Boolean
	case int, int32:
		return schema.Type() == avro.Int && logical == ""
	case int64:
		return schema.Type() == avro.Long && logical == ""
	case float32:
		return schema.Type() == avro.Float
	case float64:
		return schema.Type() == avro.Double
	case string:
		return schema.Type() == avro.String || schema.Type() == avro.Enum
	case []byte:
		return schema.Type() == avro.Bytes && logical == ""
	case []any:
		return schema.Type() == avro.Array
	case map[string]any:
		return schema.Type() == avro.Record || schema.Type() == avro.Map
	case time.Time:
		return logical == avro.Date || logical == avro.TimestampMillis ||
			logical == avro.TimestampMicros || logical == avro.LocalTimestampMillis ||
			logical == avro.LocalTimestampMicros
	case time.Duration:
		return logical == avro.TimeMillis || logical == avro.TimeMicros
	default:
		if reflect.ValueOf(value).Kind() == reflect.Array {
			return schema.Type() == avro.Fixed
		}
		return logical == avro.Decimal
	}
}

func structChild(field arrow.Field, name string) (arrow.Field, bool) {
	structType, ok := field.Type.(*arrow.StructType)
	if !ok {
		return arrow.Field{}, false
	}
	index, ok := structType.FieldIdx(name)
	if !ok {
		return arrow.Field{}, false
	}
	return structType.Field(index), true
}

func metadataValue(field arrow.Field, key string) string {
	index := field.Metadata.FindKey(key)
	if index < 0 {
		return ""
	}
	return field.Metadata.Values()[index]
}

func fixedBytes(value any) any {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() == reflect.Array {
		result := make([]byte, reflected.Len())
		for i := range result {
			result[i] = byte(reflected.Index(i).Uint())
		}
		return result
	}
	return value
}

func toFixedArray(value any, size int) (any, error) {
	data, ok := value.([]byte)
	if !ok || len(data) != size {
		return nil, fmt.Errorf("cannot convert %T to an Avro fixed of size %d", value, size)
	}
	arrayType := reflect.ArrayOf(size, reflect.TypeFor[byte]())
	result := reflect.New(arrayType).Elem()
	for i, item := range data {
		result.Index(i).Set(reflect.ValueOf(item))
	}
	return result.Interface(), nil
}

// avroJSONText encodes a generic value as Avro JSON, the tagged fallback for
// shapes with no finite Arrow form.
func avroJSONText(value any, schema avro.Schema) (string, error) {
	var buffer bytes.Buffer
	if err := writeAvroJSON(&buffer, value, schema); err != nil {
		return "", fmt.Errorf("cannot encode recursive Avro value as JSON: %w", err)
	}
	return buffer.String(), nil
}

func writeAvroJSON(buffer *bytes.Buffer, value any, schema avro.Schema) error {
	if ref, ok := schema.(*avro.RefSchema); ok {
		schema = ref.Schema()
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		if value == nil {
			buffer.WriteString("null")
			return nil
		}
		names, branches := avroUnionBranches(union)
		branchName, branchValue, err := unwrapUnion(value, names)
		if err != nil {
			if len(names) != 1 {
				return err
			}
			branchName, branchValue = names[0], value
		}
		encodedName, _ := json.Marshal(branchName)
		buffer.WriteByte('{')
		buffer.Write(encodedName)
		buffer.WriteByte(':')
		if err := writeAvroJSON(buffer, branchValue, branches[branchName]); err != nil {
			return err
		}
		buffer.WriteByte('}')
		return nil
	}
	switch typed := schema.(type) {
	case *avro.RecordSchema:
		record, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("not a record: %T", value)
		}
		buffer.WriteByte('{')
		for index, field := range typed.Fields() {
			if index > 0 {
				buffer.WriteByte(',')
			}
			encodedName, _ := json.Marshal(field.Name())
			buffer.Write(encodedName)
			buffer.WriteByte(':')
			if err := writeAvroJSON(buffer, record[field.Name()], field.Type()); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
		return nil
	case *avro.ArraySchema:
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("not an array: %T", value)
		}
		buffer.WriteByte('[')
		for index, item := range items {
			if index > 0 {
				buffer.WriteByte(',')
			}
			if err := writeAvroJSON(buffer, item, typed.Items()); err != nil {
				return err
			}
		}
		buffer.WriteByte(']')
		return nil
	case *avro.MapSchema:
		entries, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("not a map: %T", value)
		}
		buffer.WriteByte('{')
		first := true
		for _, key := range slices.Sorted(maps.Keys(entries)) {
			if !first {
				buffer.WriteByte(',')
			}
			first = false
			encodedKey, _ := json.Marshal(key)
			buffer.Write(encodedKey)
			buffer.WriteByte(':')
			if err := writeAvroJSON(buffer, entries[key], typed.Values()); err != nil {
				return err
			}
		}
		buffer.WriteByte('}')
		return nil
	case *avro.FixedSchema:
		if _, ok := typed.Logical().(*avro.DecimalLogicalSchema); ok {
			return fmt.Errorf("decimal values are unsupported in the JSON fallback")
		}
		return writeAvroJSONBytes(buffer, fixedBytes(value))
	case *avro.EnumSchema:
		encoded, _ := json.Marshal(stringOf(value))
		buffer.Write(encoded)
		return nil
	case *avro.PrimitiveSchema:
		if typed.Logical() != nil && typed.Logical().Type() == avro.Decimal {
			return fmt.Errorf("decimal values are unsupported in the JSON fallback")
		}
		if typed.Type() == avro.Bytes {
			return writeAvroJSONBytes(buffer, value)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		buffer.Write(encoded)
		return nil
	default:
		return fmt.Errorf("unsupported schema %s in the JSON fallback", schema.Type())
	}
}

// writeAvroJSONBytes encodes bytes as Avro JSON: a string with one character
// per byte.
func writeAvroJSONBytes(buffer *bytes.Buffer, value any) error {
	data, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("not bytes: %T", value)
	}
	runes := make([]rune, len(data))
	for i, item := range data {
		runes[i] = rune(item)
	}
	encoded, err := json.Marshal(string(runes))
	if err != nil {
		return err
	}
	buffer.Write(encoded)
	return nil
}

// avroJSONParse decodes Avro JSON text back into the generic representation.
func avroJSONParse(text string, schema avro.Schema) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("cannot decode recursive Avro value from JSON: %w", err)
	}
	value, err := readAvroJSON(document, schema)
	if err != nil {
		return nil, fmt.Errorf("cannot decode recursive Avro value from JSON: %w", err)
	}
	return value, nil
}

func readAvroJSON(document any, schema avro.Schema) (any, error) {
	if ref, ok := schema.(*avro.RefSchema); ok {
		schema = ref.Schema()
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		if document == nil {
			return nil, nil
		}
		wrapped, ok := document.(map[string]any)
		if !ok || len(wrapped) != 1 {
			return nil, fmt.Errorf("invalid union JSON value")
		}
		_, branches := avroUnionBranches(union)
		for branchName, branchDocument := range wrapped {
			branch, ok := branches[branchName]
			if !ok {
				return nil, fmt.Errorf("unknown union branch %s", branchName)
			}
			// Unions decode to the branch value directly, matching hamba's
			// generic representation.
			return readAvroJSON(branchDocument, branch)
		}
	}
	switch typed := schema.(type) {
	case *avro.RecordSchema:
		object, ok := document.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid record JSON value")
		}
		result := make(map[string]any, len(typed.Fields()))
		for _, field := range typed.Fields() {
			value, err := readAvroJSON(object[field.Name()], field.Type())
			if err != nil {
				return nil, err
			}
			result[field.Name()] = value
		}
		return result, nil
	case *avro.ArraySchema:
		items, ok := document.([]any)
		if !ok {
			return nil, fmt.Errorf("invalid array JSON value")
		}
		result := make([]any, len(items))
		for i, item := range items {
			value, err := readAvroJSON(item, typed.Items())
			if err != nil {
				return nil, err
			}
			result[i] = value
		}
		return result, nil
	case *avro.MapSchema:
		object, ok := document.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid map JSON value")
		}
		result := make(map[string]any, len(object))
		for key, item := range object {
			value, err := readAvroJSON(item, typed.Values())
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		return result, nil
	case *avro.FixedSchema:
		data, err := avroJSONBytes(document)
		if err != nil {
			return nil, err
		}
		return toFixedArray(data, typed.Size())
	case *avro.EnumSchema:
		return stringOf(document), nil
	case *avro.PrimitiveSchema:
		return readAvroJSONPrimitive(document, typed)
	}
	return nil, fmt.Errorf("unsupported schema %s in the JSON fallback", schema.Type())
}

func readAvroJSONPrimitive(document any, schema *avro.PrimitiveSchema) (any, error) {
	switch schema.Type() {
	case avro.Boolean:
		return document, nil
	case avro.Int:
		number, err := jsonNumber(document)
		if err != nil {
			return nil, err
		}
		result, err := number.Int64()
		return int(result), err
	case avro.Long:
		number, err := jsonNumber(document)
		if err != nil {
			return nil, err
		}
		return number.Int64()
	case avro.Float:
		number, err := jsonNumber(document)
		if err != nil {
			return nil, err
		}
		result, err := number.Float64()
		return float32(result), err
	case avro.Double:
		number, err := jsonNumber(document)
		if err != nil {
			return nil, err
		}
		return number.Float64()
	case avro.String:
		return stringOf(document), nil
	case avro.Bytes:
		return avroJSONBytes(document)
	default:
		return nil, fmt.Errorf("unsupported primitive %s in the JSON fallback", schema.Type())
	}
}

func jsonNumber(document any) (json.Number, error) {
	number, ok := document.(json.Number)
	if !ok {
		return "", fmt.Errorf("invalid numeric JSON value %T", document)
	}
	return number, nil
}

func avroJSONBytes(document any) ([]byte, error) {
	text, ok := document.(string)
	if !ok {
		return nil, fmt.Errorf("invalid bytes JSON value %T", document)
	}
	runes := []rune(text)
	result := make([]byte, len(runes))
	for i, item := range runes {
		if item > 255 {
			return nil, fmt.Errorf("invalid byte in JSON bytes value: %d", item)
		}
		result[i] = byte(item)
	}
	return result, nil
}
