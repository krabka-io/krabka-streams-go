package columnarschema

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/krabka-io/krabka-streams-go/columnar"
)

// ProtobufRowBridge converts between Protobuf messages and Arrow batches.
//
// The Arrow schema derives from the message descriptor once, at construction.
// Enums become symbol names, uint64 stays exact as an unsigned 64-bit column,
// google.protobuf.Timestamp becomes a UTC microsecond timestamp with
// sub-microsecond nanos truncated, wrapper types unwrap to their value, and
// google.protobuf.Struct, Value, ListValue, and recursive messages fall back
// to canonical Protobuf JSON text.
type ProtobufRowBridge[T proto.Message] struct {
	prototype  T
	descriptor protoreflect.MessageDescriptor
	fields     []arrow.Field
}

// NewProtobufRowBridge creates a bridge for the message type of prototype.
func NewProtobufRowBridge[T proto.Message](prototype T) *ProtobufRowBridge[T] {
	descriptor := prototype.ProtoReflect().Descriptor()
	return &ProtobufRowBridge[T]{
		prototype:  prototype,
		descriptor: descriptor,
		fields:     protoTopLevelFields(descriptor),
	}
}

// ArrowSchema returns the Arrow schema of every batch the bridge produces.
func (b *ProtobufRowBridge[T]) ArrowSchema() *arrow.Schema {
	return arrow.NewSchema(b.fields, nil)
}

// RowsToBatch implements columnar.RowBridge.
func (b *ProtobufRowBridge[T]) RowsToBatch(rows []T, mem memory.Allocator) (arrow.Record, error) {
	builder := array.NewRecordBuilder(mem, b.ArrowSchema())
	defer builder.Release()
	descriptorFields := b.descriptor.Fields()
	for column, field := range b.fields {
		target := builder.Field(column)
		descriptorField := descriptorFields.Get(column)
		for _, row := range rows {
			value, err := protoColumnValue(row.ProtoReflect(), descriptorField, field)
			if err != nil {
				return nil, err
			}
			if err := columnar.AppendValue(target, value); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// BatchToRows implements columnar.RowBridge.
func (b *ProtobufRowBridge[T]) BatchToRows(batch arrow.Record) ([]T, error) {
	rows := make([]T, 0, batch.NumRows())
	descriptorFields := b.descriptor.Fields()
	columns := make([]arrow.Array, len(b.fields))
	for column, field := range b.fields {
		indices := batch.Schema().FieldIndices(field.Name)
		if len(indices) == 0 {
			return nil, fmt.Errorf("Arrow batch has no column %s", field.Name)
		}
		columns[column] = batch.Column(indices[0])
	}
	for row := range int(batch.NumRows()) {
		message := b.prototype.ProtoReflect().New()
		for column, field := range b.fields {
			value := columnar.Value(columns[column], row)
			if err := protoSetColumn(message, descriptorFields.Get(column), field, value); err != nil {
				return nil, err
			}
		}
		rows = append(rows, message.Interface().(T))
	}
	return rows, nil
}

func protoColumnValue(message protoreflect.Message, field protoreflect.FieldDescriptor, arrowField arrow.Field) (any, error) {
	if field.IsMap() {
		entries := message.Get(field).Map()
		result := make(map[string]any, entries.Len())
		var rangeErr error
		entries.Range(func(key protoreflect.MapKey, value protoreflect.Value) bool {
			converted, err := protoSingularValue(value, field.MapValue(), mapItemField(arrowField))
			if err != nil {
				rangeErr = err
				return false
			}
			result[key.Value().String()] = converted
			return true
		})
		if rangeErr != nil {
			return nil, rangeErr
		}
		return result, nil
	}
	if field.IsList() {
		items := message.Get(field).List()
		itemField := arrowField.Type.(*arrow.ListType).ElemField()
		result := make([]any, items.Len())
		for i := range result {
			converted, err := protoSingularValue(items.Get(i), field, itemField)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	}
	if field.HasPresence() && !message.Has(field) {
		return nil, nil
	}
	return protoSingularValue(message.Get(field), field, arrowField)
}

func protoSingularValue(value protoreflect.Value, field protoreflect.FieldDescriptor, arrowField arrow.Field) (any, error) {
	if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
		return protoMessageValue(value.Message(), arrowField)
	}
	return protoScalarValue(value, field), nil
}

func protoScalarValue(value protoreflect.Value, field protoreflect.FieldDescriptor) any {
	switch field.Kind() {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return int64(value.Uint())
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return value.Uint()
	case protoreflect.EnumKind:
		if symbol := field.Enum().Values().ByNumber(value.Enum()); symbol != nil {
			return string(symbol.Name())
		}
		return strconv.Itoa(int(value.Enum()))
	case protoreflect.BytesKind:
		return bytes.Clone(value.Bytes())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return int32(value.Int())
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return value.Int()
	case protoreflect.FloatKind:
		return float32(value.Float())
	case protoreflect.DoubleKind:
		return value.Float()
	case protoreflect.BoolKind:
		return value.Bool()
	default:
		return value.String()
	}
}

func protoMessageValue(message protoreflect.Message, arrowField arrow.Field) (any, error) {
	if metadataValue(arrowField, MetadataJSON) == "true" {
		text, err := protojson.MarshalOptions{}.Marshal(message.Interface())
		if err != nil {
			return nil, fmt.Errorf("cannot encode %s as JSON in column %s: %w",
				message.Descriptor().FullName(), arrowField.Name, err)
		}
		return string(text), nil
	}
	if metadataValue(arrowField, MetadataProtoWrapper) != "" {
		valueField := message.Descriptor().Fields().ByName("value")
		return protoScalarValue(message.Get(valueField), valueField), nil
	}
	if _, ok := arrowField.Type.(*arrow.TimestampType); ok {
		descriptor := message.Descriptor()
		seconds := message.Get(descriptor.Fields().ByName("seconds")).Int()
		nanos := message.Get(descriptor.Fields().ByName("nanos")).Int()
		if seconds > math.MaxInt64/1_000_000 || seconds < math.MinInt64/1_000_000 {
			return nil, fmt.Errorf("timestamp overflow in column %s", arrowField.Name)
		}
		return seconds*1_000_000 + nanos/1_000, nil
	}
	structType := arrowField.Type.(*arrow.StructType)
	messageFields := message.Descriptor().Fields()
	result := make(map[string]any, structType.NumFields())
	for index := range structType.NumFields() {
		child := structType.Field(index)
		converted, err := protoColumnValue(message, messageFields.Get(index), child)
		if err != nil {
			return nil, err
		}
		result[child.Name] = converted
	}
	return result, nil
}

func protoSetColumn(message protoreflect.Message, field protoreflect.FieldDescriptor, arrowField arrow.Field, value any) error {
	if value == nil {
		return nil
	}
	if field.IsMap() {
		entries, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("Arrow value for field %s is not a map: %T", arrowField.Name, value)
		}
		target := message.Mutable(field).Map()
		itemField := mapItemField(arrowField)
		for key, entry := range entries {
			mapKey, err := protoMapKey(key, field.MapKey())
			if err != nil {
				return err
			}
			mapValue, err := protoSingularFromArrow(target.NewValue, field.MapValue(), itemField, entry)
			if err != nil {
				return err
			}
			target.Set(mapKey, mapValue)
		}
		return nil
	}
	if field.IsList() {
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("Arrow value for field %s is not a list: %T", arrowField.Name, value)
		}
		target := message.Mutable(field).List()
		itemField := arrowField.Type.(*arrow.ListType).ElemField()
		for _, item := range items {
			if item == nil {
				continue
			}
			listValue, err := protoSingularFromArrow(target.NewElement, field, itemField, item)
			if err != nil {
				return err
			}
			target.Append(listValue)
		}
		return nil
	}
	converted, err := protoSingularFromArrow(func() protoreflect.Value {
		return message.NewField(field)
	}, field, arrowField, value)
	if err != nil {
		return err
	}
	message.Set(field, converted)
	return nil
}

func protoSingularFromArrow(newValue func() protoreflect.Value, field protoreflect.FieldDescriptor, arrowField arrow.Field, value any) (protoreflect.Value, error) {
	if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
		container := newValue()
		if err := protoPopulateMessage(container.Message(), arrowField, value); err != nil {
			return protoreflect.Value{}, err
		}
		return container, nil
	}
	return protoScalarFromArrow(value, field)
}

func protoPopulateMessage(message protoreflect.Message, arrowField arrow.Field, value any) error {
	if metadataValue(arrowField, MetadataJSON) == "true" {
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("Arrow JSON value for column %s is not text: %T", arrowField.Name, value)
		}
		if err := protojson.Unmarshal([]byte(text), message.Interface()); err != nil {
			return fmt.Errorf("cannot parse %s JSON text in column %s: %w",
				metadataValue(arrowField, MetadataProtoMessage), arrowField.Name, err)
		}
		return nil
	}
	if metadataValue(arrowField, MetadataProtoWrapper) != "" {
		valueField := message.Descriptor().Fields().ByName("value")
		converted, err := protoScalarFromArrow(value, valueField)
		if err != nil {
			return err
		}
		message.Set(valueField, converted)
		return nil
	}
	if _, ok := arrowField.Type.(*arrow.TimestampType); ok {
		micros, err := toInt64(value)
		if err != nil {
			return err
		}
		descriptor := message.Descriptor()
		message.Set(descriptor.Fields().ByName("seconds"),
			protoreflect.ValueOfInt64(floorDiv(micros, 1_000_000)))
		message.Set(descriptor.Fields().ByName("nanos"),
			protoreflect.ValueOfInt32(int32(floorMod(micros, 1_000_000)*1_000)))
		return nil
	}
	struct_, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("Arrow value for column %s is not a struct: %T", arrowField.Name, value)
	}
	structType := arrowField.Type.(*arrow.StructType)
	messageFields := message.Descriptor().Fields()
	for index := range structType.NumFields() {
		child := structType.Field(index)
		if err := protoSetColumn(message, messageFields.Get(index), child, struct_[child.Name]); err != nil {
			return err
		}
	}
	return nil
}

func protoScalarFromArrow(value any, field protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		number, err := toInt64(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt32(int32(number)), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		number, err := toInt64(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfUint32(uint32(number)), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		number, err := toInt64(value)
		if err != nil {
			return protoreflect.Value{}, err
		}
		return protoreflect.ValueOfInt64(number), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		switch typed := value.(type) {
		case uint64:
			return protoreflect.ValueOfUint64(typed), nil
		case *big.Int:
			return protoreflect.ValueOfUint64(typed.Uint64()), nil
		default:
			number, err := toInt64(value)
			if err != nil {
				return protoreflect.Value{}, err
			}
			return protoreflect.ValueOfUint64(uint64(number)), nil
		}
	case protoreflect.FloatKind:
		switch typed := value.(type) {
		case float32:
			return protoreflect.ValueOfFloat32(typed), nil
		case float64:
			return protoreflect.ValueOfFloat32(float32(typed)), nil
		}
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to a float", value)
	case protoreflect.DoubleKind:
		switch typed := value.(type) {
		case float32:
			return protoreflect.ValueOfFloat64(float64(typed)), nil
		case float64:
			return protoreflect.ValueOfFloat64(typed), nil
		}
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to a double", value)
	case protoreflect.BoolKind:
		flag, ok := value.(bool)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("cannot convert %T to a bool", value)
		}
		return protoreflect.ValueOfBool(flag), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(stringOf(value)), nil
	case protoreflect.BytesKind:
		data, ok := value.([]byte)
		if !ok {
			return protoreflect.Value{}, fmt.Errorf("cannot convert %T to bytes", value)
		}
		return protoreflect.ValueOfBytes(data), nil
	case protoreflect.EnumKind:
		return protoEnumValue(stringOf(value), field.Enum())
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to %s", value, field.Kind())
	}
}

func protoEnumValue(symbol string, enum protoreflect.EnumDescriptor) (protoreflect.Value, error) {
	if symbol != "" && (symbol[0] == '-' || (symbol[0] >= '0' && symbol[0] <= '9')) {
		if number, err := strconv.Atoi(symbol); err == nil {
			return protoreflect.ValueOfEnum(protoreflect.EnumNumber(number)), nil
		}
	}
	value := enum.Values().ByName(protoreflect.Name(symbol))
	if value == nil {
		return protoreflect.Value{}, fmt.Errorf("enum %s has no value %s", enum.FullName(), symbol)
	}
	return protoreflect.ValueOfEnum(value.Number()), nil
}

func protoMapKey(key string, field protoreflect.FieldDescriptor) (protoreflect.MapKey, error) {
	switch field.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(key).MapKey(), nil
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(key == "true").MapKey(), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		number, err := strconv.ParseInt(key, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfInt32(int32(number)).MapKey(), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		number, err := strconv.ParseInt(key, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfInt64(number).MapKey(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		number, err := strconv.ParseUint(key, 10, 32)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfUint32(uint32(number)).MapKey(), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		number, err := strconv.ParseUint(key, 10, 64)
		if err != nil {
			return protoreflect.MapKey{}, err
		}
		return protoreflect.ValueOfUint64(number).MapKey(), nil
	default:
		return protoreflect.MapKey{}, fmt.Errorf("unsupported map key kind %s", field.Kind())
	}
}

func mapItemField(field arrow.Field) arrow.Field {
	mapType := field.Type.(*arrow.MapType)
	return arrow.Field{Name: "value", Type: mapType.ItemType(), Nullable: true}
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if value%divisor != 0 && (value < 0) != (divisor < 0) {
		quotient--
	}
	return quotient
}

func floorMod(value, divisor int64) int64 {
	return value - floorDiv(value, divisor)*divisor
}
