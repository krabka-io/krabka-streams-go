package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Reserved metadata column names. Every decoded batch carries these five
// columns, appended after the payload columns in this order.
const (
	// KeyColumn holds the record key bytes; null for a keyless record.
	KeyColumn = "__key"

	// TimestampColumn holds the record timestamp as a signed 64-bit integer.
	TimestampColumn = "__timestamp"

	// PartitionColumn holds the source partition as a signed 32-bit integer.
	PartitionColumn = "__partition"

	// OffsetColumn holds the source offset as a signed 64-bit integer.
	OffsetColumn = "__offset"

	// HeadersColumn holds the ordered Kafka headers, including null values.
	HeadersColumn = "__headers"
)

var reservedColumns = []string{KeyColumn, TimestampColumn, PartitionColumn, OffsetColumn, HeadersColumn}

const payloadNameMetadata = "krabka.payload.name"
const payloadPrefix = "__payload_"

// PayloadColumn returns the escaped name a payload column receives in the
// processing batch when its name collides with a reserved metadata column.
// Non-colliding names are returned unchanged.
func PayloadColumn(name string) string {
	if isReserved(name) {
		return payloadPrefix + name
	}
	return name
}

func isReserved(name string) bool {
	return slices.Contains(reservedColumns, name)
}

func rejectReservedPayloadColumns(names []string) error {
	for _, name := range names {
		if isReserved(name) {
			return fmt.Errorf("payload column `%s` collides with a reserved metadata column", name)
		}
	}
	return nil
}

func metadataFields() []arrow.Field {
	return []arrow.Field{
		{Name: KeyColumn, Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: TimestampColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: PartitionColumn, Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: OffsetColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: HeadersColumn, Type: arrow.BinaryTypes.Binary, Nullable: true},
	}
}

type rowMetadata struct {
	key       []byte
	timestamp int64
	partition int
	offset    int64
	headers   []RecordHeader
}

// withMetadata appends the five reserved metadata columns to a payload batch.
// Payload columns are reused zero-copy; colliding payload names are escaped.
func withMetadata(payload arrow.Record, metadata []rowMetadata, mem memory.Allocator) (arrow.Record, error) {
	if int(payload.NumRows()) != len(metadata) {
		return nil, fmt.Errorf("payload and metadata row counts differ")
	}
	fields := make([]arrow.Field, 0, payload.Schema().NumFields()+5)
	columns := make([]arrow.Array, 0, cap(fields))
	for i := range payload.Schema().NumFields() {
		fields = append(fields, escapedPayloadField(payload.Schema().Field(i)))
		columns = append(columns, payload.Column(i))
	}
	fields = append(fields, metadataFields()...)

	keyBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	timestampBuilder := array.NewInt64Builder(mem)
	partitionBuilder := array.NewInt32Builder(mem)
	offsetBuilder := array.NewInt64Builder(mem)
	headersBuilder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	builders := []array.Builder{keyBuilder, timestampBuilder, partitionBuilder, offsetBuilder, headersBuilder}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()
	for _, row := range metadata {
		if row.key == nil {
			keyBuilder.AppendNull()
		} else {
			keyBuilder.Append(row.key)
		}
		timestampBuilder.Append(row.timestamp)
		partitionBuilder.Append(int32(row.partition))
		offsetBuilder.Append(row.offset)
		headersBuilder.Append(encodeHeaders(row.headers))
	}
	built := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		built[i] = builder.NewArray()
	}
	defer func() {
		for _, arr := range built {
			arr.Release()
		}
	}()
	columns = append(columns, built...)
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, payload.NumRows()), nil
}

// concatenate merges same-schema batches into one, preserving order.
func concatenate(batches []arrow.Record, mem memory.Allocator) (arrow.Record, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("cannot concatenate an empty batch list")
	}
	schema := batches[0].Schema()
	rows := int64(0)
	for _, batch := range batches {
		if !schema.Equal(batch.Schema()) {
			return nil, fmt.Errorf("Arrow batch schemas differ")
		}
		rows += batch.NumRows()
	}
	columns := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for column := range columns {
		parts := make([]arrow.Array, len(batches))
		for i, batch := range batches {
			parts[i] = batch.Column(column)
		}
		merged, err := array.Concatenate(parts, mem)
		if err != nil {
			return nil, fmt.Errorf("cannot concatenate Arrow batches: %w", err)
		}
		columns[column] = merged
	}
	return array.NewRecordBatch(schema, columns, rows), nil
}

// payloadOnly drops the reserved metadata columns and restores escaped
// payload names, reusing the payload arrays zero-copy.
func payloadOnly(root arrow.Record) arrow.Record {
	var fields []arrow.Field
	var columns []arrow.Array
	for i := range root.Schema().NumFields() {
		field := root.Schema().Field(i)
		if isReserved(field.Name) {
			continue
		}
		fields = append(fields, restoredPayloadField(field))
		columns = append(columns, root.Column(i))
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, root.NumRows())
}

// selectColumns keeps the named columns in the order given, zero-copy.
// Duplicate names are ignored after the first.
func selectColumns(root arrow.Record, names []string) (arrow.Record, error) {
	seen := map[string]bool{}
	var fields []arrow.Field
	var columns []arrow.Array
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		indices := root.Schema().FieldIndices(name)
		if len(indices) == 0 {
			return nil, fmt.Errorf("Arrow column does not exist: %s", name)
		}
		fields = append(fields, root.Schema().Field(indices[0]))
		columns = append(columns, root.Column(indices[0]))
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, root.NumRows()), nil
}

// columnByName returns the named column, or nil when absent.
func columnByName(root arrow.Record, name string) arrow.Array {
	indices := root.Schema().FieldIndices(name)
	if len(indices) == 0 {
		return nil
	}
	return root.Column(indices[0])
}

// copyRows copies the given source rows, in order, into a new batch with the
// same schema.
func copyRows(root arrow.Record, rows []int, mem memory.Allocator) (arrow.Record, error) {
	builder := array.NewRecordBuilder(mem, root.Schema())
	defer builder.Release()
	for column := range int(root.NumCols()) {
		source := root.Column(column)
		target := builder.Field(column)
		for _, row := range rows {
			if err := copyValue(target, source, row); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// copyRange copies rows [start, start+length) zero-copy as a record slice.
func copyRange(root arrow.Record, start, length int) arrow.Record {
	return root.NewSlice(int64(start), int64(start+length))
}

type rowPair struct {
	leftRow  int
	rightRow int
}

// joinRows builds a joined batch from matched row pairs. Payload columns are
// prefixed per side; metadata columns come from the left row.
func joinRows(left, right arrow.Record, pairs []rowPair, leftPrefix, rightPrefix string, mem memory.Allocator) (arrow.Record, error) {
	type sourceColumn struct {
		arr  arrow.Array
		side int // 0 left payload, 1 right payload, 2 left metadata
	}
	var fields []arrow.Field
	var sources []sourceColumn
	for i := range left.Schema().NumFields() {
		field := left.Schema().Field(i)
		if isReserved(field.Name) {
			continue
		}
		fields = append(fields, prefixedPayloadField(field, leftPrefix))
		sources = append(sources, sourceColumn{arr: left.Column(i), side: 0})
	}
	for i := range right.Schema().NumFields() {
		field := right.Schema().Field(i)
		if isReserved(field.Name) {
			continue
		}
		fields = append(fields, prefixedPayloadField(field, rightPrefix))
		sources = append(sources, sourceColumn{arr: right.Column(i), side: 1})
	}
	for _, field := range metadataFields() {
		fields = append(fields, field)
		sources = append(sources, sourceColumn{arr: columnByName(left, field.Name), side: 2})
	}
	names := map[string]bool{}
	for _, field := range fields {
		if names[field.Name] {
			return nil, fmt.Errorf("joined Arrow column name collides: %s", field.Name)
		}
		names[field.Name] = true
	}

	builder := array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil))
	defer builder.Release()
	for column, source := range sources {
		target := builder.Field(column)
		for _, pair := range pairs {
			row := pair.leftRow
			if source.side == 1 {
				row = pair.rightRow
			}
			if source.arr == nil || source.arr.IsNull(row) {
				target.AppendNull()
				continue
			}
			if err := copyValue(target, source.arr, row); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

// headersAt decodes the ordered Kafka headers stored at a row of the headers
// column, returning nil when the column is absent or null.
func headersAt(arr arrow.Array, row int) []RecordHeader {
	binaryArray, ok := arr.(*array.Binary)
	if !ok || arr == nil || arr.IsNull(row) {
		return nil
	}
	headers, err := decodeHeaders(binaryArray.Value(row))
	if err != nil {
		return nil
	}
	return headers
}

// encodeHeaders packs headers into the __headers column layout: a big-endian
// count, then per header a length-prefixed UTF-8 key and a length-prefixed
// value with -1 marking null.
func encodeHeaders(headers []RecordHeader) []byte {
	var buffer []byte
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(headers)))
	for _, header := range headers {
		key := []byte(header.Key)
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(key)))
		buffer = append(buffer, key...)
		if header.Value == nil {
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(0xffffffff))
		} else {
			buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(header.Value)))
			buffer = append(buffer, header.Value...)
		}
	}
	return buffer
}

func decodeHeaders(data []byte) ([]RecordHeader, error) {
	readInt := func() (int32, error) {
		if len(data) < 4 {
			return 0, fmt.Errorf("truncated Kafka headers")
		}
		value := int32(binary.BigEndian.Uint32(data))
		data = data[4:]
		return value, nil
	}
	readBytes := func(length int32) ([]byte, error) {
		if int32(len(data)) < length {
			return nil, fmt.Errorf("truncated Kafka headers")
		}
		value := bytes.Clone(data[:length])
		data = data[length:]
		return value, nil
	}
	count, err := readInt()
	if err != nil {
		return nil, err
	}
	if count < 0 || int(count) > len(data)/8+1 {
		return nil, fmt.Errorf("invalid Kafka header count")
	}
	result := make([]RecordHeader, 0, count)
	for range count {
		keyLength, err := readInt()
		if err != nil {
			return nil, err
		}
		if keyLength < 0 {
			return nil, fmt.Errorf("negative Kafka header key length")
		}
		key, err := readBytes(keyLength)
		if err != nil {
			return nil, err
		}
		valueLength, err := readInt()
		if err != nil {
			return nil, err
		}
		if valueLength < -1 {
			return nil, fmt.Errorf("invalid Kafka header value length")
		}
		var value []byte
		if valueLength >= 0 {
			if value, err = readBytes(valueLength); err != nil {
				return nil, err
			}
		}
		result = append(result, RecordHeader{Key: string(key), Value: value})
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("trailing bytes in Kafka headers")
	}
	return result, nil
}

func escapedPayloadField(field arrow.Field) arrow.Field {
	if !isReserved(field.Name) {
		return field
	}
	metadata := fieldMetadataMap(field)
	metadata[payloadNameMetadata] = field.Name
	return renamedField(field, PayloadColumn(field.Name), metadata)
}

func restoredPayloadField(field arrow.Field) arrow.Field {
	metadata := fieldMetadataMap(field)
	original, ok := metadata[payloadNameMetadata]
	if !ok {
		return field
	}
	delete(metadata, payloadNameMetadata)
	return renamedField(field, original, metadata)
}

func prefixedPayloadField(field arrow.Field, prefix string) arrow.Field {
	restored := restoredPayloadField(field)
	return renamedField(restored, prefix+restored.Name, fieldMetadataMap(restored))
}

func fieldMetadataMap(field arrow.Field) map[string]string {
	result := map[string]string{}
	for i, key := range field.Metadata.Keys() {
		result[key] = field.Metadata.Values()[i]
	}
	return result
}

func renamedField(field arrow.Field, name string, metadata map[string]string) arrow.Field {
	keys := slices.Sorted(maps.Keys(metadata))
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = metadata[key]
	}
	return arrow.Field{
		Name:     name,
		Type:     field.Type,
		Nullable: field.Nullable,
		Metadata: arrow.NewMetadata(keys, values),
	}
}

func fieldMetadataValue(field arrow.Field, key string) string {
	index := field.Metadata.FindKey(key)
	if index < 0 {
		return ""
	}
	return field.Metadata.Values()[index]
}

// arrowValue reads a value as a native Go type: strings, []byte copies,
// sized integers, floats, bools, *big.Rat for decimals, []any for lists,
// and map[string]any for structs and maps. Null values are nil.
func arrowValue(arr arrow.Array, row int) any {
	if arr.IsNull(row) {
		return nil
	}
	switch typed := arr.(type) {
	case *array.String:
		return typed.Value(row)
	case *array.LargeString:
		return typed.Value(row)
	case *array.Binary:
		return bytes.Clone(typed.Value(row))
	case *array.LargeBinary:
		return bytes.Clone(typed.Value(row))
	case *array.FixedSizeBinary:
		return bytes.Clone(typed.Value(row))
	case *array.Int8:
		return typed.Value(row)
	case *array.Int16:
		return typed.Value(row)
	case *array.Int32:
		return typed.Value(row)
	case *array.Int64:
		return typed.Value(row)
	case *array.Uint8:
		return typed.Value(row)
	case *array.Uint16:
		return typed.Value(row)
	case *array.Uint32:
		return typed.Value(row)
	case *array.Uint64:
		return typed.Value(row)
	case *array.Float32:
		return typed.Value(row)
	case *array.Float64:
		return typed.Value(row)
	case *array.Boolean:
		return typed.Value(row)
	case *array.Date32:
		return int32(typed.Value(row))
	case *array.Date64:
		return int64(typed.Value(row))
	case *array.Timestamp:
		return int64(typed.Value(row))
	case *array.Time32:
		return int32(typed.Value(row))
	case *array.Time64:
		return int64(typed.Value(row))
	case *array.Decimal128:
		scale := typed.DataType().(*arrow.Decimal128Type).Scale
		return decimalRat(typed.Value(row).BigInt(), scale)
	case *array.Decimal256:
		scale := typed.DataType().(*arrow.Decimal256Type).Scale
		return decimalRat(typed.Value(row).BigInt(), scale)
	case *array.List:
		start, end := typed.ValueOffsets(row)
		return listValues(typed.ListValues(), start, end)
	case *array.LargeList:
		start, end := typed.ValueOffsets(row)
		return listValues(typed.ListValues(), start, end)
	case *array.FixedSizeList:
		start, end := typed.ValueOffsets(row)
		return listValues(typed.ListValues(), start, end)
	case *array.Struct:
		structType := typed.DataType().(*arrow.StructType)
		result := make(map[string]any, typed.NumField())
		for i := range typed.NumField() {
			result[structType.Field(i).Name] = arrowValue(typed.Field(i), row)
		}
		return result
	case *array.Map:
		start, end := typed.ValueOffsets(row)
		result := make(map[string]any, end-start)
		for i := start; i < end; i++ {
			result[stringify(arrowValue(typed.Keys(), int(i)))] = arrowValue(typed.Items(), int(i))
		}
		return result
	case *array.SparseUnion:
		child := typed.ChildID(row)
		return arrowValue(typed.Field(child), row)
	case *array.DenseUnion:
		child := typed.ChildID(row)
		return arrowValue(typed.Field(child), int(typed.ValueOffset(row)))
	default:
		return stringify(arr.ValueStr(row))
	}
}

func listValues(values arrow.Array, start, end int64) []any {
	result := make([]any, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, arrowValue(values, int(i)))
	}
	return result
}

func decimalRat(unscaled *big.Int, scale int32) *big.Rat {
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).SetFrac(unscaled, denominator)
}

// copyValue appends the value at a source row to a builder of the same Arrow
// type.
func copyValue(builder array.Builder, source arrow.Array, row int) error {
	if source.IsNull(row) {
		builder.AppendNull()
		return nil
	}
	return appendGoValue(builder, arrowValue(source, row))
}

// appendGoValue appends a Go value to a builder, coercing it to the declared
// Arrow type. Nil writes a null.
func appendGoValue(builder array.Builder, value any) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}
	switch typed := builder.(type) {
	case *array.StringBuilder:
		typed.Append(stringify(value))
	case *array.LargeStringBuilder:
		typed.Append(stringify(value))
	case *array.BinaryBuilder:
		data, err := binaryValue(value)
		if err != nil {
			return err
		}
		typed.Append(data)
	case *array.FixedSizeBinaryBuilder:
		data, err := binaryValue(value)
		if err != nil {
			return err
		}
		width := typed.Type().(*arrow.FixedSizeBinaryType).ByteWidth
		if len(data) != width {
			return fmt.Errorf("fixed-size binary column requires %d bytes, got %d", width, len(data))
		}
		typed.Append(data)
	case *array.Int64Builder:
		number, err := exactInt64(value)
		if err != nil {
			return err
		}
		typed.Append(number)
	case *array.Int32Builder:
		number, err := exactIntRange(value, math.MinInt32, math.MaxInt32)
		if err != nil {
			return err
		}
		typed.Append(int32(number))
	case *array.Int16Builder:
		number, err := exactIntRange(value, math.MinInt16, math.MaxInt16)
		if err != nil {
			return err
		}
		typed.Append(int16(number))
	case *array.Int8Builder:
		number, err := exactIntRange(value, math.MinInt8, math.MaxInt8)
		if err != nil {
			return err
		}
		typed.Append(int8(number))
	case *array.Uint8Builder:
		number, err := exactUnsigned(value, 8)
		if err != nil {
			return err
		}
		typed.Append(uint8(number))
	case *array.Uint16Builder:
		number, err := exactUnsigned(value, 16)
		if err != nil {
			return err
		}
		typed.Append(uint16(number))
	case *array.Uint32Builder:
		number, err := exactUnsigned(value, 32)
		if err != nil {
			return err
		}
		typed.Append(uint32(number))
	case *array.Uint64Builder:
		number, err := exactUnsigned(value, 64)
		if err != nil {
			return err
		}
		typed.Append(number)
	case *array.Float32Builder:
		number, err := floatValue(value)
		if err != nil {
			return err
		}
		typed.Append(float32(number))
	case *array.Float64Builder:
		number, err := floatValue(value)
		if err != nil {
			return err
		}
		typed.Append(number)
	case *array.BooleanBuilder:
		flag, ok := value.(bool)
		if !ok {
			return fmt.Errorf("cannot write %T into a boolean column", value)
		}
		typed.Append(flag)
	case *array.Date32Builder:
		if moment, ok := value.(time.Time); ok {
			typed.Append(arrow.Date32FromTime(moment))
			break
		}
		number, err := exactIntRange(value, math.MinInt32, math.MaxInt32)
		if err != nil {
			return err
		}
		typed.Append(arrow.Date32(number))
	case *array.Date64Builder:
		if moment, ok := value.(time.Time); ok {
			typed.Append(arrow.Date64FromTime(moment))
			break
		}
		number, err := exactInt64(value)
		if err != nil {
			return err
		}
		typed.Append(arrow.Date64(number))
	case *array.TimestampBuilder:
		if moment, ok := value.(time.Time); ok {
			unit := typed.Type().(*arrow.TimestampType).Unit
			converted, err := arrow.TimestampFromTime(moment, unit)
			if err != nil {
				return err
			}
			typed.Append(converted)
			break
		}
		number, err := exactInt64(value)
		if err != nil {
			return err
		}
		typed.Append(arrow.Timestamp(number))
	case *array.Time32Builder:
		if elapsed, ok := value.(time.Duration); ok {
			typed.Append(arrow.Time32(elapsed / timeUnitDuration(typed.Type().(*arrow.Time32Type).Unit)))
			break
		}
		number, err := exactIntRange(value, math.MinInt32, math.MaxInt32)
		if err != nil {
			return err
		}
		typed.Append(arrow.Time32(number))
	case *array.Time64Builder:
		if elapsed, ok := value.(time.Duration); ok {
			typed.Append(arrow.Time64(elapsed / timeUnitDuration(typed.Type().(*arrow.Time64Type).Unit)))
			break
		}
		number, err := exactInt64(value)
		if err != nil {
			return err
		}
		typed.Append(arrow.Time64(number))
	case *array.Decimal128Builder:
		decimalType := typed.Type().(*arrow.Decimal128Type)
		unscaled, err := unscaledDecimal(value, decimalType.Scale)
		if err != nil {
			return err
		}
		typed.Append(decimal128.FromBigInt(unscaled))
	case *array.Decimal256Builder:
		decimalType := typed.Type().(*arrow.Decimal256Type)
		unscaled, err := unscaledDecimal(value, decimalType.Scale)
		if err != nil {
			return err
		}
		typed.Append(decimal256.FromBigInt(unscaled))
	case *array.ListBuilder:
		return appendListValue(typed, typed.ValueBuilder(), value, -1)
	case *array.LargeListBuilder:
		return appendListValue(typed, typed.ValueBuilder(), value, -1)
	case *array.FixedSizeListBuilder:
		size := typed.Type().(*arrow.FixedSizeListType).Len()
		return appendListValue(typed, typed.ValueBuilder(), value, int(size))
	case *array.StructBuilder:
		values, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("cannot write %T into a struct column", value)
		}
		structType := typed.Type().(*arrow.StructType)
		typed.Append(true)
		for i := range typed.NumField() {
			if err := appendGoValue(typed.FieldBuilder(i), values[structType.Field(i).Name]); err != nil {
				return err
			}
		}
	case *array.MapBuilder:
		values, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("cannot write %T into a map column", value)
		}
		typed.Append(true)
		for _, key := range slices.Sorted(maps.Keys(values)) {
			if err := appendGoValue(typed.KeyBuilder(), key); err != nil {
				return err
			}
			if err := appendGoValue(typed.ItemBuilder(), values[key]); err != nil {
				return err
			}
		}
	case *array.SparseUnionBuilder:
		unionType := typed.Type().(*arrow.SparseUnionType)
		child := acceptingUnionChild(unionType.Fields(), value)
		if child < 0 {
			return fmt.Errorf("no Arrow union member accepts %T", value)
		}
		typed.Append(unionType.TypeCodes()[child])
		for i := range unionType.Fields() {
			if i == child {
				if err := appendGoValue(typed.Child(i), value); err != nil {
					return err
				}
			} else {
				typed.Child(i).AppendNull()
			}
		}
	case *array.DenseUnionBuilder:
		unionType := typed.Type().(*arrow.DenseUnionType)
		child := acceptingUnionChild(unionType.Fields(), value)
		if child < 0 {
			return fmt.Errorf("no Arrow union member accepts %T", value)
		}
		typed.Append(unionType.TypeCodes()[child])
		return appendGoValue(typed.Child(child), value)
	default:
		return fmt.Errorf("cannot write Arrow type %s", builder.Type())
	}
	return nil
}

func appendListValue(builder interface{ Append(bool) }, values array.Builder, value any, requiredSize int) error {
	items, err := anySlice(value)
	if err != nil {
		return err
	}
	if requiredSize >= 0 && len(items) != requiredSize {
		return fmt.Errorf("fixed-size list requires %d values", requiredSize)
	}
	builder.Append(true)
	for _, item := range items {
		if err := appendGoValue(values, item); err != nil {
			return err
		}
	}
	return nil
}

func anySlice(value any) ([]any, error) {
	if items, ok := value.([]any); ok {
		return items, nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice && reflected.Kind() != reflect.Array {
		return nil, fmt.Errorf("cannot write %T into a list column", value)
	}
	items := make([]any, reflected.Len())
	for i := range items {
		items[i] = reflected.Index(i).Interface()
	}
	return items, nil
}

func acceptingUnionChild(fields []arrow.Field, value any) int {
	for i, field := range fields {
		if unionAccepts(field.Type, value) {
			return i
		}
	}
	return -1
}

func unionAccepts(dataType arrow.DataType, value any) bool {
	switch value.(type) {
	case string:
		return dataType.ID() == arrow.STRING || dataType.ID() == arrow.LARGE_STRING
	case []byte:
		return dataType.ID() == arrow.BINARY || dataType.ID() == arrow.LARGE_BINARY ||
			dataType.ID() == arrow.FIXED_SIZE_BINARY
	case bool:
		return dataType.ID() == arrow.BOOL
	case float32, float64:
		return dataType.ID() == arrow.FLOAT32 || dataType.ID() == arrow.FLOAT64
	case *big.Rat:
		return dataType.ID() == arrow.DECIMAL128 || dataType.ID() == arrow.DECIMAL256
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *big.Int:
		switch dataType.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			return true
		}
		return false
	case time.Time:
		return dataType.ID() == arrow.DATE32 || dataType.ID() == arrow.DATE64 ||
			dataType.ID() == arrow.TIMESTAMP
	case time.Duration:
		return dataType.ID() == arrow.TIME32 || dataType.ID() == arrow.TIME64
	case []any:
		return dataType.ID() == arrow.LIST || dataType.ID() == arrow.LARGE_LIST ||
			dataType.ID() == arrow.FIXED_SIZE_LIST
	case map[string]any:
		return dataType.ID() == arrow.STRUCT || dataType.ID() == arrow.MAP
	default:
		return false
	}
}

func stringify(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	case *big.Rat:
		return strings.TrimRight(strings.TrimRight(typed.FloatString(38), "0"), ".")
	default:
		return fmt.Sprint(typed)
	}
}

func binaryValue(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return typed, nil
	case string:
		return []byte(typed), nil
	default:
		return nil, fmt.Errorf("cannot write %T into a binary column", value)
	}
}

func exactIntRange(value any, minimum, maximum int64) (int64, error) {
	number, err := exactInt64(value)
	if err != nil {
		return 0, err
	}
	if number < minimum || number > maximum {
		return 0, fmt.Errorf("integer overflow: %d", number)
	}
	return number, nil
}

func exactInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("integer overflow: %d", typed)
		}
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("integer overflow: %d", typed)
		}
		return int64(typed), nil
	case float32:
		return exactFloatInt64(float64(typed))
	case float64:
		return exactFloatInt64(typed)
	case *big.Int:
		if !typed.IsInt64() {
			return 0, fmt.Errorf("integer overflow: %s", typed)
		}
		return typed.Int64(), nil
	case *big.Rat:
		if !typed.IsInt() || !typed.Num().IsInt64() {
			return 0, fmt.Errorf("not an integer: %s", typed)
		}
		return typed.Num().Int64(), nil
	case string:
		parsed, ok := new(big.Int).SetString(typed, 10)
		if !ok {
			return 0, fmt.Errorf("not an integer: %q", typed)
		}
		return exactInt64(parsed)
	default:
		return 0, fmt.Errorf("cannot convert %T to an integer", value)
	}
}

func exactFloatInt64(value float64) (int64, error) {
	if value != math.Trunc(value) || value < math.MinInt64 || value >= math.MaxInt64 {
		return 0, fmt.Errorf("not an integer: %v", value)
	}
	return int64(value), nil
}

func exactUnsigned(value any, bits int) (uint64, error) {
	if typed, ok := value.(uint64); ok && bits == 64 {
		return typed, nil
	}
	if typed, ok := value.(*big.Int); ok {
		if typed.Sign() < 0 || typed.BitLen() > bits {
			return 0, fmt.Errorf("unsigned %d-bit overflow: %s", bits, typed)
		}
		return typed.Uint64(), nil
	}
	number, err := exactInt64(value)
	if err != nil {
		if typed, ok := value.(uint64); ok {
			number = int64(typed)
		} else {
			return 0, err
		}
	}
	if number < 0 {
		return 0, fmt.Errorf("unsigned %d-bit overflow: %d", bits, number)
	}
	if bits < 64 && uint64(number) > uint64(1)<<bits-1 {
		return 0, fmt.Errorf("unsigned %d-bit overflow: %d", bits, number)
	}
	return uint64(number), nil
}

func floatValue(value any) (float64, error) {
	switch typed := value.(type) {
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case *big.Rat:
		result, _ := typed.Float64()
		return result, nil
	case *big.Int:
		result, _ := new(big.Float).SetInt(typed).Float64()
		return result, nil
	default:
		number, err := exactInt64(value)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %T to a float", value)
		}
		return float64(number), nil
	}
}

func unscaledDecimal(value any, scale int32) (*big.Int, error) {
	scaled := new(big.Rat)
	switch typed := value.(type) {
	case *big.Rat:
		scaled.Set(typed)
	case *big.Int:
		scaled.SetInt(typed)
	case string:
		if _, ok := scaled.SetString(typed); !ok {
			return nil, fmt.Errorf("not a decimal: %q", typed)
		}
	case float32:
		scaled.SetFloat64(float64(typed))
	case float64:
		scaled.SetFloat64(typed)
	default:
		number, err := exactInt64(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T to a decimal", value)
		}
		scaled.SetInt64(number)
	}
	factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled.Mul(scaled, new(big.Rat).SetInt(factor))
	if !scaled.IsInt() {
		return nil, fmt.Errorf("decimal value does not fit scale %d", scale)
	}
	return new(big.Int).Set(scaled.Num()), nil
}

func timeUnitDuration(unit arrow.TimeUnit) time.Duration {
	switch unit {
	case arrow.Second:
		return time.Second
	case arrow.Millisecond:
		return time.Millisecond
	case arrow.Microsecond:
		return time.Microsecond
	default:
		return time.Nanosecond
	}
}
