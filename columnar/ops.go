package columnar

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// WindowStartColumn is the epoch-millisecond window start added by windowed
// grouping.
const WindowStartColumn = "__window_start"

// WindowEndColumn is the epoch-millisecond window end added by windowed
// grouping.
const WindowEndColumn = "__window_end"

// RowPredicate decides whether a row passes a filter.
type RowPredicate func(batch arrow.Record, row int) bool

// RowValue computes a derived value for a row.
type RowValue func(batch arrow.Record, row int) (any, error)

// DerivedColumn pairs an Arrow field with the expression that fills it.
// Returned values are coerced to the declared Arrow type; nil writes a null.
type DerivedColumn struct {
	// Field declares the column name and Arrow type.
	Field arrow.Field

	// Value computes the column value for each row.
	Value RowValue
}

// AggregateFunction enumerates the built-in aggregations.
type AggregateFunction int

const (
	// Count counts rows in the group, nulls included. The output type is a
	// signed 64-bit integer.
	Count AggregateFunction = iota

	// Sum accumulates exactly for integral inputs and as float64 for
	// floating-point inputs; an all-null group yields null.
	Sum

	// Min keeps the smallest non-null value.
	Min

	// Max keeps the largest non-null value.
	Max
)

// Aggregation names an input column, an output column, and the function that
// folds one into the other. OutputType overrides the inferred output type
// when non-nil.
type Aggregation struct {
	// InputColumn is the column the aggregate reads.
	InputColumn string

	// OutputColumn is the column the aggregate writes.
	OutputColumn string

	// Function is the aggregate function.
	Function AggregateFunction

	// OutputType overrides the output Arrow type when non-nil.
	OutputType arrow.DataType
}

// BuiltinOp is a built-in [Processor] that always forwards exactly one batch.
// Use [Filter], [Select], [WithColumns], [GroupBy], or [WindowedGroupBy] to
// create one.
type BuiltinOp struct {
	factory func() builtinOperation
	op      builtinOperation
}

type builtinOperation interface {
	apply(batch arrow.Record) (arrow.Record, error)
}

type statefulOperation interface {
	builtinOperation
	snapshot() ([]byte, error)
	restore(snapshot []byte) error
}

func newBuiltinOp(factory func() builtinOperation) *BuiltinOp {
	return &BuiltinOp{factory: factory, op: factory()}
}

// Process implements [Processor].
func (o *BuiltinOp) Process(ctx *Context, batch arrow.Record) error {
	output, err := o.op.apply(batch)
	if err != nil {
		return err
	}
	ctx.Forward(output)
	return nil
}

// Snapshot implements [StatefulProcessor]. Stateless operators return nil.
func (o *BuiltinOp) Snapshot() ([]byte, error) {
	if stateful, ok := o.op.(statefulOperation); ok {
		return stateful.snapshot()
	}
	return nil, nil
}

// Restore implements [StatefulProcessor].
func (o *BuiltinOp) Restore(snapshot []byte) error {
	if stateful, ok := o.op.(statefulOperation); ok {
		return stateful.restore(snapshot)
	}
	if len(snapshot) != 0 {
		return fmt.Errorf("cannot restore state into a stateless operator")
	}
	return nil
}

// fresh creates an independent operator instance with fresh state.
func (o *BuiltinOp) fresh() *BuiltinOp {
	return newBuiltinOp(o.factory)
}

// Filter keeps the rows the predicate passes, in their original order, with
// the same schema. Metadata columns travel with the rows.
func Filter(mem memory.Allocator, predicate RowPredicate) *BuiltinOp {
	return newBuiltinOp(func() builtinOperation {
		return statelessOperation(func(batch arrow.Record) (arrow.Record, error) {
			var rows []int
			for row := 0; row < int(batch.NumRows()); row++ {
				if predicate(batch, row) {
					rows = append(rows, row)
				}
			}
			return copyRows(batch, rows, mem)
		})
	})
}

// Select keeps the named payload columns, in the order given, then appends
// whichever reserved metadata columns exist in the input. Duplicate names are
// ignored after the first; a missing name fails.
func Select(mem memory.Allocator, columns ...string) *BuiltinOp {
	requested := append([]string{}, columns...)
	return newBuiltinOp(func() builtinOperation {
		return statelessOperation(func(batch arrow.Record) (arrow.Record, error) {
			selected := append([]string{}, requested...)
			for _, reserved := range reservedColumns {
				if columnByName(batch, reserved) != nil {
					selected = append(selected, reserved)
				}
			}
			return selectColumns(batch, selected)
		})
	})
}

// WithColumns adds derived columns. A derived column whose name matches an
// existing column replaces it in place, keeping its position; a new name is
// appended after the existing columns. Reserved names are rejected here, at
// construction time.
func WithColumns(mem memory.Allocator, columns ...DerivedColumn) (*BuiltinOp, error) {
	derived := append([]DerivedColumn{}, columns...)
	names := make([]string, len(derived))
	for i, column := range derived {
		names[i] = column.Field.Name
	}
	if err := rejectReservedPayloadColumns(names); err != nil {
		return nil, err
	}
	return newBuiltinOp(func() builtinOperation {
		return statelessOperation(func(batch arrow.Record) (arrow.Record, error) {
			return applyDerivedColumns(batch, derived, mem)
		})
	}), nil
}

// GroupBy groups rows by the values of the key columns and retains those
// groups across every batch the same operator instance sees. The output is
// the current cumulative result, ordered by first appearance, with key
// columns first and aggregate columns after them. Metadata columns are
// dropped unless you group by them.
func GroupBy(mem memory.Allocator, keys []string, aggregations ...Aggregation) *BuiltinOp {
	keyColumns := append([]string{}, keys...)
	aggregates := append([]Aggregation{}, aggregations...)
	return newBuiltinOp(func() builtinOperation {
		return &groupByOperation{keys: keyColumns, aggregations: aggregates, mem: mem, streamTime: math.MinInt64}
	})
}

// WindowedGroupBy is the same cumulative aggregation split into fixed
// event-time windows. It reads __timestamp and adds __window_start and
// __window_end as epoch milliseconds. Closed windows are retained for one
// window size.
func WindowedGroupBy(mem memory.Allocator, keys []string, windowSize time.Duration, aggregations ...Aggregation) (*BuiltinOp, error) {
	return WindowedGroupByWithRetention(mem, keys, windowSize, windowSize, aggregations...)
}

// WindowedGroupByWithRetention keeps closed windows for a retention duration
// no shorter than the window size.
func WindowedGroupByWithRetention(mem memory.Allocator, keys []string, windowSize, retention time.Duration, aggregations ...Aggregation) (*BuiltinOp, error) {
	windowMillis := windowSize.Milliseconds()
	retentionMillis := retention.Milliseconds()
	if windowMillis < 1 {
		return nil, fmt.Errorf("windowSize must be at least one millisecond")
	}
	if retentionMillis < windowMillis {
		return nil, fmt.Errorf("retention must not be shorter than windowSize")
	}
	keyColumns := append([]string{}, keys...)
	aggregates := append([]Aggregation{}, aggregations...)
	return newBuiltinOp(func() builtinOperation {
		return &groupByOperation{
			keys:            keyColumns,
			aggregations:    aggregates,
			windowMillis:    windowMillis,
			retentionMillis: retentionMillis,
			mem:             mem,
			streamTime:      math.MinInt64,
		}
	}), nil
}

type statelessOperation func(batch arrow.Record) (arrow.Record, error)

func (o statelessOperation) apply(batch arrow.Record) (arrow.Record, error) { return o(batch) }

func applyDerivedColumns(batch arrow.Record, derived []DerivedColumn, mem memory.Allocator) (arrow.Record, error) {
	expressions := map[string]RowValue{}
	replacements := map[string]arrow.Field{}
	for _, column := range derived {
		expressions[column.Field.Name] = column.Value
		replacements[column.Field.Name] = column.Field
	}
	var fields []arrow.Field
	appended := map[string]bool{}
	for i := 0; i < batch.Schema().NumFields(); i++ {
		field := batch.Schema().Field(i)
		if replacement, ok := replacements[field.Name]; ok {
			fields = append(fields, replacement)
		} else {
			fields = append(fields, field)
		}
		appended[field.Name] = true
	}
	for _, column := range derived {
		if !appended[column.Field.Name] {
			fields = append(fields, column.Field)
			appended[column.Field.Name] = true
		}
	}

	columns := make([]arrow.Array, 0, len(fields))
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()
	for _, field := range fields {
		expression, ok := expressions[field.Name]
		if !ok {
			source := columnByName(batch, field.Name)
			source.Retain()
			columns = append(columns, source)
			continue
		}
		builder := array.NewBuilder(mem, field.Type)
		for row := 0; row < int(batch.NumRows()); row++ {
			value, err := expression(batch, row)
			if err != nil {
				builder.Release()
				return nil, err
			}
			if err := appendGoValue(builder, value); err != nil {
				builder.Release()
				return nil, err
			}
		}
		columns = append(columns, builder.NewArray())
		builder.Release()
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), columns, batch.NumRows()), nil
}

type groupEntry struct {
	keyParts   []any
	aggregates []any
}

type groupByOperation struct {
	keys            []string
	aggregations    []Aggregation
	windowMillis    int64 // 0 means unwindowed
	retentionMillis int64
	mem             memory.Allocator

	groups     map[string]*groupEntry
	order      []string
	streamTime int64
}

func (o *groupByOperation) apply(batch arrow.Record) (arrow.Record, error) {
	if len(o.keys) == 0 {
		return nil, fmt.Errorf("groupBy requires at least one key column")
	}
	if o.groups == nil {
		o.groups = map[string]*groupEntry{}
	}
	var timestamps *array.Int64
	if o.windowMillis > 0 {
		column := columnByName(batch, TimestampColumn)
		if column == nil {
			return nil, fmt.Errorf("Arrow column does not exist: %s", TimestampColumn)
		}
		typed, ok := column.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("Arrow column does not exist: %s", TimestampColumn)
		}
		timestamps = typed
		for row := 0; row < int(batch.NumRows()); row++ {
			if !typed.IsNull(row) && typed.Value(row) > o.streamTime {
				o.streamTime = typed.Value(row)
			}
		}
		if columnByName(batch, WindowStartColumn) != nil || columnByName(batch, WindowEndColumn) != nil {
			return nil, fmt.Errorf("window output column already exists")
		}
	}
	retainedAfter := o.pruneExpired()

	keyVectors := make([]arrow.Array, len(o.keys))
	for i, name := range o.keys {
		if keyVectors[i] = columnByName(batch, name); keyVectors[i] == nil {
			return nil, fmt.Errorf("Arrow column does not exist: %s", name)
		}
	}
	inputVectors := make([]arrow.Array, len(o.aggregations))
	for i, aggregation := range o.aggregations {
		if inputVectors[i] = columnByName(batch, aggregation.InputColumn); inputVectors[i] == nil {
			return nil, fmt.Errorf("Arrow column does not exist: %s", aggregation.InputColumn)
		}
	}

	for row := 0; row < int(batch.NumRows()); row++ {
		keyParts := make([]any, 0, len(o.keys)+2)
		for _, vector := range keyVectors {
			keyParts = append(keyParts, arrowValue(vector, row))
		}
		if o.windowMillis > 0 {
			if timestamps.IsNull(row) {
				return nil, fmt.Errorf("event timestamp is null at row %d", row)
			}
			timestamp := timestamps.Value(row)
			start := floorDiv(timestamp, o.windowMillis) * o.windowMillis
			end := start + o.windowMillis
			if end < retainedAfter {
				continue
			}
			keyParts = append(keyParts, start, end)
		}
		encoded, err := encodeKeyParts(keyParts)
		if err != nil {
			return nil, err
		}
		entry, ok := o.groups[encoded]
		if !ok {
			entry = &groupEntry{keyParts: keyParts, aggregates: make([]any, len(o.aggregations))}
			o.groups[encoded] = entry
			o.order = append(o.order, encoded)
		}
		for index, aggregation := range o.aggregations {
			next := arrowValue(inputVectors[index], row)
			accumulated, err := accumulate(entry.aggregates[index], aggregation.Function, next)
			if err != nil {
				return nil, err
			}
			entry.aggregates[index] = accumulated
		}
	}

	var fields []arrow.Field
	for _, name := range o.keys {
		index := batch.Schema().FieldIndices(name)[0]
		fields = append(fields, batch.Schema().Field(index))
	}
	if o.windowMillis > 0 {
		fields = append(fields,
			arrow.Field{Name: WindowStartColumn, Type: arrow.PrimitiveTypes.Int64},
			arrow.Field{Name: WindowEndColumn, Type: arrow.PrimitiveTypes.Int64})
	}
	for index, aggregation := range o.aggregations {
		fields = append(fields, aggregateField(aggregation, inputVectors[index].DataType()))
	}

	builder := array.NewRecordBuilder(o.mem, arrow.NewSchema(fields, nil))
	defer builder.Release()
	for _, encoded := range o.order {
		entry := o.groups[encoded]
		for column, part := range entry.keyParts {
			if err := appendGoValue(builder.Field(column), part); err != nil {
				return nil, err
			}
		}
		for index := range o.aggregations {
			value := finishedAggregate(entry.aggregates[index])
			if err := appendGoValue(builder.Field(len(entry.keyParts)+index), value); err != nil {
				return nil, err
			}
		}
	}
	return builder.NewRecordBatch(), nil
}

func (o *groupByOperation) pruneExpired() int64 {
	if o.windowMillis == 0 || o.streamTime == math.MinInt64 {
		return math.MinInt64
	}
	cutoff := saturatingSubtract(o.streamTime, o.retentionMillis)
	windowEndIndex := len(o.keys) + 1
	var kept []string
	for _, encoded := range o.order {
		entry := o.groups[encoded]
		windowEnd, _ := entry.keyParts[windowEndIndex].(int64)
		if windowEnd < cutoff {
			delete(o.groups, encoded)
		} else {
			kept = append(kept, encoded)
		}
	}
	o.order = kept
	return cutoff
}

const groupSnapshotVersion = 1

func (o *groupByOperation) snapshot() ([]byte, error) {
	var buffer []byte
	buffer = append(buffer, groupSnapshotVersion)
	buffer = binary.BigEndian.AppendUint64(buffer, uint64(o.streamTime))
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(o.order)))
	for _, encoded := range o.order {
		entry := o.groups[encoded]
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(entry.keyParts)))
		for _, part := range entry.keyParts {
			encodedPart, err := encodeValue(part)
			if err != nil {
				return nil, fmt.Errorf("cannot snapshot groupBy state: %w", err)
			}
			buffer = append(buffer, encodedPart...)
		}
		buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(entry.aggregates)))
		for _, value := range entry.aggregates {
			encodedValue, err := encodeValue(value)
			if err != nil {
				return nil, fmt.Errorf("cannot snapshot groupBy state: %w", err)
			}
			buffer = append(buffer, encodedValue...)
		}
	}
	return buffer, nil
}

func (o *groupByOperation) restore(snapshot []byte) error {
	reader := &valueReader{data: snapshot}
	version, err := reader.byte()
	if err != nil || version != groupSnapshotVersion {
		return fmt.Errorf("cannot restore groupBy state: unsupported snapshot")
	}
	streamTime, err := reader.uint64()
	if err != nil {
		return fmt.Errorf("cannot restore groupBy state: %w", err)
	}
	groupCount, err := reader.uint32()
	if err != nil {
		return fmt.Errorf("cannot restore groupBy state: %w", err)
	}
	groups := map[string]*groupEntry{}
	var order []string
	for i := uint32(0); i < groupCount; i++ {
		partCount, err := reader.uint32()
		if err != nil {
			return fmt.Errorf("cannot restore groupBy state: %w", err)
		}
		keyParts := make([]any, partCount)
		for part := range keyParts {
			if keyParts[part], err = reader.value(); err != nil {
				return fmt.Errorf("cannot restore groupBy state: %w", err)
			}
		}
		aggregateCount, err := reader.uint32()
		if err != nil {
			return fmt.Errorf("cannot restore groupBy state: %w", err)
		}
		aggregates := make([]any, aggregateCount)
		for index := range aggregates {
			if aggregates[index], err = reader.value(); err != nil {
				return fmt.Errorf("cannot restore groupBy state: %w", err)
			}
		}
		encoded, err := encodeKeyParts(keyParts)
		if err != nil {
			return fmt.Errorf("cannot restore groupBy state: %w", err)
		}
		groups[encoded] = &groupEntry{keyParts: keyParts, aggregates: aggregates}
		order = append(order, encoded)
	}
	if !reader.empty() {
		return fmt.Errorf("cannot restore groupBy state: trailing bytes")
	}
	o.streamTime = int64(streamTime)
	o.groups = groups
	o.order = order
	return nil
}

func aggregateField(aggregation Aggregation, inputType arrow.DataType) arrow.Field {
	outputType := aggregation.OutputType
	if outputType == nil {
		if aggregation.Function == Count {
			outputType = arrow.PrimitiveTypes.Int64
		} else {
			outputType = inputType
		}
	}
	return arrow.Field{Name: aggregation.OutputColumn, Type: outputType, Nullable: true}
}

// accumulate folds the next value into an aggregate accumulator.
func accumulate(current any, function AggregateFunction, next any) (any, error) {
	switch function {
	case Count:
		if current == nil {
			return int64(1), nil
		}
		count := current.(int64)
		if count == math.MaxInt64 {
			return nil, fmt.Errorf("count overflow")
		}
		return count + 1, nil
	case Sum:
		if next == nil {
			return current, nil
		}
		switch typed := next.(type) {
		case float32:
			return sumFloat(current) + float64(typed), nil
		case float64:
			return sumFloat(current) + typed, nil
		case *big.Rat:
			accumulator, ok := current.(*big.Rat)
			if !ok {
				accumulator = new(big.Rat)
			}
			return new(big.Rat).Add(accumulator, typed), nil
		default:
			number, err := exactBigInt(next)
			if err != nil {
				return nil, err
			}
			accumulator, ok := current.(*big.Int)
			if !ok {
				accumulator = new(big.Int)
			}
			return new(big.Int).Add(accumulator, number), nil
		}
	case Min, Max:
		if next == nil {
			return current, nil
		}
		if current == nil {
			return next, nil
		}
		comparison, err := compareValues(current, next)
		if err != nil {
			return nil, err
		}
		if function == Min {
			if comparison <= 0 {
				return current, nil
			}
			return next, nil
		}
		if comparison >= 0 {
			return current, nil
		}
		return next, nil
	default:
		return nil, fmt.Errorf("unknown aggregate function %d", function)
	}
}

// finishedAggregate converts accumulator-internal types to writable values.
func finishedAggregate(value any) any {
	return value
}

func sumFloat(current any) float64 {
	if number, ok := current.(float64); ok {
		return number
	}
	return 0
}

func exactBigInt(value any) (*big.Int, error) {
	if typed, ok := value.(*big.Int); ok {
		return typed, nil
	}
	if typed, ok := value.(uint64); ok {
		return new(big.Int).SetUint64(typed), nil
	}
	if typed, ok := value.(bool); ok {
		if typed {
			return big.NewInt(1), nil
		}
		return big.NewInt(0), nil
	}
	number, err := exactInt64(value)
	if err != nil {
		return nil, err
	}
	return big.NewInt(number), nil
}

// compareValues orders two values of compatible types: strings and bytes
// lexicographically, booleans false before true, and numbers numerically.
func compareValues(left, right any) (int, error) {
	if leftText, ok := left.(string); ok {
		if rightText, ok := right.(string); ok {
			return strings.Compare(leftText, rightText), nil
		}
	}
	if leftBytes, ok := left.([]byte); ok {
		if rightBytes, ok := right.([]byte); ok {
			return bytes.Compare(leftBytes, rightBytes), nil
		}
	}
	if leftFlag, ok := left.(bool); ok {
		if rightFlag, ok := right.(bool); ok {
			leftValue, rightValue := 0, 0
			if leftFlag {
				leftValue = 1
			}
			if rightFlag {
				rightValue = 1
			}
			return leftValue - rightValue, nil
		}
	}
	leftNumber, leftErr := numericRat(left)
	rightNumber, rightErr := numericRat(right)
	if leftErr != nil || rightErr != nil {
		return 0, fmt.Errorf("cannot compare %T with %T", left, right)
	}
	return leftNumber.Cmp(rightNumber), nil
}

func numericRat(value any) (*big.Rat, error) {
	switch typed := value.(type) {
	case *big.Rat:
		return typed, nil
	case *big.Int:
		return new(big.Rat).SetInt(typed), nil
	case float32:
		return new(big.Rat).SetFloat64(float64(typed)), nil
	case float64:
		return new(big.Rat).SetFloat64(typed), nil
	case uint64:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(typed)), nil
	default:
		number, err := exactInt64(value)
		if err != nil {
			return nil, err
		}
		return new(big.Rat).SetInt64(number), nil
	}
}

func floorDiv(value, divisor int64) int64 {
	quotient := value / divisor
	if (value%divisor != 0) && ((value < 0) != (divisor < 0)) {
		quotient--
	}
	return quotient
}

func saturatingSubtract(left, right int64) int64 {
	result := left - right
	if right > 0 && result > left {
		return math.MinInt64
	}
	if right < 0 && result < left {
		return math.MaxInt64
	}
	return result
}

// encodeKeyParts renders group key parts into a stable map key.
func encodeKeyParts(parts []any) (string, error) {
	var buffer []byte
	for _, part := range parts {
		encoded, err := encodeValue(part)
		if err != nil {
			return "", err
		}
		buffer = append(buffer, encoded...)
	}
	return string(buffer), nil
}

// encodeValue writes a value in the tagged binary encoding used for group
// keys and state snapshots.
func encodeValue(value any) ([]byte, error) {
	switch typed := value.(type) {
	case nil:
		return []byte{'n'}, nil
	case bool:
		if typed {
			return []byte{'t'}, nil
		}
		return []byte{'F'}, nil
	case string:
		return appendSized([]byte{'s'}, []byte(typed)), nil
	case []byte:
		return appendSized([]byte{'b'}, typed), nil
	case int, int8, int16, int32, int64:
		number, _ := exactInt64(typed)
		return binary.BigEndian.AppendUint64([]byte{'i'}, uint64(number)), nil
	case uint, uint8, uint16, uint32:
		number, _ := exactInt64(typed)
		return binary.BigEndian.AppendUint64([]byte{'i'}, uint64(number)), nil
	case uint64:
		return binary.BigEndian.AppendUint64([]byte{'u'}, typed), nil
	case float32:
		return binary.BigEndian.AppendUint64([]byte{'g'}, math.Float64bits(float64(typed))), nil
	case float64:
		return binary.BigEndian.AppendUint64([]byte{'f'}, math.Float64bits(typed)), nil
	case *big.Int:
		return appendSized([]byte{'I'}, []byte(typed.String())), nil
	case *big.Rat:
		return appendSized([]byte{'R'}, []byte(typed.RatString())), nil
	case []any:
		buffer := binary.BigEndian.AppendUint32([]byte{'L'}, uint32(len(typed)))
		for _, item := range typed {
			encoded, err := encodeValue(item)
			if err != nil {
				return nil, err
			}
			buffer = append(buffer, encoded...)
		}
		return buffer, nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buffer := binary.BigEndian.AppendUint32([]byte{'M'}, uint32(len(keys)))
		for _, key := range keys {
			buffer = appendSized(buffer, []byte(key))
			encoded, err := encodeValue(typed[key])
			if err != nil {
				return nil, err
			}
			buffer = append(buffer, encoded...)
		}
		return buffer, nil
	default:
		return nil, fmt.Errorf("cannot use %T as a group key or aggregate", value)
	}
}

func appendSized(buffer, data []byte) []byte {
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(data)))
	return append(buffer, data...)
}

type valueReader struct {
	data []byte
}

func (r *valueReader) empty() bool { return len(r.data) == 0 }

func (r *valueReader) byte() (byte, error) {
	if len(r.data) < 1 {
		return 0, fmt.Errorf("truncated snapshot")
	}
	result := r.data[0]
	r.data = r.data[1:]
	return result, nil
}

func (r *valueReader) uint32() (uint32, error) {
	if len(r.data) < 4 {
		return 0, fmt.Errorf("truncated snapshot")
	}
	result := binary.BigEndian.Uint32(r.data)
	r.data = r.data[4:]
	return result, nil
}

func (r *valueReader) uint64() (uint64, error) {
	if len(r.data) < 8 {
		return 0, fmt.Errorf("truncated snapshot")
	}
	result := binary.BigEndian.Uint64(r.data)
	r.data = r.data[8:]
	return result, nil
}

func (r *valueReader) sized() ([]byte, error) {
	length, err := r.uint32()
	if err != nil {
		return nil, err
	}
	if uint32(len(r.data)) < length {
		return nil, fmt.Errorf("truncated snapshot")
	}
	result := append([]byte{}, r.data[:length]...)
	r.data = r.data[length:]
	return result, nil
}

func (r *valueReader) value() (any, error) {
	tag, err := r.byte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 'n':
		return nil, nil
	case 't':
		return true, nil
	case 'F':
		return false, nil
	case 's':
		data, err := r.sized()
		if err != nil {
			return nil, err
		}
		return string(data), nil
	case 'b':
		return r.sized()
	case 'i':
		number, err := r.uint64()
		if err != nil {
			return nil, err
		}
		return int64(number), nil
	case 'u':
		return r.uint64()
	case 'g':
		number, err := r.uint64()
		if err != nil {
			return nil, err
		}
		return float32(math.Float64frombits(number)), nil
	case 'f':
		number, err := r.uint64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(number), nil
	case 'I':
		data, err := r.sized()
		if err != nil {
			return nil, err
		}
		result, ok := new(big.Int).SetString(string(data), 10)
		if !ok {
			return nil, fmt.Errorf("invalid integer in snapshot")
		}
		return result, nil
	case 'R':
		data, err := r.sized()
		if err != nil {
			return nil, err
		}
		result, ok := new(big.Rat).SetString(string(data))
		if !ok {
			return nil, fmt.Errorf("invalid rational in snapshot")
		}
		return result, nil
	case 'L':
		count, err := r.uint32()
		if err != nil {
			return nil, err
		}
		result := make([]any, count)
		for i := range result {
			if result[i], err = r.value(); err != nil {
				return nil, err
			}
		}
		return result, nil
	case 'M':
		count, err := r.uint32()
		if err != nil {
			return nil, err
		}
		result := make(map[string]any, count)
		for i := uint32(0); i < count; i++ {
			key, err := r.sized()
			if err != nil {
				return nil, err
			}
			if result[string(key)], err = r.value(); err != nil {
				return nil, err
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unknown snapshot value tag %q", tag)
	}
}
