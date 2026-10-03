package columnar

import (
	"bytes"
	"math"
	"math/big"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestWindowsAndSnapshotsRetainEventTimeState(t *testing.T) {
	cases := []struct {
		windowMillis    int64
		expectedWindows int64
		expectedTotal   int64
	}{
		{windowMillis: 10, expectedWindows: 2, expectedTotal: 7},
		{windowMillis: 20, expectedWindows: 1, expectedTotal: 10},
	}
	for _, testCase := range cases {
		mem := checkedAllocator(t)
		payload := transactions(mem, []string{"a", "a"}, []int64{5, 3})
		batch := annotate(t, payload, mem,
			rowMetadata{timestamp: 1, offset: 0},
			rowMetadata{timestamp: 12, offset: 1})
		payload.Release()

		window := time.Duration(testCase.windowMillis) * time.Millisecond
		aggregation := Aggregation{InputColumn: "amount", OutputColumn: "total", Function: Sum}
		operator, err := WindowedGroupBy(mem, []string{"user"}, window, aggregation)
		if err != nil {
			t.Fatal(err)
		}
		initial := runOp(t, operator, batch)
		if initial.NumRows() != testCase.expectedWindows {
			t.Fatalf("window %d: unexpected window count %d", testCase.windowMillis, initial.NumRows())
		}
		initial.Release()
		batch.Release()

		restored, err := WindowedGroupBy(mem, []string{"user"}, window, aggregation)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := operator.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if err := restored.Restore(snapshot); err != nil {
			t.Fatal(err)
		}

		nextPayload := transactions(mem, []string{"a"}, []int64{2})
		next := annotate(t, nextPayload, mem, rowMetadata{timestamp: 2, offset: 2})
		nextPayload.Release()
		result := runOp(t, restored, next)
		next.Release()
		if int64Column(t, result, "total").Value(0) != testCase.expectedTotal {
			t.Fatalf("window %d: unexpected total %d", testCase.windowMillis,
				int64Column(t, result, "total").Value(0))
		}
		if int64Column(t, result, WindowStartColumn).Value(0) != 0 {
			t.Fatal("unexpected window start")
		}
		result.Release()

		farPayload := transactions(mem, []string{"a"}, []int64{1})
		far := annotate(t, farPayload, mem, rowMetadata{timestamp: 100, offset: 3})
		farPayload.Release()
		advanced := runOp(t, restored, far)
		far.Release()
		if advanced.NumRows() != 1 {
			t.Fatalf("expired windows must be pruned, got %d rows", advanced.NumRows())
		}
		advanced.Release()

		retainedPayload := transactions(mem, []string{"a"}, []int64{1})
		retained := annotate(t, retainedPayload, mem, rowMetadata{timestamp: 101, offset: 4})
		retainedPayload.Release()
		late := runOp(t, restored, retained)
		retained.Release()
		if late.NumRows() != 1 {
			t.Fatalf("the open window must be retained, got %d rows", late.NumRows())
		}
		late.Release()
	}
}

func TestJoinsAcrossBatchesAndRestoresOnlyTheMatchingPartition(t *testing.T) {
	mem := checkedAllocator(t)
	topology := NewTopology(mem)
	codec := NewBlobCodec(mem)
	left, err := topology.AddSource("left", []string{"left"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	right, err := topology.AddSource("right", []string{"right"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := topology.AddJoin("join",
		Join{LeftKey: "user", RightKey: "user", Window: 10 * time.Millisecond}, left, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, joined); err != nil {
		t.Fatal(err)
	}

	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	leftPayload := transactions(mem, []string{"a"}, []int64{5})
	leftBytes, _ := NewIPCSerde(mem).Serialize("", leftPayload)
	leftPayload.Release()
	leftOutput, err := built.RunBatch("left", []ConsumedRecord{
		NewConsumedRecord([]byte("key"), leftBytes, 100, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(leftOutput) != 0 {
		t.Fatal("an unmatched left batch must produce nothing")
	}
	snapshot, err := built.SnapshotPartition(0)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.RestorePartition(0, snapshot); err != nil {
		t.Fatal(err)
	}
	rightPayload := transactions(mem, []string{"a"}, []int64{9})
	rightBytes, _ := NewIPCSerde(mem).Serialize("", rightPayload)
	rightPayload.Release()

	otherPartition, err := restored.RunBatch("right", []ConsumedRecord{
		NewConsumedRecord(nil, rightBytes, 105, 1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(otherPartition) != 0 {
		t.Fatal("state must be isolated by partition")
	}
	output, err := restored.RunBatch("right", []ConsumedRecord{
		NewConsumedRecord(nil, rightBytes, 105, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 1 {
		t.Fatalf("expected one joined record, got %d", len(output))
	}
	result, err := NewIPCSerde(mem).Deserialize("", output[0].Record.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	expected := []string{"left_user", "left_amount", "right_user", "right_amount"}
	if !slices.Equal(columnNames(result), expected) {
		t.Fatalf("unexpected joined columns %v", columnNames(result))
	}
	if int64Column(t, result, "left_amount").Value(0) != 5 ||
		int64Column(t, result, "right_amount").Value(0) != 9 {
		t.Fatal("unexpected joined values")
	}
}

func TestFileStateStoreAtomicallyRoundTripsSnapshots(t *testing.T) {
	cases := []struct {
		name  string
		epoch int64
	}{
		{name: "outside a barrier", epoch: NoEpoch},
		{name: "at a barrier epoch", epoch: 7},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			store := NewFileStateStore(t.TempDir())
			expected := map[string][]byte{"aggregate": {1, 2, 3}}

			if err := store.Save(4, testCase.epoch, expected); err != nil {
				t.Fatal(err)
			}

			loaded, err := store.Load(4, testCase.epoch)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(loaded, expected) {
				t.Fatalf("unexpected snapshot %v", loaded)
			}
			missing, err := store.Load(5, testCase.epoch)
			if err != nil {
				t.Fatal(err)
			}
			if len(missing) != 0 {
				t.Fatal("a missing partition must load empty")
			}
			otherEpoch, err := store.Load(4, testCase.epoch+1)
			if err != nil {
				t.Fatal(err)
			}
			if len(otherEpoch) != 0 {
				t.Fatal("a snapshot must not answer for another epoch")
			}
		})
	}
}

func TestValueFacadeRoundTripsScalars(t *testing.T) {
	mem := checkedAllocator(t)
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "user", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil))
	defer builder.Release()

	if err := AppendValue(builder.Field(0), "ada"); err != nil {
		t.Fatal(err)
	}
	if err := AppendValue(builder.Field(1), int64(7)); err != nil {
		t.Fatal(err)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()

	if Value(batch.Column(0), 0) != "ada" {
		t.Fatal("unexpected string value")
	}
	if Value(batch.Column(1), 0) != int64(7) {
		t.Fatal("unexpected integer value")
	}
}

func TestValueFacadeWritesFixedSizeBinaryAndRejectsWrongLength(t *testing.T) {
	mem := checkedAllocator(t)
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "fixed", Type: &arrow.FixedSizeBinaryType{ByteWidth: 4}, Nullable: true},
	}, nil))
	defer builder.Release()

	if err := AppendValue(builder.Field(0), []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := AppendValue(builder.Field(0), []byte{1}); err == nil {
		t.Fatal("expected a wrong-length error")
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()

	if !bytes.Equal(Value(batch.Column(0), 0).([]byte), []byte{1, 2, 3, 4}) {
		t.Fatal("unexpected fixed-size binary value")
	}
}

func TestValueFacadeHandlesUnsigned64BitValuesExactly(t *testing.T) {
	mem := checkedAllocator(t)
	aboveSignedRange, _ := new(big.Int).SetString("18446744073709551615", 10)
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	}, nil))
	defer builder.Release()

	if err := AppendValue(builder.Field(0), aboveSignedRange); err != nil {
		t.Fatal(err)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()

	if Value(batch.Column(0), 0) != uint64(18446744073709551615) {
		t.Fatal("unexpected unsigned value")
	}
}

func TestValueFacadeHandlesTimeColumns(t *testing.T) {
	mem := checkedAllocator(t)
	elapsed := 13*time.Hour + 30*time.Minute + 5*time.Second + 123456789*time.Nanosecond
	builder := array.NewRecordBuilder(mem, arrow.NewSchema([]arrow.Field{
		{Name: "sec", Type: arrow.FixedWidthTypes.Time32s, Nullable: true},
		{Name: "milli", Type: arrow.FixedWidthTypes.Time32ms, Nullable: true},
		{Name: "micro", Type: arrow.FixedWidthTypes.Time64us, Nullable: true},
		{Name: "nano", Type: arrow.FixedWidthTypes.Time64ns, Nullable: true},
	}, nil))
	defer builder.Release()

	for column := range 4 {
		if err := AppendValue(builder.Field(column), elapsed); err != nil {
			t.Fatal(err)
		}
		builder.Field(column).AppendNull()
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()

	if Value(batch.Column(0), 0) != int32(elapsed/time.Second) {
		t.Fatal("unexpected seconds value")
	}
	if Value(batch.Column(1), 0) != int32(elapsed/time.Millisecond) {
		t.Fatal("unexpected milliseconds value")
	}
	if Value(batch.Column(2), 0) != int64(elapsed/time.Microsecond) {
		t.Fatal("unexpected microseconds value")
	}
	if Value(batch.Column(3), 0) != int64(elapsed) {
		t.Fatal("unexpected nanoseconds value")
	}
	for column := range 4 {
		if Value(batch.Column(column), 1) != nil {
			t.Fatal("null must read as nil")
		}
	}
}

func TestValueFacadeNumericConversions(t *testing.T) {
	cases := []struct {
		name      string
		typ       arrow.DataType
		input     any
		want      any
		wantError bool
	}{
		{"int8", arrow.PrimitiveTypes.Int8, int64(math.MinInt8), int8(math.MinInt8), false},
		{"int16", arrow.PrimitiveTypes.Int16, int64(math.MaxInt16), int16(math.MaxInt16), false},
		{"int32", arrow.PrimitiveTypes.Int32, int64(math.MinInt32), int32(math.MinInt32), false},
		{"int64", arrow.PrimitiveTypes.Int64, int64(math.MaxInt64), int64(math.MaxInt64), false},
		{"uint8", arrow.PrimitiveTypes.Uint8, uint64(math.MaxUint8), uint8(math.MaxUint8), false},
		{"uint16", arrow.PrimitiveTypes.Uint16, uint64(math.MaxUint16), uint16(math.MaxUint16), false},
		{"uint32", arrow.PrimitiveTypes.Uint32, uint64(math.MaxUint32), uint32(math.MaxUint32), false},
		{"uint64", arrow.PrimitiveTypes.Uint64, uint64(math.MaxUint64), uint64(math.MaxUint64), false},
		{"float32", arrow.PrimitiveTypes.Float32, float64(1.5), float32(1.5), false},
		{"float64", arrow.PrimitiveTypes.Float64, int32(7), float64(7), false},
		{"date32", arrow.FixedWidthTypes.Date32, int64(math.MaxInt32), int32(math.MaxInt32), false},
		{"date64", arrow.FixedWidthTypes.Date64, int64(math.MaxInt64), int64(math.MaxInt64), false},
		{"timestamp", arrow.FixedWidthTypes.Timestamp_ms, int64(42), int64(42), false},
		{"time32", arrow.FixedWidthTypes.Time32s, int64(42), int32(42), false},
		{"time64", arrow.FixedWidthTypes.Time64ns, int64(42), int64(42), false},
		{"signed overflow", arrow.PrimitiveTypes.Int8, int64(math.MaxInt8) + 1, nil, true},
		{"signed underflow", arrow.PrimitiveTypes.Int16, int64(math.MinInt16) - 1, nil, true},
		{"unsigned overflow", arrow.PrimitiveTypes.Uint8, uint64(math.MaxUint8) + 1, nil, true},
		{"negative unsigned", arrow.PrimitiveTypes.Uint64, int64(-1), nil, true},
		{"fractional integer", arrow.PrimitiveTypes.Int32, 1.5, nil, true},
		{"invalid float", arrow.PrimitiveTypes.Float32, "invalid", nil, true},
		{"date overflow", arrow.FixedWidthTypes.Date32, int64(math.MaxInt32) + 1, nil, true},
		{"null", arrow.PrimitiveTypes.Int8, nil, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			builder := array.NewBuilder(checkedAllocator(t), tc.typ)
			defer builder.Release()
			err := AppendValue(builder, tc.input)
			if tc.wantError {
				if err == nil || builder.Len() != 0 {
					t.Fatalf("invalid value must fail without appending: len=%d, err=%v", builder.Len(), err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			values := builder.NewArray()
			defer values.Release()
			if got := Value(values, 0); got != tc.want {
				t.Fatalf("got %T(%v), want %T(%v)", got, got, tc.want, tc.want)
			}
		})
	}
}
