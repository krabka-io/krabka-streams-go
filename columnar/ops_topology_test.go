package columnar

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"math"
)

func TestFiltersSelectsAndAddsColumns(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"a", "a", "b"}, []int64{5, 3, 9})
	defer payload.Release()
	batch := annotate(t, payload, mem,
		rowMetadata{timestamp: 1, offset: 0},
		rowMetadata{timestamp: 2, offset: 1},
		rowMetadata{timestamp: 3, offset: 2})
	defer batch.Release()

	filtered := runOp(t, Filter(mem, func(root arrow.Record, row int) bool {
		return int64Column(t, root, "amount").Value(row) > 4
	}), batch)
	defer filtered.Release()
	if filtered.NumRows() != 2 {
		t.Fatalf("unexpected filtered rows %d", filtered.NumRows())
	}
	if filtered.Schema().Field(2).Name != KeyColumn {
		t.Fatal("metadata columns must travel with the rows")
	}

	selected := runOp(t, Select(mem, "user"), filtered)
	defer selected.Release()
	if selected.NumCols() != 6 {
		t.Fatalf("unexpected selected columns %d", selected.NumCols())
	}
	if columnByName(selected, "amount") != nil {
		t.Fatal("unselected payload columns must be dropped")
	}

	doubleAmount, err := WithColumns(mem, DerivedColumn{
		Field: arrow.Field{Name: "double_amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		Value: func(root arrow.Record, row int) (any, error) {
			return int64Column(t, root, "amount").Value(row) * 2, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	doubled := runOp(t, doubleAmount, filtered)
	defer doubled.Release()
	if int64Column(t, doubled, "double_amount").Value(0) != 10 ||
		int64Column(t, doubled, "double_amount").Value(1) != 18 {
		t.Fatal("unexpected derived values")
	}
}

func TestWithColumnsRejectsReservedNames(t *testing.T) {
	_, err := WithColumns(memory.NewGoAllocator(), DerivedColumn{
		Field: arrow.Field{Name: KeyColumn, Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		Value: func(arrow.Record, int) (any, error) { return nil, nil },
	})
	if err == nil || !strings.Contains(err.Error(), "reserved metadata column") {
		t.Fatalf("expected a reserved-name error, got %v", err)
	}
}

func TestGroupsAndAggregatesWithinOneBatch(t *testing.T) {
	mem := checkedAllocator(t)
	batch := transactions(mem, []string{"a", "a", "b"}, []int64{5, 3, 9})
	defer batch.Release()

	grouped := runOp(t, GroupBy(mem, []string{"user"},
		Aggregation{InputColumn: "amount", OutputColumn: "total", Function: Sum},
		Aggregation{InputColumn: "amount", OutputColumn: "count", Function: Count}), batch)
	defer grouped.Release()

	if grouped.NumRows() != 2 {
		t.Fatalf("unexpected group count %d", grouped.NumRows())
	}
	users := stringColumn(t, grouped, "user")
	totals := int64Column(t, grouped, "total")
	counts := int64Column(t, grouped, "count")
	if users.Value(0) != "a" || totals.Value(0) != 8 || counts.Value(0) != 2 {
		t.Fatalf("unexpected first group %s %d %d", users.Value(0), totals.Value(0), counts.Value(0))
	}
	if users.Value(1) != "b" || totals.Value(1) != 9 {
		t.Fatal("unexpected second group")
	}
}

func TestKeepsAggregateStateAcrossBatchesAndDetectsOverflow(t *testing.T) {
	mem := checkedAllocator(t)
	first := transactions(mem, []string{"a"}, []int64{5})
	defer first.Release()
	second := transactions(mem, []string{"a"}, []int64{3})
	defer second.Release()
	operator := GroupBy(mem, []string{"user"},
		Aggregation{InputColumn: "amount", OutputColumn: "total", Function: Sum})

	initial := runOp(t, operator, first)
	defer initial.Release()
	result := runOp(t, operator, second)
	defer result.Release()
	if int64Column(t, initial, "total").Value(0) != 5 {
		t.Fatal("unexpected initial total")
	}
	if int64Column(t, result, "total").Value(0) != 8 {
		t.Fatal("state must accumulate across batches")
	}
	fresh := runOp(t, operator.fresh(), second)
	defer fresh.Release()
	if int64Column(t, fresh, "total").Value(0) != 3 {
		t.Fatal("a fresh instance must start clean")
	}

	max := transactions(mem, []string{"a"}, []int64{math.MaxInt64})
	defer max.Release()
	one := transactions(mem, []string{"a"}, []int64{1})
	defer one.Release()
	overflowing := GroupBy(mem, []string{"user"},
		Aggregation{InputColumn: "amount", OutputColumn: "total", Function: Sum})
	atMax := runOp(t, overflowing, max)
	defer atMax.Release()
	if int64Column(t, atMax, "total").Value(0) != math.MaxInt64 {
		t.Fatal("unexpected max total")
	}
	ctx := &Context{}
	if err := overflowing.Process(ctx, one); err == nil ||
		!strings.Contains(err.Error(), "overflow") {
		t.Fatalf("expected an overflow error, got %v", err)
	}
}

func TestRunsReusableTopologyAndFanOut(t *testing.T) {
	mem := checkedAllocator(t)
	codec := NewBlobCodec(mem)
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := topology.AddOperator("filter", Filter(mem, func(batch arrow.Record, row int) bool {
		return int64Column(t, batch, "amount").Value(row) > 4
	}), source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("first", "out-a", codec, filter); err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("second", "out-b", codec, filter); err != nil {
		t.Fatal(err)
	}
	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}

	payload := transactions(mem, []string{"a", "b", "c"}, []int64{1, 5, 9})
	defer payload.Release()
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	input := []ConsumedRecord{NewConsumedRecord(nil, data, 7, 0, 0)}

	firstRun, err := built.RunBatch("in", input)
	if err != nil {
		t.Fatal(err)
	}
	secondRun, err := built.RunBatch("in", input)
	if err != nil {
		t.Fatal(err)
	}

	topics := []string{firstRun[0].Topic, firstRun[1].Topic}
	if !reflect.DeepEqual(topics, []string{"out-a", "out-b"}) {
		t.Fatalf("unexpected fan-out topics %v", topics)
	}
	if len(secondRun) != 2 {
		t.Fatal("the built topology must be reusable")
	}
	decoded, err := NewIPCSerde(mem).Deserialize("", firstRun[0].Record.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()
	if decoded.NumRows() != 2 {
		t.Fatalf("unexpected filtered rows %d", decoded.NumRows())
	}
	other, err := built.RunBatch("other", input)
	if err != nil {
		t.Fatal(err)
	}
	if len(other) != 0 {
		t.Fatal("an undeclared topic must produce nothing")
	}
}

func TestValidatesSourcesSinksAndNames(t *testing.T) {
	mem := checkedAllocator(t)
	if _, err := NewTopology(mem).Build(); err == nil {
		t.Fatal("an empty topology must not build")
	}

	topology := NewTopology(mem)
	source, err := topology.AddSource("same", []string{"in"}, NewBlobCodec(mem))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("same", "out", NewBlobCodec(mem), source); err != nil {
		t.Fatal(err)
	}
	if _, err := topology.Build(); err == nil ||
		!strings.Contains(err.Error(), "duplicate node name `same`") {
		t.Fatalf("expected a duplicate-name error, got %v", err)
	}

	other := NewTopology(mem)
	foreignSource, err := other.AddSource("foreign", []string{"in"}, NewBlobCodec(mem))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("foreign-sink", "out", NewBlobCodec(mem), foreignSource); err == nil ||
		!strings.Contains(err.Error(), "parent is not a node in this topology") {
		t.Fatalf("expected a foreign-parent error, got %v", err)
	}
}

func TestMergesBranchesAndPassesSourceBytesThrough(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"a"}, []int64{1})
	defer payload.Release()
	codec := NewBlobCodec(mem)
	topology := NewTopology(mem)
	left, err := topology.AddSource("left", []string{"in-left"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	right, err := topology.AddSource("right", []string{"in-right"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := topology.AddMerge("merge", []Node{left, right})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, merged); err != nil {
		t.Fatal(err)
	}
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	output, err := built.RunBatches(map[string][]ConsumedRecord{
		"in-left":  {NewConsumedRecord(nil, data, 1, 0, 0)},
		"in-right": {NewConsumedRecord(nil, data, 1, 0, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := NewIPCSerde(mem).Deserialize("", output[0].Record.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.NumRows() != 2 {
		t.Fatalf("merge must concatenate, got %d rows", result.NumRows())
	}

	passthrough := NewTopology(mem)
	raw, err := passthrough.AddSource("raw", []string{"raw-in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := passthrough.AddPassThroughSink("copy", "raw-out", raw); err != nil {
		t.Fatal(err)
	}
	builtPassthrough, err := passthrough.Build()
	if err != nil {
		t.Fatal(err)
	}
	malformed := []byte{1, 2, 3}
	copied, err := builtPassthrough.RunBatch("raw-in", []ConsumedRecord{
		NewConsumedRecord(nil, malformed, 4, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(copied[0].Record.Value, malformed) {
		t.Fatal("pass-through must copy bytes untouched")
	}
}
