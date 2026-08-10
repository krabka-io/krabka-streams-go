package columnarschema

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hamba/avro/v2"
)

const scalarsSchema = `{"type": "record", "name": "Scalars", "fields": [
  {"name": "text", "type": "string"},
  {"name": "small", "type": "int"},
  {"name": "big", "type": "long"},
  {"name": "single", "type": "float"},
  {"name": "wide", "type": "double"},
  {"name": "flag", "type": "boolean"},
  {"name": "raw", "type": "bytes"},
  {"name": "maybe", "type": ["null", "string"]},
  {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
  {"name": "day", "type": {"type": "int", "logicalType": "date"}},
  {"name": "clock", "type": {"type": "int", "logicalType": "time-millis"}},
  {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
  {"name": "local_at", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
]}`

const nestedSchema = `{"type": "record", "name": "Nested", "fields": [
  {"name": "child", "type": {"type": "record", "name": "Child", "fields": [
    {"name": "name", "type": "string"},
    {"name": "score", "type": ["null", "long"]}]}},
  {"name": "tags", "type": {"type": "array", "items": "string"}},
  {"name": "labels", "type": {"type": "map", "values": "long"}},
  {"name": "color", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "BLUE"]}},
  {"name": "checksum", "type": {"type": "fixed", "name": "Sum", "size": 4}},
  {"name": "either", "type": ["int", "string"]}
]}`

func checkedTestAllocator(t *testing.T) *memory.CheckedAllocator {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return mem
}

func TestRoundTripsScalarsAndLogicalTypes(t *testing.T) {
	mem := checkedTestAllocator(t)
	bridge, err := NewAvroRowBridge(avro.MustParse(scalarsSchema))
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"text":     "a",
		"small":    7,
		"big":      int64(9),
		"single":   float32(1.5),
		"wide":     2.5,
		"flag":     true,
		"raw":      []byte{1, 2},
		"maybe":    nil,
		"price":    big.NewRat(1234, 100),
		"day":      time.Unix(19_000*86_400, 0).UTC(),
		"clock":    3_600_123 * time.Millisecond,
		"at":       time.UnixMilli(1_700_000_000_123).UTC(),
		"local_at": time.UnixMicro(1_700_000_000_123_456).UTC(),
	}

	batch, err := bridge.RowsToBatch([]any{record}, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	priceIndex := batch.Schema().FieldIndices("price")[0]
	price, ok := batch.Column(priceIndex).(*array.Decimal128)
	if !ok {
		t.Fatalf("price is not a decimal column: %T", batch.Column(priceIndex))
	}
	if price.IsNull(0) {
		t.Fatal("price must not be null")
	}

	back, err := bridge.BatchToRows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != 1 {
		t.Fatalf("unexpected row count %d", len(back))
	}
	decoded := back[0].(map[string]any)
	expectedPrice := record["price"].(*big.Rat)
	if decoded["price"].(*big.Rat).Cmp(expectedPrice) != 0 {
		t.Fatalf("unexpected price %v", decoded["price"])
	}
	decoded["price"], record["price"] = nil, nil
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("unexpected round trip\n got %#v\nwant %#v", decoded, record)
	}
}

func TestRoundTripsNestedRecordsCollectionsEnumsFixedAndUnions(t *testing.T) {
	mem := checkedTestAllocator(t)
	bridge, err := NewAvroRowBridge(avro.MustParse(nestedSchema))
	if err != nil {
		t.Fatal(err)
	}
	first := map[string]any{
		"child":    map[string]any{"name": "n", "score": int64(42)},
		"tags":     []any{"x", "y"},
		"labels":   map[string]any{"k": int64(7)},
		"color":    "BLUE",
		"checksum": [4]byte{1, 2, 3, 4},
		"either":   "words",
	}
	second := map[string]any{
		"child":    map[string]any{"name": "m", "score": nil},
		"tags":     []any{},
		"labels":   map[string]any{},
		"color":    "RED",
		"checksum": [4]byte{5, 6, 7, 8},
		"either":   42,
	}

	batch, err := bridge.RowsToBatch([]any{first, second}, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	eitherIndex := batch.Schema().FieldIndices("either")[0]
	union := batch.Column(eitherIndex).(*array.Struct)
	unionType := union.DataType().(interface{ FieldIdx(string) (int, bool) })
	stringIndex, _ := unionType.FieldIdx("string")
	intIndex, _ := unionType.FieldIdx("int")
	if union.Field(stringIndex).IsNull(0) || !union.Field(intIndex).IsNull(0) {
		t.Fatal("first row must fill the string branch only")
	}
	if union.Field(intIndex).IsNull(1) {
		t.Fatal("second row must fill the int branch")
	}

	back, err := bridge.BatchToRows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back[0], any(first)) {
		t.Fatalf("unexpected first row\n got %#v\nwant %#v", back[0], first)
	}
	if !reflect.DeepEqual(back[1], any(second)) {
		t.Fatalf("unexpected second row\n got %#v\nwant %#v", back[1], second)
	}
}

func TestRoundTripsRecursiveRecordsThroughJSONFallback(t *testing.T) {
	mem := checkedTestAllocator(t)
	schema := avro.MustParse(`{"type": "record", "name": "Node", "fields": [
	  {"name": "label", "type": "string"},
	  {"name": "next", "type": ["null", "Node"]}
	]}`)
	bridge, err := NewAvroRowBridge(schema)
	if err != nil {
		t.Fatal(err)
	}
	tail := map[string]any{"label": "b", "next": nil}
	head := map[string]any{"label": "a", "next": tail}

	batch, err := bridge.RowsToBatch([]any{head}, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	back, err := bridge.BatchToRows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back[0], any(head)) {
		t.Fatalf("unexpected round trip\n got %#v\nwant %#v", back[0], head)
	}
}

func TestEmptyBatchCarriesTheFullSchemaAndNullRequiredFieldsFail(t *testing.T) {
	mem := checkedTestAllocator(t)
	bridge, err := NewAvroRowBridge(avro.MustParse(scalarsSchema))
	if err != nil {
		t.Fatal(err)
	}

	batch, err := bridge.RowsToBatch(nil, mem)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Schema().NumFields() != 13 || batch.NumRows() != 0 {
		t.Fatalf("unexpected empty batch shape %d x %d", batch.Schema().NumFields(), batch.NumRows())
	}
	batch.Release()

	_, err = bridge.RowsToBatch([]any{map[string]any{}}, mem)
	if err == nil || !strings.Contains(err.Error(), "text") {
		t.Fatalf("expected a required-field error naming the field, got %v", err)
	}
}

func TestRejectsNonRecordTopLevelSchemas(t *testing.T) {
	_, err := NewAvroRowBridge(avro.MustParse(`"string"`))

	if err == nil || !strings.Contains(err.Error(), "record") {
		t.Fatalf("expected a top-level record error, got %v", err)
	}
}

func TestBridgeExposesTheDerivedArrowSchema(t *testing.T) {
	bridge, err := NewAvroRowBridge(avro.MustParse(nestedSchema))
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, field := range bridge.ArrowSchema().Fields() {
		names = append(names, field.Name)
	}

	expected := []string{"child", "tags", "labels", "color", "checksum", "either"}
	if !reflect.DeepEqual(names, expected) {
		t.Fatalf("unexpected schema fields %v", names)
	}
}
