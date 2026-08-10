package columnar

import (
	"reflect"
	"strings"
	"testing"
)

func TestRowCodecAssemblesAndExplodesRows(t *testing.T) {
	mem := checkedAllocator(t)
	codec := NewRowCodec[string](stringSerde{}, NewJSONRowBridge[string](), mem)
	records := []ConsumedRecord{
		NewConsumedRecord([]byte{1}, []byte("a"), 10, 0, 5),
		NewConsumedRecord([]byte{2}, []byte("b"), 11, 0, 6),
	}

	batch, err := codec.Decode("in", records)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	if batch.NumRows() != 2 {
		t.Fatalf("unexpected row count %d", batch.NumRows())
	}
	expectedColumns := []string{"value", "__key", "__timestamp", "__partition", "__offset", "__headers"}
	if !reflect.DeepEqual(columnNames(batch), expectedColumns) {
		t.Fatalf("unexpected columns %v", columnNames(batch))
	}

	output, err := codec.Encode("out", batch)
	if err != nil {
		t.Fatal(err)
	}
	expected := []ProduceRecord{
		NewProduceRecord([]byte{1}, []byte("a"), 10),
		NewProduceRecord([]byte{2}, []byte("b"), 11),
	}
	if len(output) != 2 || !output[0].Equal(expected[0]) || !output[1].Equal(expected[1]) {
		t.Fatalf("unexpected output %+v", output)
	}
}

type bridgeOrder struct {
	ID     string   `json:"id"`
	Amount int64    `json:"amount"`
	Tags   []string `json:"tags"`
}

func TestJSONBridgeRoundTripsRecordsAndNestedJSON(t *testing.T) {
	mem := checkedAllocator(t)
	bridge := NewJSONRowBridge[bridgeOrder]()
	rows := []bridgeOrder{
		{ID: "a", Amount: 5, Tags: []string{"new", "paid"}},
		{ID: "b", Amount: 7, Tags: []string{"new"}},
	}

	batch, err := bridge.RowsToBatch(rows, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	if !reflect.DeepEqual(columnNames(batch), []string{"id", "amount", "tags"}) {
		t.Fatalf("unexpected columns %v", columnNames(batch))
	}
	back, err := bridge.BatchToRows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, rows) {
		t.Fatalf("unexpected round trip %+v", back)
	}
}

type nullableSample struct {
	Value *int64 `json:"value"`
}

func TestJSONBridgeKeepsOneSchemaAcrossBatches(t *testing.T) {
	mem := checkedAllocator(t)
	bridge := NewJSONRowBridge[nullableSample]()

	first, err := bridge.RowsToBatch([]nullableSample{{Value: nil}}, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	seven := int64(7)
	second, err := bridge.RowsToBatch([]nullableSample{{Value: &seven}}, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()

	if !first.Schema().Equal(second.Schema()) {
		t.Fatal("the inferred schema must be retained across batches")
	}
	if stringColumn(t, second, "value").Value(0) != "7" {
		t.Fatal("later values must coerce into the retained Utf8 column")
	}
}

type schemaOrder struct {
	ID     *string  `json:"id"`
	Amount int64    `json:"amount"`
	Tags   []string `json:"tags"`
}

func TestJSONBridgeDerivesStableRequiredFieldsFromJSONSchema(t *testing.T) {
	mem := checkedAllocator(t)
	schema := `{"type":"object","required":["id"],"properties":{` +
		`"id":{"type":"string"},"amount":{"type":"integer"},"tags":{"type":"array"}}}`
	bridge, err := JSONRowBridgeFromJSONSchema[schemaOrder](schema)
	if err != nil {
		t.Fatal(err)
	}
	id := "a"
	rows := []schemaOrder{{ID: &id, Amount: 5, Tags: []string{"new"}}}

	batch, err := bridge.RowsToBatch(rows, mem)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Release()

	nullables := []bool{
		batch.Schema().Field(0).Nullable,
		batch.Schema().Field(1).Nullable,
		batch.Schema().Field(2).Nullable,
	}
	if !reflect.DeepEqual(nullables, []bool{false, true, true}) {
		t.Fatalf("unexpected nullability %v", nullables)
	}
	back, err := bridge.BatchToRows(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, rows) {
		t.Fatalf("unexpected round trip %+v", back)
	}

	_, err = bridge.RowsToBatch([]schemaOrder{{ID: nil, Amount: 1}}, mem)
	if err == nil || !strings.Contains(err.Error(), "required JSON field is null: id") {
		t.Fatalf("expected a required-field error, got %v", err)
	}
}

func TestJSONBridgeEnforcesScalarJSONSchemaNullability(t *testing.T) {
	mem := checkedAllocator(t)
	bridge, err := JSONRowBridgeFromJSONSchema[*string](`{"type":"string"}`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = bridge.RowsToBatch([]*string{nil}, mem)

	if err == nil || !strings.Contains(err.Error(), "required JSON field is null: value") {
		t.Fatalf("expected a required-field error, got %v", err)
	}
}
