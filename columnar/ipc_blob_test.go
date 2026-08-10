package columnar

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestIPCRoundTripsArrowStream(t *testing.T) {
	mem := checkedAllocator(t)
	batch := transactions(mem, []string{"a", "b"}, []int64{1, 2})
	defer batch.Release()
	serde := NewIPCSerde(mem)

	data, err := serde.Serialize("orders", batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()

	if !decoded.Schema().Equal(batch.Schema()) {
		t.Fatal("schema did not round trip")
	}
	if decoded.NumRows() != 2 {
		t.Fatalf("unexpected row count %d", decoded.NumRows())
	}
	if int64Column(t, decoded, "amount").Value(1) != 2 {
		t.Fatal("unexpected amount value")
	}
}

func TestIPCRejectsGarbage(t *testing.T) {
	serde := NewIPCSerde(checkedAllocator(t))

	if _, err := serde.Deserialize("orders", []byte{1, 2, 3}); err == nil {
		t.Fatal("expected an error for garbage bytes")
	}
}

func TestBlobStacksRecordsAndAddsMetadata(t *testing.T) {
	mem := checkedAllocator(t)
	first := transactions(mem, []string{"a", "a"}, []int64{1, 2})
	defer first.Release()
	second := transactions(mem, []string{"b"}, []int64{3})
	defer second.Release()
	serde := NewIPCSerde(mem)
	codec := NewBlobCodec(mem)
	firstBytes, _ := serde.Serialize("", first)
	secondBytes, _ := serde.Serialize("", second)
	records := []ConsumedRecord{
		NewConsumedRecord(nil, firstBytes, 10, 0, 5),
		NewConsumedRecord([]byte{7}, secondBytes, 11, 0, 6),
	}

	decoded, err := codec.Decode("in", records)
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()

	if decoded.NumRows() != 3 {
		t.Fatalf("unexpected row count %d", decoded.NumRows())
	}
	if int64Column(t, decoded, OffsetColumn).Value(2) != 6 {
		t.Fatal("unexpected offset")
	}
	if int64Column(t, decoded, TimestampColumn).Value(2) != 11 {
		t.Fatal("unexpected timestamp")
	}

	output, err := codec.Encode("out", decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 2 {
		t.Fatalf("unexpected output size %d", len(output))
	}
	if output[0].Timestamp != 10 || output[1].Timestamp != 11 {
		t.Fatal("unexpected timestamps")
	}
	firstPayload, err := serde.Deserialize("", output[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer firstPayload.Release()
	secondPayload, err := serde.Deserialize("", output[1].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer secondPayload.Release()
	if firstPayload.NumRows() != 2 || secondPayload.NumRows() != 1 {
		t.Fatal("unexpected payload row counts")
	}
	if firstPayload.NumCols() != 2 {
		t.Fatal("metadata columns must be dropped at encode")
	}
}

func TestBlobEscapesReservedPayloadAndRejectsEmptyInput(t *testing.T) {
	mem := checkedAllocator(t)
	codec := NewBlobCodec(mem)

	if _, err := codec.Decode("in", nil); err == nil {
		t.Fatal("expected an error for an empty record batch")
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: KeyColumn, Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	builder.Field(0).AppendNull()
	bad := builder.NewRecordBatch()
	builder.Release()
	defer bad.Release()
	data, err := NewIPCSerde(mem).Serialize("", bad)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := codec.Decode("in", []ConsumedRecord{NewConsumedRecord(nil, data, 0, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()
	if columnByName(decoded, PayloadColumn(KeyColumn)) == nil {
		t.Fatal("colliding payload column must be escaped")
	}
	output, err := codec.Encode("out", decoded)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewIPCSerde(mem).Deserialize("", output[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Release()
	if columnByName(restored, KeyColumn) == nil {
		t.Fatal("original payload name must be restored")
	}
}

func TestBlobSplitsOutputAtSoftCap(t *testing.T) {
	mem := checkedAllocator(t)
	users := make([]string, 64)
	amounts := make([]int64, 64)
	for row := range users {
		users[row] = fmt.Sprintf("user-%d-%s", row, strings.Repeat("x", 32))
		amounts[row] = int64(row)
	}
	payload := transactions(mem, users, amounts)
	defer payload.Release()
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	codec, err := NewBlobCodecWithMaxBytes(mem, 1024)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := codec.Decode("in", []ConsumedRecord{NewConsumedRecord(nil, data, 0, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()
	output, err := codec.Encode("out", decoded)
	if err != nil {
		t.Fatal(err)
	}

	if len(output) <= 1 {
		t.Fatalf("expected the output to split, got %d records", len(output))
	}
	for _, record := range output {
		if len(record.Value) > 1024 {
			t.Fatalf("record exceeds the cap: %d bytes", len(record.Value))
		}
	}
}

func TestBlobRejectsASingleRowOverTheHardCap(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"too-large"}, []int64{1})
	defer payload.Release()
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	relaxed := NewBlobCodec(mem)
	tight, err := NewBlobCodecWithMaxBytes(mem, 1)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := relaxed.Decode("in", []ConsumedRecord{NewConsumedRecord(nil, data, 0, 0, 0)})
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()

	if _, err := tight.Encode("out", decoded); err == nil ||
		!strings.Contains(err.Error(), "maxRecordBytes=1") {
		t.Fatalf("expected a hard-cap error, got %v", err)
	}
}

func TestGzipAndHeadersRoundTripThroughAnExistingCodec(t *testing.T) {
	mem := checkedAllocator(t)
	raw := NewRowCodec[string](stringSerde{}, NewJSONRowBridge[string](), mem)
	gzipCodec := NewGzipBatchCodec(raw)
	header := RecordHeader{Key: "trace-id", Value: []byte("abc")}
	input := NewConsumedRecord([]byte("k"), []byte("hello"), 7, 0, 4, header)

	decoded, err := raw.Decode("in", []ConsumedRecord{input})
	if err != nil {
		t.Fatal(err)
	}
	defer decoded.Release()
	compressed, err := gzipCodec.Encode("compressed", decoded)
	if err != nil {
		t.Fatal(err)
	}

	tight, err := NewGzipBatchCodecWithMaxBytes(raw, 4)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tight.Decode("compressed", []ConsumedRecord{
		NewConsumedRecord(compressed[0].Key, compressed[0].Value, 7, 0, 4, compressed[0].Headers...)})
	if err == nil || !strings.Contains(err.Error(), "maxUncompressedBytes=4") {
		t.Fatalf("expected a ceiling error, got %v", err)
	}

	inflated, err := gzipCodec.Decode("compressed", []ConsumedRecord{
		NewConsumedRecord(compressed[0].Key, compressed[0].Value, compressed[0].Timestamp, 0, 4, compressed[0].Headers...)})
	if err != nil {
		t.Fatal(err)
	}
	defer inflated.Release()
	output, err := raw.Encode("out", inflated)
	if err != nil {
		t.Fatal(err)
	}
	expected := NewProduceRecord([]byte("k"), []byte("hello"), 7, header)
	if !output[0].Equal(expected) {
		t.Fatalf("unexpected round trip %+v", output[0])
	}
}

// stringSerde is a plain pass-through value serde for tests.
type stringSerde struct{}

func (stringSerde) Serialize(topic string, value string) ([]byte, error) {
	return []byte(value), nil
}

func (stringSerde) Deserialize(topic string, data []byte) (string, error) {
	return string(data), nil
}

var _ = bytes.Equal
