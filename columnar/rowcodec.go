package columnar

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ValueSerde converts one record value to and from bytes. The serdes in the
// schema package satisfy this interface.
type ValueSerde[T any] interface {
	// Serialize encodes a value for a topic.
	Serialize(topic string, value T) ([]byte, error)

	// Deserialize decodes bytes read from a topic.
	Deserialize(topic string, data []byte) (T, error)
}

// RowBridge converts between typed rows and Arrow batches. The caller owns
// the batch RowsToBatch returns and must release it.
type RowBridge[T any] interface {
	// RowsToBatch converts rows into one Arrow batch.
	RowsToBatch(rows []T, mem memory.Allocator) (arrow.Record, error)

	// BatchToRows converts an Arrow batch back into typed rows.
	BatchToRows(batch arrow.Record) ([]T, error)
}

// RowCodec is the [BatchCodec] for ordinary Kafka records that hold one value
// each, giving a columnar view of them.
//
// Decoding deserializes each record value with the supplied serde, converts
// the values into Arrow columns through a [RowBridge], and attaches metadata.
// Encoding reverses it: the payload columns become typed rows again, each row
// becomes one record, the key comes from __key, and the timestamp from
// __timestamp (or 0). Row count is preserved in both directions.
type RowCodec[T any] struct {
	valueSerde ValueSerde[T]
	bridge     RowBridge[T]
	mem        memory.Allocator
}

// NewRowCodec creates a codec over a value serde and a row bridge.
func NewRowCodec[T any](valueSerde ValueSerde[T], bridge RowBridge[T], mem memory.Allocator) *RowCodec[T] {
	return &RowCodec[T]{valueSerde: valueSerde, bridge: bridge, mem: mem}
}

// Decode implements [BatchCodec].
func (c *RowCodec[T]) Decode(topic string, records []ConsumedRecord) (arrow.Record, error) {
	values := make([]T, len(records))
	metadata := make([]rowMetadata, len(records))
	for i, record := range records {
		value, err := c.valueSerde.Deserialize(topic, record.Value)
		if err != nil {
			return nil, err
		}
		values[i] = value
		metadata[i] = rowMetadata{
			key:       record.Key,
			timestamp: record.Timestamp,
			partition: record.Partition,
			offset:    record.Offset,
			headers:   record.Headers,
		}
	}
	payload, err := c.bridge.RowsToBatch(values, c.mem)
	if err != nil {
		return nil, err
	}
	defer payload.Release()
	return withMetadata(payload, metadata, c.mem)
}

// Encode implements [BatchCodec].
func (c *RowCodec[T]) Encode(topic string, batch arrow.Record) ([]ProduceRecord, error) {
	payload := payloadOnly(batch)
	defer payload.Release()
	rows, err := c.bridge.BatchToRows(payload)
	if err != nil {
		return nil, err
	}
	keys := columnByName(batch, KeyColumn)
	timestamps := columnByName(batch, TimestampColumn)
	headersColumn := columnByName(batch, HeadersColumn)
	output := make([]ProduceRecord, 0, len(rows))
	for row, value := range rows {
		encoded, err := c.valueSerde.Serialize(topic, value)
		if err != nil {
			return nil, err
		}
		output = append(output, NewProduceRecord(
			rowKey(keys, row),
			encoded,
			rowTimestamp(timestamps, row),
			headersAt(headersColumn, row)...,
		))
	}
	return output, nil
}

func rowKey(arr arrow.Array, row int) []byte {
	keys, ok := arr.(*array.Binary)
	if !ok || keys.IsNull(row) {
		return nil
	}
	return append([]byte{}, keys.Value(row)...)
}

func rowTimestamp(arr arrow.Array, row int) int64 {
	timestamps, ok := arr.(*array.Int64)
	if !ok || timestamps.IsNull(row) {
		return 0
	}
	return timestamps.Value(row)
}
