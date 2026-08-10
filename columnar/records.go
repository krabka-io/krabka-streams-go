// Package columnar processes Kafka records as Apache Arrow batches instead of
// one record at a time.
//
// It is a separate execution model from Kafka Streams: it does not build a
// Kafka topology, but its runner can participate in a consumer group and
// retain partition-local state. It is meant for analytical work where
// per-record dispatch dominates the cost.
//
// One fetched topic-partition batch is one processing unit. Records arrive
// from a consumer poll, are decoded into a single [arrow.Record], flow through
// operators as whole batches, and are encoded back into records at a sink.
//
// Arrow memory is reference-counted. Whatever a public method returns to you,
// you release; batches inside a running topology belong to the framework.
package columnar

import "bytes"

// RecordHeader is one ordered Kafka record header. Header values may be nil,
// and keys may repeat.
type RecordHeader struct {
	// Key is the header key.
	Key string

	// Value is the header value, or nil for a null-valued header.
	Value []byte
}

// Equal reports whether two headers have the same key and value bytes.
func (h RecordHeader) Equal(other RecordHeader) bool {
	return h.Key == other.Key && bytes.Equal(h.Value, other.Value)
}

// ConsumedRecord is one record fetched from Kafka. Byte slices and header
// lists are defensively copied by [NewConsumedRecord]; consumer buffers get
// reused by Kafka clients, and Arrow-adjacent code holds references longer
// than a single callback.
type ConsumedRecord struct {
	// Key is the record key, or nil for a keyless record.
	Key []byte

	// Value is the record value. A tombstone becomes an empty slice.
	Value []byte

	// Timestamp is the record timestamp in epoch milliseconds.
	Timestamp int64

	// Partition is the source partition.
	Partition int

	// Offset is the source offset.
	Offset int64

	// Headers are the ordered Kafka record headers.
	Headers []RecordHeader
}

// NewConsumedRecord copies its arguments into an independent record.
func NewConsumedRecord(key, value []byte, timestamp int64, partition int, offset int64, headers ...RecordHeader) ConsumedRecord {
	return ConsumedRecord{
		Key:       cloneBytes(key),
		Value:     append([]byte{}, value...),
		Timestamp: timestamp,
		Partition: partition,
		Offset:    offset,
		Headers:   cloneHeaders(headers),
	}
}

// ProduceRecord is one record to be written to Kafka. Byte slices and header
// lists are defensively copied by [NewProduceRecord].
type ProduceRecord struct {
	// Key is the record key, or nil for a keyless record.
	Key []byte

	// Value is the record value.
	Value []byte

	// Timestamp is the record timestamp in epoch milliseconds. A negative
	// timestamp lets the producer apply its own.
	Timestamp int64

	// Headers are the ordered Kafka record headers.
	Headers []RecordHeader
}

// NewProduceRecord copies its arguments into an independent record.
func NewProduceRecord(key, value []byte, timestamp int64, headers ...RecordHeader) ProduceRecord {
	return ProduceRecord{
		Key:       cloneBytes(key),
		Value:     append([]byte{}, value...),
		Timestamp: timestamp,
		Headers:   cloneHeaders(headers),
	}
}

// Equal reports whether two produce records carry the same key, value,
// timestamp, and headers.
func (r ProduceRecord) Equal(other ProduceRecord) bool {
	if !bytes.Equal(r.Key, other.Key) || !bytes.Equal(r.Value, other.Value) ||
		r.Timestamp != other.Timestamp || len(r.Headers) != len(other.Headers) {
		return false
	}
	for i, header := range r.Headers {
		if !header.Equal(other.Headers[i]) {
			return false
		}
	}
	return true
}

// ProducedToTopic pairs a produced record with its sink topic.
type ProducedToTopic struct {
	// Topic is the sink topic.
	Topic string

	// Record is the produced record.
	Record ProduceRecord
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte{}, value...)
}

func cloneHeaders(headers []RecordHeader) []RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	result := make([]RecordHeader, len(headers))
	for i, header := range headers {
		result[i] = RecordHeader{Key: header.Key, Value: cloneBytes(header.Value)}
	}
	return result
}
