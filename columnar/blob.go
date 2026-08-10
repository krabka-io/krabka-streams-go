package columnar

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// DefaultMaxRecordBytes is the soft cap on one encoded record, chosen to stay
// under a 1 MiB broker message limit with room for headers and framing.
const DefaultMaxRecordBytes = 900 * 1024

// BlobCodec is the [BatchCodec] for records that are already Arrow: each
// Kafka record value holds an Arrow IPC stream with many rows.
//
// Decoding reads each record value as an Arrow IPC stream, attaches metadata
// columns, and concatenates the results; every row keeps the metadata of the
// record it came from. All records in a batch must share one payload schema.
//
// Encoding drops the metadata columns and serializes the payload as Arrow IPC
// records. It packs the largest consecutive rows that fit under the byte cap
// and share one key, timestamp, and header list, then applies that envelope
// to the output record. A single row that exceeds the cap fails instead of
// sending a record the broker will reject.
type BlobCodec struct {
	mem            memory.Allocator
	serde          *IPCSerde
	maxRecordBytes int
}

// NewBlobCodec creates a codec with [DefaultMaxRecordBytes].
func NewBlobCodec(mem memory.Allocator) *BlobCodec {
	codec, _ := NewBlobCodecWithMaxBytes(mem, DefaultMaxRecordBytes)
	return codec
}

// NewBlobCodecWithMaxBytes creates a codec with a custom record byte cap.
// Raise the cap only alongside the broker's max.message.bytes.
func NewBlobCodecWithMaxBytes(mem memory.Allocator, maxRecordBytes int) (*BlobCodec, error) {
	if maxRecordBytes < 1 {
		return nil, fmt.Errorf("maxRecordBytes must be positive")
	}
	return &BlobCodec{mem: mem, serde: NewIPCSerde(mem), maxRecordBytes: maxRecordBytes}, nil
}

// Decode implements [BatchCodec].
func (c *BlobCodec) Decode(topic string, records []ConsumedRecord) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("decode called with an empty record batch")
	}
	batches := make([]arrow.Record, 0, len(records))
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	for recordIndex, record := range records {
		payload, err := c.serde.deserialize(record.Value)
		if err != nil {
			return nil, fmt.Errorf("cannot decode Arrow record %d: %w", recordIndex, err)
		}
		metadata := make([]rowMetadata, payload.NumRows())
		for row := range metadata {
			metadata[row] = rowMetadata{
				key:       record.Key,
				timestamp: record.Timestamp,
				partition: record.Partition,
				offset:    record.Offset,
				headers:   record.Headers,
			}
		}
		annotated, err := withMetadata(payload, metadata, c.mem)
		payload.Release()
		if err != nil {
			return nil, fmt.Errorf("cannot decode Arrow record %d: %w", recordIndex, err)
		}
		batches = append(batches, annotated)
	}
	return concatenate(batches, c.mem)
}

// Encode implements [BatchCodec].
func (c *BlobCodec) Encode(topic string, batch arrow.Record) ([]ProduceRecord, error) {
	payload := payloadOnly(batch)
	defer payload.Release()
	var result []ProduceRecord
	start := 0
	rows := int(payload.NumRows())
	headersColumn := columnByName(batch, HeadersColumn)
	for start < rows {
		chunkRows, chunkBytes, err := c.largestChunk(payload, start, envelopeRows(batch, start))
		if err != nil {
			return nil, err
		}
		row := start + chunkRows - 1
		result = append(result, NewProduceRecord(
			blobKey(batch, row),
			chunkBytes,
			blobTimestamp(batch, row),
			headersAt(headersColumn, row)...,
		))
		start += chunkRows
	}
	return result, nil
}

// largestChunk binary-searches the largest row count from start that fits
// under the byte cap.
func (c *BlobCodec) largestChunk(payload arrow.Record, start, availableRows int) (int, []byte, error) {
	low, high := 1, availableRows
	bestRows, bestBytes := 0, []byte(nil)
	for low <= high {
		candidateRows := low + (high-low)/2
		candidate := copyRange(payload, start, candidateRows)
		encoded, err := c.serde.serialize(candidate)
		candidate.Release()
		if err != nil {
			return 0, nil, err
		}
		if len(encoded) <= c.maxRecordBytes {
			bestRows, bestBytes = candidateRows, encoded
			low = candidateRows + 1
		} else {
			high = candidateRows - 1
		}
	}
	if bestRows == 0 {
		return 0, nil, fmt.Errorf("one Arrow row exceeds maxRecordBytes=%d", c.maxRecordBytes)
	}
	return bestRows, bestBytes, nil
}

// envelopeRows counts the consecutive rows from start that share one key,
// timestamp, and header list, so they can travel in one record envelope.
func envelopeRows(batch arrow.Record, start int) int {
	headersColumn := columnByName(batch, HeadersColumn)
	expectedKey := blobKey(batch, start)
	expectedTimestamp := blobTimestamp(batch, start)
	expectedHeaders := headersAt(headersColumn, start)
	end := start + 1
	for end < int(batch.NumRows()) &&
		bytes.Equal(expectedKey, blobKey(batch, end)) &&
		expectedTimestamp == blobTimestamp(batch, end) &&
		headersEqual(expectedHeaders, headersAt(headersColumn, end)) {
		end++
	}
	return end - start
}

func headersEqual(left, right []RecordHeader) bool {
	if len(left) != len(right) {
		return false
	}
	for i, header := range left {
		if !header.Equal(right[i]) {
			return false
		}
	}
	return true
}

func blobKey(batch arrow.Record, row int) []byte {
	keys, ok := columnByName(batch, KeyColumn).(*array.Binary)
	if !ok || keys.IsNull(row) {
		return nil
	}
	return bytes.Clone(keys.Value(row))
}

func blobTimestamp(batch arrow.Record, row int) int64 {
	timestamps, ok := columnByName(batch, TimestampColumn).(*array.Int64)
	if !ok || timestamps.IsNull(row) {
		return 0
	}
	return timestamps.Value(row)
}
