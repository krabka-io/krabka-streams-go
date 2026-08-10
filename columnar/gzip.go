package columnar

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
)

// DefaultMaxUncompressedBytes is the decompression ceiling of a
// [GzipBatchCodec].
const DefaultMaxUncompressedBytes = 16 * 1024 * 1024

// GzipBatchCodec wraps any [BatchCodec] with per-record GZIP compression.
// Decoding decompresses each record value before delegating; encoding
// compresses each produced record value after delegating.
type GzipBatchCodec struct {
	delegate             BatchCodec
	maxUncompressedBytes int
}

// NewGzipBatchCodec wraps a codec with [DefaultMaxUncompressedBytes].
func NewGzipBatchCodec(delegate BatchCodec) *GzipBatchCodec {
	codec, _ := NewGzipBatchCodecWithMaxBytes(delegate, DefaultMaxUncompressedBytes)
	return codec
}

// NewGzipBatchCodecWithMaxBytes wraps a codec with a custom decompression
// ceiling.
func NewGzipBatchCodecWithMaxBytes(delegate BatchCodec, maxUncompressedBytes int) (*GzipBatchCodec, error) {
	if maxUncompressedBytes < 1 || maxUncompressedBytes == math.MaxInt {
		return nil, fmt.Errorf("maxUncompressedBytes must be between 1 and %d", math.MaxInt-1)
	}
	return &GzipBatchCodec{delegate: delegate, maxUncompressedBytes: maxUncompressedBytes}, nil
}

// Decode implements [BatchCodec].
func (c *GzipBatchCodec) Decode(topic string, records []ConsumedRecord) (arrow.Record, error) {
	decompressed := make([]ConsumedRecord, len(records))
	for i, record := range records {
		value, err := c.decompress(record.Value)
		if err != nil {
			return nil, err
		}
		decompressed[i] = ConsumedRecord{
			Key:       record.Key,
			Value:     value,
			Timestamp: record.Timestamp,
			Partition: record.Partition,
			Offset:    record.Offset,
			Headers:   record.Headers,
		}
	}
	return c.delegate.Decode(topic, decompressed)
}

// Encode implements [BatchCodec].
func (c *GzipBatchCodec) Encode(topic string, batch arrow.Record) ([]ProduceRecord, error) {
	records, err := c.delegate.Encode(topic, batch)
	if err != nil {
		return nil, err
	}
	result := make([]ProduceRecord, len(records))
	for i, record := range records {
		result[i] = ProduceRecord{
			Key:       record.Key,
			Value:     compress(record.Value),
			Timestamp: record.Timestamp,
			Headers:   record.Headers,
		}
	}
	return result, nil
}

func (c *GzipBatchCodec) decompress(compressed []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("cannot decompress GZIP record: %w", err)
	}
	defer reader.Close()
	result, err := io.ReadAll(io.LimitReader(reader, int64(c.maxUncompressedBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("cannot decompress GZIP record: %w", err)
	}
	if len(result) > c.maxUncompressedBytes {
		return nil, fmt.Errorf("GZIP record exceeds maxUncompressedBytes=%d", c.maxUncompressedBytes)
	}
	return result, nil
}

func compress(uncompressed []byte) []byte {
	var result bytes.Buffer
	writer := gzip.NewWriter(&result)
	_, _ = writer.Write(uncompressed)
	_ = writer.Close()
	return result.Bytes()
}
