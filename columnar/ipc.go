package columnar

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// IPCSerde reads and writes single Arrow batches in the Arrow IPC stream
// format. [BlobCodec] uses it internally, and you can use it directly to read
// or write Arrow-valued topics.
//
// Each serialized value contains exactly one record batch. Deserialization
// reads the first batch and returns a batch owned by the caller: the caller
// must release it. Nil bytes deserialize to nil, and a nil batch serializes
// to nil, so Kafka tombstones survive untouched.
type IPCSerde struct {
	mem memory.Allocator
}

// NewIPCSerde creates a serde whose deserialized batches are allocated from
// mem.
func NewIPCSerde(mem memory.Allocator) *IPCSerde {
	return &IPCSerde{mem: mem}
}

// Serialize writes one batch as an Arrow IPC stream.
func (s *IPCSerde) Serialize(topic string, batch arrow.Record) ([]byte, error) {
	if batch == nil {
		return nil, nil
	}
	return s.serialize(batch)
}

// Deserialize reads the first record batch of an Arrow IPC stream. The caller
// owns the returned batch and must release it.
func (s *IPCSerde) Deserialize(topic string, data []byte) (arrow.Record, error) {
	if data == nil {
		return nil, nil
	}
	return s.deserialize(data)
}

func (s *IPCSerde) serialize(batch arrow.Record) ([]byte, error) {
	var output bytes.Buffer
	writer := ipc.NewWriter(&output, ipc.WithSchema(batch.Schema()), ipc.WithAllocator(s.mem))
	if err := writer.Write(batch); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("cannot write Arrow IPC stream: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("cannot write Arrow IPC stream: %w", err)
	}
	return output.Bytes(), nil
}

func (s *IPCSerde) deserialize(data []byte) (arrow.Record, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(s.mem))
	if err != nil {
		return nil, fmt.Errorf("cannot read Arrow IPC stream: %w", err)
	}
	defer reader.Release()
	if !reader.Next() {
		if err := reader.Err(); err != nil {
			return nil, fmt.Errorf("cannot read Arrow IPC stream: %w", err)
		}
		return nil, fmt.Errorf("Arrow IPC stream has no record batch")
	}
	batch := reader.RecordBatch()
	batch.Retain()
	return batch, nil
}
