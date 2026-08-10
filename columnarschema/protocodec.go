package columnarschema

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/krabka-io/krabka-streams-go/columnar"
	registry "github.com/krabka-io/krabka-streams-go/schema"
)

// ProtobufBatchCodec is a columnar.BatchCodec for Protobuf topics backed by
// the schema registry.
//
// The Arrow schema derives from the message descriptor once, at construction,
// and the embedded serde enforces the registry messageType check on read.
type ProtobufBatchCodec[T proto.Message] struct {
	serde       *registry.ProtobufSerde[T]
	delegate    *columnar.RowCodec[T]
	arrowSchema *arrow.Schema
}

// NewProtobufBatchCodec creates a codec for the message type of prototype,
// which is typically the zero value of a generated message type.
func NewProtobufBatchCodec[T proto.Message](prototype T, cache *registry.SchemaCache, mem memory.Allocator) *ProtobufBatchCodec[T] {
	serde := registry.NewProtobufSerde(prototype, cache, registry.RoleValue)
	bridge := NewProtobufRowBridge(prototype)
	return &ProtobufBatchCodec[T]{
		serde:       serde,
		delegate:    columnar.NewRowCodec[T](serde, bridge, mem),
		arrowSchema: bridge.ArrowSchema(),
	}
}

// RegisterSubject interns the codec's schema under the topic's subject for
// the next prewarm.
func (c *ProtobufBatchCodec[T]) RegisterSubject(topic string) {
	c.serde.RegisterSubject(topic)
}

// ArrowSchema returns the payload schema of every decoded batch.
func (c *ProtobufBatchCodec[T]) ArrowSchema() *arrow.Schema { return c.arrowSchema }

// Decode implements columnar.BatchCodec.
func (c *ProtobufBatchCodec[T]) Decode(topic string, records []columnar.ConsumedRecord) (arrow.Record, error) {
	return c.delegate.Decode(topic, records)
}

// Encode implements columnar.BatchCodec.
func (c *ProtobufBatchCodec[T]) Encode(topic string, batch arrow.Record) ([]columnar.ProduceRecord, error) {
	return c.delegate.Encode(topic, batch)
}
