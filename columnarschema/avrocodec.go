package columnarschema

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hamba/avro/v2"

	"github.com/krabka-io/krabka-streams-go/columnar"
	registry "github.com/krabka-io/krabka-streams-go/schema"
)

// AvroBatchCodec is a columnar.BatchCodec for Avro topics backed by the
// schema registry.
//
// The Arrow schema derives from the fixed reader schema once, at
// construction. Records written with other registered writer schemas are
// resolved onto that reader view by the embedded serde, so a schema change
// mid-stream never changes the columns, and an unknown writer schema ID
// surfaces as the cache's retriable pending-fetch error, which the group
// runner rethrows instead of skipping or dead-lettering.
type AvroBatchCodec struct {
	serde       *registry.AvroSerde[any]
	delegate    *columnar.RowCodec[any]
	arrowSchema *arrow.Schema
}

// NewAvroBatchCodec creates a codec for a reader schema over generic Avro
// values.
func NewAvroBatchCodec(schemaText string, cache *registry.SchemaCache, mem memory.Allocator) (*AvroBatchCodec, error) {
	serde, err := registry.NewGenericAvroSerde(schemaText, cache, registry.RoleValue)
	if err != nil {
		return nil, err
	}
	bridge, err := NewAvroRowBridge(serde.ReaderSchema())
	if err != nil {
		return nil, err
	}
	return &AvroBatchCodec{
		serde:       serde,
		delegate:    columnar.NewRowCodec[any](serde, bridge, mem),
		arrowSchema: bridge.ArrowSchema(),
	}, nil
}

// RegisterSubject interns the codec's schema under the topic's subject for
// the next prewarm.
func (c *AvroBatchCodec) RegisterSubject(topic string) {
	c.serde.RegisterSubject(topic)
}

// ArrowSchema returns the payload schema of every decoded batch.
func (c *AvroBatchCodec) ArrowSchema() *arrow.Schema { return c.arrowSchema }

// Decode implements columnar.BatchCodec.
func (c *AvroBatchCodec) Decode(topic string, records []columnar.ConsumedRecord) (arrow.Record, error) {
	return c.delegate.Decode(topic, records)
}

// Encode implements columnar.BatchCodec.
func (c *AvroBatchCodec) Encode(topic string, batch arrow.Record) ([]columnar.ProduceRecord, error) {
	return c.delegate.Encode(topic, batch)
}

// ParseAvroSchema parses Avro schema text, for callers that build bridges
// directly.
func ParseAvroSchema(schemaText string) (avro.Schema, error) {
	parsed, err := avro.Parse(schemaText)
	if err != nil {
		return nil, fmt.Errorf("invalid Avro schema: %w", err)
	}
	return parsed, nil
}
