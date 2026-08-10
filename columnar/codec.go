package columnar

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// BatchCodec converts between fetched Kafka records and Arrow batches.
//
// Decode turns one fetched batch into one Arrow batch that carries the five
// reserved metadata columns; Encode turns an Arrow batch back into records.
// The caller owns the batch Decode returns and must release it. Encode
// returns plain byte-backed records and releases its internal temporaries
// itself.
type BatchCodec interface {
	// Decode turns a non-empty fetched batch into one Arrow batch.
	Decode(topic string, records []ConsumedRecord) (arrow.Record, error)

	// Encode turns an Arrow batch back into producible records.
	Encode(topic string, batch arrow.Record) ([]ProduceRecord, error)
}
