package schema

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

// AvroSerde is a [Serde] that encodes values as Confluent-framed Avro binary.
//
// The schema registered with the registry is the Avro canonical parsing form,
// which strips documentation, ordering, and aliases. Two schemas that differ
// only in those respects therefore resolve to the same registry ID.
//
// Writing uses binary encoding against the serde's own reader schema. Reading
// resolves the writer schema from the cache onto the reader schema, so full
// Avro schema resolution applies: added fields with defaults, dropped fields,
// promoted numeric types, and renamed records with aliases all work as Avro
// specifies.
type AvroSerde[T any] struct {
	schemaSerde[T]
	reader avro.Schema
}

// NewAvroSerde creates a serde that maps Avro data onto the Go type T using
// hamba/avro struct tags, the Go equivalent of Avro's reflect and specific
// data models. The schema text must parse as an Avro schema.
func NewAvroSerde[T any](schemaText string, cache *SchemaCache, role Role, options ...SerdeOption) (*AvroSerde[T], error) {
	return newAvroSerde[T](schemaText, cache, role, options)
}

// NewGenericAvroSerde creates a serde over Avro's generic representation:
// records become map[string]any, and scalars their natural Go types.
func NewGenericAvroSerde(schemaText string, cache *SchemaCache, role Role, options ...SerdeOption) (*AvroSerde[any], error) {
	return newAvroSerde[any](schemaText, cache, role, options)
}

func newAvroSerde[T any](schemaText string, cache *SchemaCache, role Role, options []SerdeOption) (*AvroSerde[T], error) {
	reader, err := avro.Parse(schemaText)
	if err != nil {
		return nil, fmt.Errorf("invalid Avro schema: %w", err)
	}
	var applied serdeOptions
	for _, option := range options {
		option(&applied)
	}
	serde := &AvroSerde[T]{reader: reader}
	serde.schemaSerde = schemaSerde[T]{
		cache:           cache,
		role:            role,
		kind:            KindAvro,
		schema:          canonicalAvro(reader),
		strategy:        applied.strategy,
		serializeBody:   serde.serializeBody,
		deserializeBody: serde.deserializeBody,
		frame:           standardFrame,
		unframe:         Decode,
	}
	return serde, nil
}

// ReaderSchema returns the serde's parsed reader schema.
func (s *AvroSerde[T]) ReaderSchema() avro.Schema { return s.reader }

func (s *AvroSerde[T]) serializeBody(value T) ([]byte, error) {
	return avro.Marshal(s.reader, value)
}

func (s *AvroSerde[T]) deserializeBody(schemaID int, body []byte) (T, error) {
	var zero T
	writer, err := s.writerSchema(schemaID)
	if err != nil {
		return zero, err
	}
	resolved, err := avro.NewSchemaCompatibility().Resolve(s.reader, writer)
	if err != nil {
		return zero, fmt.Errorf("cannot resolve writer schema %d against the reader schema: %w", schemaID, err)
	}
	var value T
	if err := avro.Unmarshal(resolved, body, &value); err != nil {
		return zero, err
	}
	return value, nil
}

func (s *AvroSerde[T]) writerSchema(schemaID int) (avro.Schema, error) {
	text, err := s.cache.WriterSchema(schemaID)
	if err != nil {
		return nil, err
	}
	parseCache := &avro.SchemaCache{}
	for name, reference := range s.cache.WriterReferences(schemaID) {
		if _, err := avro.ParseWithCache(reference, "", parseCache); err != nil {
			return nil, fmt.Errorf("invalid Avro reference schema %s: %w", name, err)
		}
	}
	writer, err := avro.ParseWithCache(text, "", parseCache)
	if err != nil {
		return nil, fmt.Errorf("invalid writer schema %d: %w", schemaID, err)
	}
	return writer, nil
}

// canonicalAvro renders a schema in its canonical form, the representation
// hamba/avro uses for fingerprinting, which matches the Avro canonical
// parsing form.
func canonicalAvro(schema avro.Schema) string {
	return schema.String()
}
