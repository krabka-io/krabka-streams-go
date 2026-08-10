# Serdes

The `schema` package provides three `Serde[T]` implementations, for Avro,
Protobuf, and JSON Schema. They produce and consume Confluent-framed record
bytes, and all three share the same lifecycle.

## The shared lifecycle

```go
serde, err := schema.NewGenericAvroSerde(schemaText, cache, schema.RoleValue)
serde.RegisterSubject("orders")     // intern the subject for prewarming
cache.Prewarm(ctx)                  // resolve the subject to a schema ID
data, err := serde.Serialize("orders", record)
back, err := serde.Deserialize("orders", data)
```

Serialization:

1. A nil-like value serializes to nil bytes. Kafka tombstones survive
   untouched.
2. The subject is computed from the topic and the serde's `Role`.
3. The schema ID comes from the cache. If it is absent, the serde fails with
   `schema ID for orders-value is not resolved; call RegisterSubject and
   prewarm first`.
4. The body is encoded by the format-specific implementation and framed.

Deserialization decodes the frame, reads the writer schema from the cache —
which may return the retriable `*FetchPendingError` — and decodes the body
against it.

## The Confluent wire format

`Encode`, `Decode`, `EncodeProtobuf`, and `DecodeProtobuf` implement the
framing and are public, so you can use them without the serdes.

Standard frame (Avro and JSON Schema): the `0x00` magic byte, the schema ID
as a big-endian 32-bit integer, then the body. Protobuf frames insert a
zigzag-varint message-index path between the header and the body; the common
`[0]` path is the single byte `0x00`.

Malformed input fails with the same messages as the Java library:
`schema frame is shorter than 5 bytes`, `invalid schema frame magic byte
0x01`, `truncated Protobuf message-index varint`, and so on.

## Avro

```go
// Typed: hamba/avro struct tags
serde, err := schema.NewAvroSerde[Order](schemaText, cache, schema.RoleValue)

// Generic: records become map[string]any
generic, err := schema.NewGenericAvroSerde(schemaText, cache, schema.RoleValue)
```

The schema registered with the registry is the Avro canonical parsing form,
so two schemas differing only in docs or aliases resolve to the same ID.
Reading resolves the writer schema from the cache onto the serde's reader
schema, so added fields with defaults, dropped fields, and promoted types
work as Avro specifies. Schema references are parsed before the writer
schema, so referenced names resolve.

## Protobuf

```go
serde := schema.NewProtobufSerde(&ordersv1.Order{}, cache, schema.RoleValue)
```

The serde derives the schema text (printed from the file descriptor with
`PrintProtoFile`), the `messageType`, and the message-index path from the
prototype message. Serialization is deterministic (field-number-ordered), so
encoded frames round trip byte-for-byte.

Deserialization enforces a type check: if the cache holds a `messageType`
for the writer's schema ID and it differs from the local message's full
name, the serde fails with `Protobuf messageType mismatch: writer demo.Other,
local google.protobuf.StringValue`. The check is skipped when the registry
supplied no `messageType`.

## JSON Schema

```go
serde, err := schema.NewJSONSchemaSerde[Order](schemaJSON, cache, schema.RoleValue, true)
```

The validate flag checks serialization against the local schema and
deserialization against the fetched writer schema; a failure reports
`JSON Schema validation failed: <first message>`. The serde detects Draft 4,
6, 7, 2019-09, or 2020-12 from `$schema`; schemas without it default to
2020-12. Compiled validators are cached per schema ID. Documents map onto `T`
with `encoding/json`.

## Local compatibility before registration

```go
result, err := schema.AvroCompatibility(previous, candidate, schema.Backward)
if !result.Compatible {
    log.Fatal(result.Incompatibilities)
}
```

Choose `Backward`, `Forward`, or `Full`. Avro delegates to hamba's
reader/writer compatibility checker; JSON Schema checks type narrowing,
required fields, and closed object properties; Protobuf compares message
fields by wire type, cardinality, and required-field presence.

## Writing your own serde

Implement `Serde[T]` directly and reuse the public pieces: frame with
`Encode`, resolve IDs with `SchemaCache.IDForSubject`, and read writer
schemas with `SchemaCache.WriterSchema`. Keep the two rules that make the
built-in serdes safe: never block on I/O inside `Serialize`/`Deserialize`,
and let `*FetchPendingError` propagate so the caller can retry.
