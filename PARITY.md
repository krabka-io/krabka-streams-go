# Feature parity with krabka-streams-java

Version `0.2.0` tracks `krabka-streams-java` `1.3.0`. Each row names the Java
area, its Go equivalent, and any deliberate adaptation.

| Area                               | Go implementation                                                    | Status   |
| ---------------------------------- | -------------------------------------------------------------------- | -------- |
| Streams DSL / Processor API        | Not applicable: Go has no Kafka Streams; the columnar runtime is the processing engine | Adapted  |
| Streams group protocol             | `krabkastreams.WithDefaults` over a string-keyed config map          | Complete |
| Schema registry client             | `schema.RegistryClient`: context-based synchronous methods instead of futures; same endpoints, retries, and errors | Complete |
| Schema cache and prewarming        | `schema.SchemaCache` with `Prewarm`, `PrewarmReport`, seeding, and background writer-schema fetches | Complete |
| Confluent wire format              | `schema.Encode`/`Decode`/`EncodeProtobuf`/`DecodeProtobuf`           | Complete |
| Avro serde                         | `schema.NewAvroSerde` (struct tags) and `NewGenericAvroSerde` over hamba/avro; registers the canonical parsing form; reader/writer resolution on read | Complete |
| Protobuf serde                     | `schema.NewProtobufSerde` over protoreflect with a native `.proto` printer; deterministic (field-number-ordered) serialization | Complete |
| JSON Schema serde                  | `schema.NewJSONSchemaSerde` over santhosh-tekuri/jsonschema; drafts 4–2020-12 | Complete |
| Local compatibility checks         | `schema.AvroCompatibility`, `JSONCompatibility`, `ProtobufCompatibility` | Complete |
| Arrow IPC serde                    | `columnar.IPCSerde`                                                  | Complete |
| Blob and row codecs                | `columnar.BlobCodec`, `RowCodec`, `GzipBatchCodec`, `JSONRowBridge`  | Complete |
| Built-in operators                 | `Filter`, `Select`, `WithColumns`, `GroupBy`, `WindowedGroupBy` with snapshot/restore | Complete |
| Topology and runtime               | `columnar.Topology`/`BuiltTopology` with per-partition processor state and explicit lifecycle | Complete |
| Event-time join                    | `columnar.Join` via `AddJoin`, windowed, snapshot-capable            | Complete |
| Group runner                       | `columnar.GroupRunner` and `RunPartitionOnce`/`RunGroupOnce` over small `Consumer`/`Producer` interfaces; adapters for franz-go or confluent-kafka-go are your ~50 lines | Adapted  |
| Error policies, metrics, state store | `columnar.ErrorPolicy`, `Metrics`, `FileStateStore`                | Complete |
| Avro Arrow bridge                  | `columnarschema.AvroRowBridge`/`AvroBatchCodec` over hamba generic values | Complete |
| Protobuf Arrow bridge              | `columnarschema.ProtobufRowBridge`/`ProtobufBatchCodec` over protoreflect | Complete |
| Test utilities                     | `krabkatest.SchemaRegistryStub`, `krabkatest.ColumnarTestDriver`     | Complete |
| Barrier cuts and state snapshots   | `columnar.CutReader` with `LatestCompleteCut`/`CompleteCutsAfter`, `WithBarrierGroup`, `WithBarrierListener`, `RestoreToEpoch`/`RestoreToLatestCut`, epoch-keyed `StateStore` | Complete |
| Broker and registry integration tests | Not yet ported                                                    | Missing  |

## Known divergences

- **Errors, not exceptions.** Kafka's `SerializationException` and
  `RetriableException` have no Go equivalents. Serde failures return wrapped
  errors with the same messages; retriability is modeled by a
  `Retriable() bool` method on the error (`schema.FetchPendingError` has it),
  which the group runner detects through the error chain.
- **Snapshot formats are Go-specific.** Java serializes group-by state with
  Java serialization; Go uses its own tagged binary format. Snapshots are not
  portable between the two libraries (they never were meant to be).
- **hamba generic representation.** Avro generic values are `map[string]any`
  with hamba's type mapping (`*big.Rat` decimals, `time.Time` timestamps,
  unions unwrapped to the branch value), not Java `GenericRecord`.
- **Zero-copy where Java copies.** Go Arrow arrays are immutable, so `Select`,
  metadata attachment, and per-operator batch isolation share buffers instead
  of copying rows. Observable behavior is unchanged.
- **The Avro JSON fallback rejects decimals.** A decimal logical type inside a
  recursive record (the JSON-text fallback path) returns an error instead of
  encoding the unscaled bytes.
- **Dictionary-encoded columns** are not supported by the value read/write
  facade (`columnar.Value`/`AppendValue`).
- **The cut reader needs a partition count, and a barrier needs a position
  the runner saw.** The Go `Consumer` seam has no partition lookup and no
  position query, both of which the Java `KafkaConsumer` has. So
  `NewCutReader` takes the partition count of `__barrier_state`, and a
  barrier holds a partition until a record or a restore tells the runner
  where that partition is.
- **No typed Avro bridge.** Java's `AvroRowBridge.forSpecific` maps generated
  `SpecificRecord` classes; the Go bridge is generic-only. Typed Go structs
  can use the typed serde (`schema.NewAvroSerde`) with `JSONRowBridge`, or
  convert at the edges.
