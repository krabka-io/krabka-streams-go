# Architecture

## Packages

```text
krabkastreams (root)      configuration defaults only
schema                    hamba/avro, google.golang.org/protobuf,
                          santhosh-tekuri/jsonschema
columnar                  apache/arrow-go
columnarschema            schema + columnar + hamba/avro + protobuf
krabkatest                everything above, plus net/http for the stub
```

The two base feature packages do not know about each other: `schema` has no
Arrow dependency and `columnar` has no Avro or Protobuf dependency. They meet
in `columnarschema`, whose batch codecs and row bridges map registry schemas
onto native Arrow columns, and in `krabkatest`.

## Design decisions

### There is no streams DSL to re-export

The Java library re-exports Kafka Streams; Go has no equivalent runtime. The
columnar model — with its own node graph, consumer-group runner, and
partition state lifecycle — is the processing engine of this library. The
runner talks to Kafka through small `Consumer` and `Producer` interfaces so
the library does not pin a Kafka client; adapters over franz-go or
confluent-kafka-go are a few dozen lines of glue.

### Schema resolution is asynchronous; serdes are synchronous

A registry lookup is an HTTP call and must not run inside a poll loop. The
split is explicit: `RegistryClient` performs I/O with a context per call,
`SchemaCache` is an in-memory map serdes read synchronously, and `Prewarm`
is the one place the two meet — normally before processing starts, where a
failure is a startup failure.

The residual case is a consumer meeting an unknown writer schema ID
mid-stream. That resolves to a single background fetch plus the retriable
`*FetchPendingError`; concurrent callers share one in-flight request, and a
failed fetch clears its marker. The first record carrying a new schema ID
always fails once, on the view that a failed-and-retried record costs less
than a blocked poll loop.

### One fetched batch is the unit of work

Arrow pays off when a vector is long enough to amortize per-batch overhead,
and a consumer fetch is where many records already arrive together. Making
the fetch the processing unit means no buffering layer and no extra latency
knob; at most one batch per node is live at a time, and everything is
released when `RunBatch` returns.

### Metadata travels as columns

Kafka record metadata is projected into five reserved columns rather than a
side structure, so filtering on partition or sorting by offset is plain
column access. Under `BlobCodec`, where one record expands into many rows,
each row keeps the metadata of the record it came from; `__offset` is the
link back from a row to its source record.

### Ownership is explicit because the memory is reference-counted

Go Arrow arrays are immutable and reference-counted. The API states who
releases what: the framework owns batches inside `RunBatch`, callers own
whatever a public method returns, and forwarding a batch transfers ownership.
Where the Java library defensively copies mutable vectors, the Go library
shares immutable buffers zero-copy — `Select`, metadata attachment, and
per-operator batch isolation are structural, not physical, copies.
`memory.NewCheckedAllocator` turns a leak into a loud test failure.

### Validation happens at build time

`Topology.Validate` checks names, parents, and the presence of a source and
a sink before any data flows. Parents must already exist when a child is
added, which makes cycles unrepresentable. `Build` returns a separate type,
`BuiltTopology`, so a validated topology is distinguishable in the type
system.

### Errors are typed where callers branch

| Error                       | Returned by                 | Meaning                                                 |
| --------------------------- | --------------------------- | ------------------------------------------------------- |
| `*schema.RegistryError`     | registry client             | transport, status, or response problem; `StatusCode` tells which |
| `*schema.FetchPendingError` | `SchemaCache`               | a writer schema is being fetched; retriable             |
| plain wrapped errors        | serdes, codecs, topology    | messages name the offending subject, column, node, or record index |

Retriability is a behavior, not a type: anything in an error chain with a
`Retriable() bool` method returning true makes the group runner fail the
poll so the records are retried.

## Concurrency

| Type                      | Safety                                              |
| ------------------------- | --------------------------------------------------- |
| `RegistryClient`          | safe for concurrent use                             |
| `SchemaCache`             | safe for concurrent use                             |
| Serdes                    | safe once the cache is prewarmed                    |
| Batch codecs, row bridges | safe; the Arrow schema is fixed at construction (except `JSONRowBridge`, which infers and retains its schema and is not safe while inferring) |
| `Topology`                | not safe while building                             |
| `BuiltTopology`           | safe; calls are serialized, state isolated by partition |
| `ColumnarTestDriver`      | not safe                                            |
| `SchemaRegistryStub`      | request handling is synchronized                    |
| Arrow builders and records| confine builders to one goroutine                   |

## Dependency pinning

| Dependency                        | Module                                  |
| --------------------------------- | --------------------------------------- |
| Apache Arrow                      | `github.com/apache/arrow-go/v18`        |
| Avro                              | `github.com/hamba/avro/v2`              |
| Protobuf                          | `google.golang.org/protobuf`            |
| JSON Schema                       | `github.com/santhosh-tekuri/jsonschema/v6` |

The library pins no Kafka client.
