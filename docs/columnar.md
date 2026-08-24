# Columnar processing

The `columnar` package processes Kafka records as Apache Arrow batches
instead of one record at a time. It is meant for analytical work where
per-record dispatch dominates the cost.

## The batch model

One fetched topic-partition batch is one processing unit. Records arrive from
a consumer poll, are decoded into a single `arrow.Record`, flow through
operators as whole batches, and are encoded back into records at a sink.

The processor instances created when a topology is built survive across calls
to `RunBatch`, so `GroupBy` accumulates across batches, and custom processors
can keep state. Processor instances are isolated by logical partition number.

## Reserved metadata columns

Every decoded batch carries five columns that hold Kafka record metadata,
appended after the payload columns:

| Column        | Arrow type       | Contents                                      |
| ------------- | ---------------- | --------------------------------------------- |
| `__key`       | `Binary`, nullable | record key bytes, null for a keyless record |
| `__timestamp` | `Int64`          | record timestamp                              |
| `__partition` | `Int32`          | source partition                              |
| `__offset`    | `Int64`          | source offset                                 |
| `__headers`   | `Binary`         | ordered Kafka headers, including null values  |

A colliding payload name is escaped in the processing batch;
`columnar.PayloadColumn(name)` returns that name. The sink restores the
original payload name before encoding. The names are exported as
`KeyColumn`, `TimestampColumn`, `PartitionColumn`, `OffsetColumn`, and
`HeadersColumn`.

## Codecs

```go
type BatchCodec interface {
    Decode(topic string, records []ConsumedRecord) (arrow.Record, error)
    Encode(topic string, batch arrow.Record) ([]ProduceRecord, error)
}
```

### BlobCodec, for records that are already Arrow

Use `NewBlobCodec(mem)` when producers write Arrow IPC streams as record
values. Decoding reads each value as an IPC stream, attaches metadata
columns, and concatenates the results; all records in a batch must share one
payload schema. Encoding drops the metadata columns and packs the largest
consecutive rows that fit under `DefaultMaxRecordBytes` (900 KiB) and share
one key, timestamp, and header list. A single row over the cap fails instead
of sending a record the broker will reject.

### RowCodec, for ordinary Kafka records

```go
codec := columnar.NewRowCodec[string](valueSerde, columnar.NewJSONRowBridge[string](), mem)
```

Decoding deserializes each value with the supplied `ValueSerde`, converts
the values into columns through a `RowBridge`, and attaches metadata.
Encoding reverses it; row count is preserved in both directions. Wrap any
codec in `NewGzipBatchCodec` for per-record GZIP compression (16 MiB
decompression ceiling by default).

### JSONRowBridge

Routes values through JSON. Column inference uses the first non-null sample
and is retained across batches; pin the schema with
`NewJSONRowBridgeWithSchema` or derive it from JSON Schema with
`JSONRowBridgeFromJSONSchema`, which also enforces required fields. Nested
objects and arrays become JSON text columns tagged `krabka.json`. Scalar row
types are wrapped in a single column named `value`.

### Registry-backed codecs

`columnarschema.NewAvroBatchCodec` and `NewProtobufBatchCodec` implement
`BatchCodec` with columns that follow the record schema: nested records
become `Struct`, arrays `List`, maps `Map`, and decimals and timestamps
their native Arrow types. The Arrow schema derives from the reader schema
once, at construction (`ArrowSchema()`); writer schemas resolve onto it, and
an unknown writer schema ID surfaces as the cache's retriable pending-fetch
error. See the package documentation for the tagged fallbacks (multi-branch
unions, recursive shapes, wrappers, `google.protobuf.Timestamp`).

### IPCSerde

`NewIPCSerde(mem)` reads and writes single batches in the Arrow IPC stream
format. Each serialized value contains exactly one record batch; the caller
releases what `Deserialize` returns.

## Operators

```go
type Processor interface {
    Process(ctx *Context, batch arrow.Record) error
}
```

`Context.Forward` emits one output batch; zero calls drop the input, several
fan it out. Built-ins always forward exactly one batch:

- `Filter(mem, predicate)` keeps passing rows in order, metadata included.
- `Select(mem, columns...)` keeps the named payload columns and appends
  whichever reserved metadata columns exist. A missing name fails with
  `Arrow column does not exist: sku`.
- `WithColumns(mem, derived...)` adds derived columns; matching names replace
  in place, new names append. Reserved names are rejected at construction.
  Returned values are coerced to the declared Arrow type through
  `columnar.AppendValue`.
- `GroupBy(mem, keys, aggregations...)` groups cumulatively across batches;
  output is ordered by first appearance, keys first. `Count` yields `Int64`;
  `Sum` accumulates exactly for integral inputs (overflow fails) and as
  float64 for floats; `Min`/`Max` skip nulls. Metadata columns are dropped
  unless you group by them.
- `WindowedGroupBy(mem, keys, windowSize, aggregations...)` splits the same
  aggregation into fixed event-time windows, reading `__timestamp` and
  adding `__window_start` and `__window_end`. Closed windows are retained
  for one window size, or the retention passed to
  `WindowedGroupByWithRetention`.

Custom stateful processors implement `StatefulProcessor` to participate in
partition snapshot and restore.

### Buffer ownership

Arrow buffers are reference-counted. The rules for a topology are short: the
input batch belongs to the framework; forwarding a batch transfers it to the
framework; a batch you create and do not forward is yours to release. All
intermediate batches are released before `RunBatch` returns, including on the
error path. Outside a topology, whatever a method returns to you, you
release. In tests, `memory.NewCheckedAllocator` turns a leak into a failure.

## Topologies

```go
topology := columnar.NewTopology(mem)
source, err := topology.AddSource("source", []string{"transactions"}, codec)
large, err := topology.AddOperator("large", columnar.Filter(mem, predicate), source)
_, err = topology.AddSink("archive", "large-transactions", codec, large)
_, err = topology.AddSink("audit", "audit-log", codec, large) // fan-out
built, err := topology.Build()
```

`AddProcessor` takes a factory invoked once per logical partition; `AddMerge`
concatenates same-schema branches; `AddJoin` performs a stateful
co-partitioned inner equi-join within an event-time window;
`AddPassThroughSink` copies source records byte-for-byte. `Build` validates:
unique names, parents added earlier (cycles are unrepresentable), at least
one source and one sink.

`RunBatch(topic, records)` evaluates nodes in order and returns the produced
records; `RunBatches` evaluates several source topics together for fan-in.
Partitions run with isolated processor state; `SnapshotPartition`,
`RestorePartition`, and `ReleasePartition` expose the lifecycle. A built
topology serializes concurrent calls; use one per goroutine for parallelism.

## The runner

`RunPartitionOnce` runs one explicit assign–seek–poll–process–send–commit
cycle. For automatic partition discovery, create a group runner:

```go
runner, err := columnar.NewGroupRunner(topology, consumer, producer,
    columnar.WithErrorPolicy(columnar.DeadLetterPolicy("dlq")),
    columnar.WithStateStore(columnar.NewFileStateStore(dir)),
    columnar.WithMetrics(metrics))
for running {
    offsets, err := runner.RunOnce(ctx, 250*time.Millisecond)
}
```

`Consumer` and `Producer` are small interfaces; adapters over franz-go or
confluent-kafka-go are a few dozen lines. The runner implements
`RebalanceListener`: newly assigned logical partitions restore from the state
store, revoked partitions save before release, lost partitions release
without saving. `RunOnceTransactional` sends produced records and consumed
offsets in one producer transaction through the `TransactionalProducer`
interface.

Failed partition state is rolled back before the error policy applies, and
retriable errors (anything whose chain has `Retriable() bool` returning
true, such as the schema cache's pending fetch) always fail the poll so the
records are retried. Dead letters carry `krabka.error.class`,
`krabka.error.message`, `krabka.source.topic`, `krabka.source.partition`,
and `krabka.source.offset` headers.

`WithBarrierGroup` aligns the runner on the cuts a krabka broker publishes,
snapshots the state of every partition at each cut, and commits the cut
offsets. `RestoreToEpoch` puts the runner back at one of those cuts. See
[barrier cuts](barriers.md).
