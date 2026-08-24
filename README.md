# krabka streams for Go

`krabka-streams-go` is the Go client library for stream processing with krabka.
It provides krabka schema registry support and Apache Arrow batch processing,
the Go equivalent of [`krabka-streams-java`](https://github.com/krabka-io/krabka-streams-java).

The minimum Go version is 1.26.

```shell
go get github.com/krabka-io/krabka-streams-go
```

## Packages

| Package                                              | Purpose                                        |
| ---------------------------------------------------- | ---------------------------------------------- |
| `github.com/krabka-io/krabka-streams-go`             | krabka client configuration defaults           |
| `github.com/krabka-io/krabka-streams-go/schema`      | Registry client, cache, and Avro/Protobuf/JSON Schema serdes |
| `github.com/krabka-io/krabka-streams-go/columnar`    | Apache Arrow batch processing and barrier cuts |
| `github.com/krabka-io/krabka-streams-go/columnarschema` | Avro and Protobuf Arrow bridges             |
| `github.com/krabka-io/krabka-streams-go/krabkatest`  | Test helpers for all packages                  |

Go has no Kafka Streams runtime, so unlike the Java library there is no
re-exported streams DSL; the columnar packages are the processing engine here,
and the runner in `columnar` participates in an ordinary consumer group
through small `Consumer` and `Producer` interfaces you satisfy with your Kafka
client of choice. See [PARITY.md](PARITY.md) for the full mapping to the Java
library.

## Configuration

krabka brokers coordinate stream processing applications through the streams
group protocol (KIP-1071). `WithDefaults` applies `group.protocol=streams` to
a string-keyed configuration map — the shape librdkafka-based clients take —
while leaving every setting you supply untouched:

```go
settings := krabkastreams.WithDefaults(map[string]any{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "order-counter",
})
```

## Schema registry serdes

```go
client, err := schema.NewRegistryClient("http://localhost:8081")
cache := schema.NewSchemaCache(client)

serde, err := schema.NewGenericAvroSerde(orderSchema, cache, schema.RoleValue)
serde.RegisterSubject("orders")
if err := cache.Prewarm(ctx); err != nil {
    log.Fatal(err) // a registry problem is a startup failure
}

data, err := serde.Serialize("orders", map[string]any{"id": "o-1"})
back, err := serde.Deserialize("orders", data)
```

Serialization and deserialization never perform I/O. A consumer meeting an
unknown writer schema ID mid-stream triggers a single background fetch and a
retriable `FetchPendingError`; retry the record and it resolves.

## Columnar processing

```go
mem := memory.NewGoAllocator()
codec := columnar.NewBlobCodec(mem)
topology := columnar.NewTopology(mem)
source, _ := topology.AddSource("source", []string{"transactions"}, codec)
large, _ := topology.AddOperator("large",
    columnar.Filter(mem, func(batch arrow.Record, row int) bool {
        return batch.Column(1).(*array.Int64).Value(row) > 4
    }), source)
topology.AddSink("archive", "large-transactions", codec, large)
built, err := topology.Build()

produced, err := built.RunBatch("transactions", records)
```

Every decoded batch carries five reserved metadata columns — `__key`,
`__timestamp`, `__partition`, `__offset`, and `__headers` — so operators
filter on partitions or sort by offset with plain column access.

## Barrier cuts

A krabka broker puts an epoch-stamped marker into every partition of a named
barrier group, and publishes the resulting cut on the internal topic
`__barrier_state`. A cut is an exact point in every input at once.

```go
reader, err := columnar.NewCutReader(barrierConsumer, partitionCount)
runner, err := columnar.NewGroupRunner(topology, consumer, producer,
    columnar.WithStateStore(columnar.NewFileStateStore(dir)),
    columnar.WithBarrierGroup("audit", reader))

offsets, err := runner.RunOnce(ctx, 250*time.Millisecond)
cut, err := runner.RestoreToLatestCut(ctx)
```

The runner holds the records after each cut, snapshots the state of every
partition at the cut, and commits the cut offsets. `RestoreToEpoch` and
`RestoreToLatestCut` load the snapshot of an epoch and seek the inputs back
to that cut. See [barrier cuts](docs/barriers.md).

## Documentation

Full documentation is in [docs/](docs/index.md). The API reference, generated
from the godoc comments, is published at
<https://krabka-io.github.io/krabka-streams-go/>.

| Document                                   | Contents                                            |
| ------------------------------------------ | --------------------------------------------------- |
| [Getting started](docs/getting-started.md) | Requirements, module path, and first examples       |
| [Configuration](docs/configuration.md)     | `WithDefaults` and broker requirements              |
| [Schema registry](docs/schema-registry.md) | Registry client, schema cache, prewarming           |
| [Serdes](docs/serdes.md)                   | Avro, Protobuf, JSON Schema, and the Confluent wire format |
| [Columnar processing](docs/columnar.md)    | Arrow batches, codecs, topologies, runner           |
| [Barrier cuts](docs/barriers.md)           | Cut manifests, alignment, epoch-keyed snapshots     |
| [Testing](docs/testing.md)                 | Test driver and registry stub                       |
| [Architecture](docs/architecture.md)       | Package layout and design decisions                 |

## Build

```shell
go build ./...
go test ./...
```

The repository also builds hermetically with Bazel, which is what CI runs:

```shell
bazel test //...
```

BUILD files are generated by Gazelle; after adding or moving Go files, run:

```shell
bazel run //:gazelle
```

The published API reference builds hermetically through Bazel as well:

```shell
bazel build //:docsite-site
```

## License

Apache License 2.0; see [LICENSE](LICENSE).
