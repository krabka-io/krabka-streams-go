# Getting started

## Requirements

- Go 1.26 or newer.
- A krabka broker (or any Apache Kafka 4.x cluster) if you run the group
  runner; the serdes and columnar runtime themselves need no broker.
- A Confluent-compatible schema registry for the `schema` and
  `columnarschema` packages.

## Installation

```shell
go get github.com/krabka-io/krabka-streams-go
```

The packages:

```go
import (
    krabkastreams "github.com/krabka-io/krabka-streams-go"
    "github.com/krabka-io/krabka-streams-go/schema"
    "github.com/krabka-io/krabka-streams-go/columnar"
    "github.com/krabka-io/krabka-streams-go/columnarschema"
    "github.com/krabka-io/krabka-streams-go/krabkatest"
)
```

## A first serde

```go
client, err := schema.NewRegistryClient("http://localhost:8081")
if err != nil {
    log.Fatal(err)
}
cache := schema.NewSchemaCache(client)

type Order struct {
    ID string `avro:"id"`
}
serde, err := schema.NewAvroSerde[Order](
    `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`,
    cache, schema.RoleValue)
if err != nil {
    log.Fatal(err)
}

serde.RegisterSubject("orders")
if err := cache.Prewarm(context.Background()); err != nil {
    log.Fatal(err)
}

data, err := serde.Serialize("orders", Order{ID: "o-1"})
back, err := serde.Deserialize("orders", data)
```

## A first columnar topology

```go
mem := memory.NewGoAllocator()
codec := columnar.NewRowCodec[string](stringSerde{}, columnar.NewJSONRowBridge[string](), mem)
topology := columnar.NewTopology(mem)
source, err := topology.AddSource("source", []string{"in"}, codec)
if err != nil {
    log.Fatal(err)
}
if _, err := topology.AddSink("sink", "out", codec, source); err != nil {
    log.Fatal(err)
}
built, err := topology.Build()
if err != nil {
    log.Fatal(err)
}

produced, err := built.RunBatch("in", records)
```

## Building this repository

```shell
go test ./...
```

CI runs the same suite hermetically through Bazel:

```shell
bazel test //...
```
