# Testing

The `krabkatest` package bundles what is needed to test the other packages
without a broker or a registry.

## ColumnarTestDriver

Runs a built columnar topology in-process and queues the produced records per
topic.

```go
mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
defer mem.AssertSize(t, 0)
codec := columnar.NewRowCodec[string](serde, columnar.NewJSONRowBridge[string](), mem)
topology := columnar.NewTopology(mem)
source, _ := topology.AddSource("source", []string{"in"}, codec)
topology.AddSink("sink", "out", codec, source)
built, _ := topology.Build()
driver := krabkatest.NewColumnarTestDriver(built)

driver.PipeInput("in", 0, []byte("a"), []byte("first"), 10)
driver.PipeInput("in", 0, []byte("b"), []byte("second"), 11)

record, err := driver.ReadOutput("out")
```

| Method                                              | Behavior                                                              |
| --------------------------------------------------- | --------------------------------------------------------------------- |
| `PipeInput(topic, partition, key, value, ts, headers...)` | Runs one record as a single-record batch; offsets start at 0 per topic-partition |
| `PipeBatch(topic, records)`                         | Runs a whole record list as one batch                                 |
| `FailNext(fault)`                                   | Returns one deterministic fault before the next batch                 |
| `OutputSize` / `IsOutputEmpty`                      | Queue depth for a sink topic                                          |
| `ReadOutput`                                        | Removes and returns the oldest record; fails when empty               |
| `DrainOutput`                                       | Removes and returns everything queued                                 |

`PipeInput` and `PipeBatch` differ for per-batch operators such as `GroupBy`:
each `PipeInput` call is its own batch.

## SchemaRegistryStub

A real HTTP server implementing the registry endpoints the client uses,
backed by in-memory state, bound to 127.0.0.1 on an ephemeral port.

```go
stub, err := krabkatest.NewSchemaRegistryStub()
defer stub.Close()
client, err := schema.NewRegistryClient(stub.URL())
```

Implemented endpoints: `POST /subjects/{subject}/versions` (register, IDs
from 1, identical schemas reuse an ID), `POST /subjects/{subject}` (lookup,
404/40403 when unregistered), `GET /subjects/{subject}/versions/latest`,
and `GET /schemas/ids/{id}`. Anything else is 404/40401; malformed bodies
are 422/42201. Schema identity is the triple (schema, schemaType,
messageType).

`RequestCount(method, path)` counts requests by raw path, which is how you
assert that prewarming resolved a subject exactly once.

## Testing serdes without any server

Seed the cache and skip the network entirely:

```go
client, _ := schema.NewRegistryClient("http://127.0.0.1:1") // unreachable on purpose
cache := schema.NewSchemaCache(client)
cache.SeedSubjectID("orders-value", 11)
cache.SeedWriterSchema(11, schemaText)
```

The unreachable URL is intentional: if the test ever performs a lookup, it
fails immediately instead of hanging. To test the pending-fetch path, ask for
an unseeded ID and assert `*schema.FetchPendingError` with `errors.As`.
