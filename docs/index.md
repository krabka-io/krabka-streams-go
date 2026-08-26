# krabka streams for Go

`krabka-streams-go` provides krabka schema registry support and Apache Arrow
batch processing for Go stream processors.

| Document                              | Contents                                                   |
| ------------------------------------- | ---------------------------------------------------------- |
| [Getting started](getting-started.md) | Requirements, module path, and first examples              |
| [Configuration](configuration.md)     | `WithDefaults` and broker requirements                     |
| [Schema registry](schema-registry.md) | Registry client, schema cache, prewarming                  |
| [Serdes](serdes.md)                   | Avro, Protobuf, JSON Schema, and the Confluent wire format |
| [Columnar processing](columnar.md)    | Arrow batches, codecs, topologies, runner                  |
| [Barrier cuts](barriers.md)           | Cut manifests, alignment, epoch-keyed snapshots             |
| [Coordination](coordination.md)       | Leader election, leases, fencing tokens                    |
| [Testing](testing.md)                 | Test driver and registry stub                              |
| [Architecture](architecture.md)       | Package layout and design decisions                        |

The API reference for every package is published at
<https://krabka-io.github.io/krabka-streams-go/>, generated from the godoc
comments by `bazel build //:docsite-site`.

For the mapping to `krabka-streams-java`, see [PARITY.md](../PARITY.md).
