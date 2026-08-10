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
| [Testing](testing.md)                 | Test driver and registry stub                              |
| [Architecture](architecture.md)       | Package layout and design decisions                        |

For the mapping to `krabka-streams-java`, see [PARITY.md](../PARITY.md).
