# Configuration

## WithDefaults

The root package holds one function:

```go
func WithDefaults(settings map[string]any) map[string]any
```

It copies every entry of `settings` into a fresh map and then applies krabka
defaults for keys that are absent. The input map is never modified. Constants:

| Constant              | Value              |
| --------------------- | ------------------ |
| `GroupProtocolConfig` | `"group.protocol"` |
| `StreamsGroupProtocol`| `"streams"`        |

### The default it applies

`group.protocol=streams` selects the KIP-1071 streams group protocol, in which
the broker owns task assignment instead of the client. This is the protocol
krabka brokers implement.

An explicit setting always wins:

```go
classic := krabkastreams.WithDefaults(map[string]any{
    krabkastreams.GroupProtocolConfig: "classic",
})
// classic["group.protocol"] == "classic"
```

The map shape matches librdkafka-based clients such as confluent-kafka-go.
Clients with typed options can use the constants directly.

The columnar group runner in this library uses an ordinary consumer group,
not the streams protocol; the helper exists for applications that also run
Kafka Streams clients (for example JVM services in the same system).

## Broker requirements

The streams group protocol needs both of the following on the broker:

1. The `streams` rebalance protocol enabled in the group coordinator.
2. The `streams.version=1` feature finalized on the cluster.

For Apache Kafka 4.3.1 in a container that means, at minimum:

```text
KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer,streams
KAFKA_UNSTABLE_API_VERSIONS_ENABLE=true
KAFKA_UNSTABLE_FEATURE_VERSIONS_ENABLE=true
```

followed by finalizing the feature:

```shell
kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature streams.version=1
```

A krabka broker finalizes the feature at format time.

## Memory

Arrow memory in Go is reference-counted but heap-allocated through a
`memory.Allocator`; there are no JVM flags or direct-memory settings to
configure. Use `memory.NewCheckedAllocator` in tests to turn a leaked batch
into a failing test.
