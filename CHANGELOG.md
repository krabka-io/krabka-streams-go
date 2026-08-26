# Changelog

## Unreleased

- `coordination`: leader election, leases, and fencing tokens. The leadership
  epoch is the producer epoch that Kafka's transaction coordinator mints for
  `transactional.id = <role>`, so the broker fences a deposed leader and the
  lease is an anti-flap device only. The package ships the frozen
  `__coordination_state` record codec, the roster and rank rules,
  `Evaluate`, the lease clock, `AcquireLeadership` with a background renewal
  loop, and `DescribeLeadership` for a third-party check. It reaches a broker
  through the small `Coordinator`, `StateReader`, `Registrar`, and
  `LeaseWriter` interfaces, and it pins no Kafka client.
- `columnar`: barrier-aligned, epoch-keyed state snapshots. `CutReader` reads
  the cut manifests a krabka broker publishes on `__barrier_state`,
  `WithBarrierGroup` aligns a group runner on each complete cut, and the
  runner snapshots every partition at the cut and commits the cut offsets.
  `RestoreToEpoch` and `RestoreToLatestCut` put a runner back at a cut. A
  barrier commit rides inside the transaction of `RunOnceTransactional`.
- `columnar`: **breaking.** `StateStore.Load` and `StateStore.Save` take the
  epoch of the cut beside the partition. A snapshot outside a barrier uses
  the `NoEpoch` key. The container inside a snapshot does not change.

## 0.1.0

Initial release, porting `krabka-streams-java` 1.2.0 to Go:

- `krabkastreams`: configuration defaults for the streams group protocol.
- `schema`: Confluent wire format, registry client with retries, schema cache
  with prewarming and background writer-schema fetches, Avro, Protobuf, and
  JSON Schema serdes, a `.proto` printer, and local compatibility checks.
- `columnar`: Arrow IPC serde, blob and row codecs, GZIP wrapper, JSON row
  bridge, built-in operators with windowed grouping, topology builder and
  runtime with per-partition state, event-time join, file state store,
  metrics, error policies, and a consumer-group runner over minimal Kafka
  client interfaces.
- `columnarschema`: Avro and Protobuf Arrow bridges and registry-backed batch
  codecs.
- `krabkatest`: in-memory schema registry server and columnar test driver.
