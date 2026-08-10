# Changelog

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
