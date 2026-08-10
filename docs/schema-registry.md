# Schema registry

The `schema` package talks to a Confluent-compatible schema registry over
HTTP and keeps the results in a cache that serdes can read synchronously.

## Why there is a cache

A serde call inside a poll loop must not block on network I/O. krabka splits
the two concerns:

- `RegistryClient` performs HTTP calls, synchronously, with a
  `context.Context` per call.
- `SchemaCache` holds resolved IDs and writer schemas in memory.
- Serdes only ever read from the cache. They never perform I/O.

You resolve everything you can before processing starts, with
`SchemaCache.Prewarm`. One case cannot be predicted: a consumer meeting a
schema ID it has never seen. That is handled with a single background fetch
and a retriable error.

## RegistryClient

```go
client, err := schema.NewRegistryClient("http://localhost:8081")
custom, err := schema.NewRegistryClient(baseURL,
    schema.WithHTTPClient(httpClient),   // TLS, proxies, timeouts
    schema.WithMaxRetries(2),
    schema.WithBasicAuth(username, password))
```

Trailing slashes are normalized and context paths are preserved. Requests set
both `Accept` and `Content-Type` to `application/vnd.schemaregistry.v1+json`.
Subjects are URL-encoded, so subjects containing `/` or spaces are safe.

### Operations

| Method                                   | HTTP                                      |
| ---------------------------------------- | ----------------------------------------- |
| `Register(ctx, subject, kind, schema, messageType, refs...)` | `POST /subjects/{subject}/versions` |
| `Lookup(...)`                            | `POST /subjects/{subject}`                |
| `Latest(ctx, subject)` / `LatestID`      | `GET /subjects/{subject}/versions/latest` |
| `SchemaByID(ctx, id)`                    | `GET /schemas/ids/{id}`                   |
| `ResolvedSchemaByID(ctx, id)`            | ID and referenced-version reads           |
| `Subjects(ctx)`                          | `GET /subjects`                           |
| `Versions(ctx, subject)` / `Version`     | `GET /subjects/{subject}/versions[/{v}]`  |
| `Compatibility` / `SetCompatibility` and subject variants | `GET`/`PUT /config[...]` |
| `DeleteSubject` / `DeleteVersion`        | `DELETE /subjects/...`                    |

Avro is the registry default and is sent without a `schemaType` field;
`PROTOBUF` and `JSON` are sent explicitly. `messageType` carries the fully
qualified Protobuf message name.

### Errors

Every failure produces a `*RegistryError`. `StatusCode` is the HTTP status,
or `-1` for a transport or parsing error. The client retries transport
failures, HTTP 429, and 5xx responses twice by default.

```go
if _, err := client.SchemaByID(ctx, 7); err != nil {
    var registryErr *schema.RegistryError
    if errors.As(err, &registryErr) && registryErr.StatusCode == 404 {
        // the ID does not exist
    }
}
```

## SchemaCache

```go
cache := schema.NewSchemaCache(client)
strict := schema.NewSchemaCache(client,
    schema.WithRegisterMode(schema.LookupOnly),
    schema.WithSubjectNameStrategy(schema.TopicNameStrategy))
```

All internal state is mutex-protected, so a cache is safe to share across
goroutines; normally you want exactly one per application.

### Register modes

| Mode           | Registry call during prewarm              | Use when                                           |
| -------------- | ----------------------------------------- | -------------------------------------------------- |
| `AutoRegister` | `POST /subjects/{subject}/versions`       | Development, or producers that own the schema      |
| `LookupOnly`   | `POST /subjects/{subject}`                | Production, where an unregistered schema must fail |
| `UseLatest`    | `GET /subjects/{subject}/versions/latest` | Consumers that follow the registry                 |

`UseLatest` is the only mode that adopts the registry's `messageType` in
place of the locally derived one.

### Subject naming

```go
type SubjectNameStrategy func(topic string, role Role) string
```

`TopicNameStrategy` implements the Confluent rule: `orders` becomes
`orders-key` or `orders-value` depending on the role. Each serde accepts an
optional strategy through `WithStrategy`, so one cache can serve topic-name
and record-name subjects together.

### The prewarm cycle

```go
serde.RegisterSubject("orders")      // interns the subject; no I/O
otherSerde.RegisterSubject("payments")
if err := cache.Prewarm(ctx); err != nil { // one registry call per subject
    log.Fatal(err)
}
```

`Prewarm` resolves every interned subject concurrently and joins the
failures. Use `PrewarmReport` when startup should continue after partial
success; its `Resolved` and `Failures` maps are independent.

### Reading writer schemas

```go
text, err := cache.WriterSchema(schemaID) // *FetchPendingError on a miss
messageType := cache.WriterMessageType(schemaID)
references := cache.WriterReferences(schemaID)
id, ok := cache.IDForSubject(subject)
```

`WriterSchema` never blocks. On a miss it starts exactly one background
fetch and returns `*FetchPendingError`; concurrent callers for the same ID
join the in-flight fetch. The error has a `Retriable() bool` method, which
the columnar group runner uses to rethrow instead of skipping or
dead-lettering. If the background fetch fails, the marker is removed and the
next call starts a fresh fetch.

### Seeding

```go
cache.SeedSubjectID("orders-value", 11)
cache.SeedWriterSchema(11, schemaText)
cache.SeedWriterMessageType(11, "demo.Order")
```

These write to the cache directly and perform no I/O, for deterministic
tests and offline startup. A fully seeded cache never contacts the registry.
