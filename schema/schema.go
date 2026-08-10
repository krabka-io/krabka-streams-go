// Package schema talks to a Confluent-compatible schema registry and provides
// Avro, Protobuf, and JSON Schema serdes over the Confluent wire format.
//
// The package splits registry I/O from serialization: [RegistryClient] performs
// HTTP calls, [SchemaCache] holds resolved schema IDs and writer schemas in
// memory, and the serdes only ever read from the cache. Resolve everything you
// can before processing starts with [SchemaCache.Prewarm]; a consumer meeting
// an unknown writer schema ID mid-stream triggers a single background fetch
// and a retriable [FetchPendingError].
package schema

// Role identifies whether a schema belongs to a record key or value.
//
// The role selects the registry subject through the configured
// [SubjectNameStrategy], for example "orders-key" versus "orders-value" under
// the default [TopicNameStrategy].
type Role int

const (
	// RoleKey marks a schema that describes the record key.
	RoleKey Role = iota

	// RoleValue marks a schema that describes the record value.
	RoleValue
)

// String returns "KEY" or "VALUE".
func (r Role) String() string {
	if r == RoleKey {
		return "KEY"
	}
	return "VALUE"
}

// Kind enumerates the schema formats supported by the registry client.
//
// The kind is sent as the Confluent schemaType field when registering or
// looking up a schema. Avro is the registry default and is therefore
// transmitted without an explicit type name.
type Kind int

const (
	// KindAvro is an Apache Avro schema, the registry default format.
	KindAvro Kind = iota

	// KindProtobuf is a Protocol Buffers file descriptor rendered as .proto text.
	KindProtobuf

	// KindJSON is a JSON Schema document.
	KindJSON
)

// wireName returns the registry schemaType value, or "" for Avro, which the
// registry treats as the default and is sent without a schemaType field.
func (k Kind) wireName() string {
	switch k {
	case KindProtobuf:
		return "PROTOBUF"
	case KindJSON:
		return "JSON"
	default:
		return ""
	}
}

// RegisterMode defines how prewarming resolves a schema ID.
//
// [SchemaCache.Prewarm] resolves every interned subject to a schema ID by
// talking to the registry once. The mode chooses the registry operation used
// for that resolution.
type RegisterMode int

const (
	// AutoRegister registers the local schema, creating a new subject version
	// when the schema is unknown to the registry. Registration is idempotent:
	// re-registering an existing schema returns its existing ID.
	AutoRegister RegisterMode = iota

	// LookupOnly looks the local schema up under the subject and fails
	// prewarming when the registry does not already know it. Use this in
	// environments where schemas are registered by a deployment pipeline
	// rather than by applications.
	LookupOnly

	// UseLatest uses the latest registered version of the subject regardless
	// of the local schema text. The writer schema recorded for the resolved ID
	// is still the local schema, so the local and latest schemas must be
	// compatible.
	UseLatest
)

// SubjectNameStrategy maps a topic and record role to a schema registry
// subject.
//
// The default strategy is [TopicNameStrategy]. Provide a custom implementation
// to serdes or to [SchemaCache] when subjects follow a different convention,
// such as one subject per record type shared across topics.
type SubjectNameStrategy func(topic string, role Role) string

// TopicNameStrategy returns the Confluent topic naming rule: a key schema maps
// to "<topic>-key" and a value schema to "<topic>-value". This matches
// Confluent's default TopicNameStrategy, so applications sharing topics with
// Confluent-configured clients resolve the same subjects.
func TopicNameStrategy(topic string, role Role) string {
	if role == RoleKey {
		return topic + "-key"
	}
	return topic + "-value"
}
