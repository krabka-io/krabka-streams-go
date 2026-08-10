package schema

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// SchemaCache stores schema IDs and writer schemas for synchronous serde
// operations.
//
// Serdes must never block a Kafka poll loop on registry I/O, so all registry
// traffic is funneled through this cache:
//
//  1. Each serde calls [SchemaCache.Intern] (through its RegisterSubject
//     method) to declare the subjects it will use.
//  2. The application calls [SchemaCache.Prewarm] once at startup, which
//     resolves every interned subject to a schema ID with the configured
//     [RegisterMode].
//  3. Serialization then reads IDs synchronously with
//     [SchemaCache.IDForSubject], and deserialization reads writer schemas
//     with [SchemaCache.WriterSchema]; an unknown ID starts one background
//     fetch and returns the retriable [FetchPendingError].
//
// The cache is safe for concurrent use and can be shared by any number of
// serdes; sharing one cache per application is the intended usage.
type SchemaCache struct {
	client       *RegistryClient
	registerMode RegisterMode
	strategy     SubjectNameStrategy

	mu                 sync.Mutex
	interned           map[string]internedSchema
	subjectIDs         map[string]int
	writerSchemas      map[int]string
	writerMessageTypes map[int]string
	writerReferences   map[int]map[string]string
	fetching           map[int]bool
}

type internedSchema struct {
	kind        Kind
	schema      string
	messageType string
}

// PrewarmReport is the per-subject outcome of [SchemaCache.PrewarmReport].
type PrewarmReport struct {
	// Resolved maps each successful subject to its resolved schema ID.
	Resolved map[string]int

	// Failures maps each failed subject to its failure cause.
	Failures map[string]error
}

// Successful reports whether every subject resolved.
func (r PrewarmReport) Successful() bool { return len(r.Failures) == 0 }

// SchemaCacheOption configures a [SchemaCache].
type SchemaCacheOption func(*SchemaCache)

// WithRegisterMode sets how prewarming resolves each interned subject to an
// ID. The default is [AutoRegister].
func WithRegisterMode(mode RegisterMode) SchemaCacheOption {
	return func(c *SchemaCache) { c.registerMode = mode }
}

// WithSubjectNameStrategy sets the default topic-to-subject mapping. The
// default is [TopicNameStrategy].
func WithSubjectNameStrategy(strategy SubjectNameStrategy) SchemaCacheOption {
	return func(c *SchemaCache) { c.strategy = strategy }
}

// NewSchemaCache creates a cache over the given registry client. Without
// options it auto-registers schemas and uses the topic naming rule.
func NewSchemaCache(client *RegistryClient, options ...SchemaCacheOption) *SchemaCache {
	cache := &SchemaCache{
		client:             client,
		registerMode:       AutoRegister,
		strategy:           TopicNameStrategy,
		interned:           map[string]internedSchema{},
		subjectIDs:         map[string]int{},
		writerSchemas:      map[int]string{},
		writerMessageTypes: map[int]string{},
		writerReferences:   map[int]map[string]string{},
		fetching:           map[int]bool{},
	}
	for _, option := range options {
		option(cache)
	}
	return cache
}

// Subject maps a topic and role to a subject with the cache's default
// strategy.
func (c *SchemaCache) Subject(topic string, role Role) string {
	return c.strategy(topic, role)
}

// SubjectWith maps a topic and role to a subject with an explicit strategy
// instead of the cache default.
func (c *SchemaCache) SubjectWith(topic string, role Role, strategy SubjectNameStrategy) string {
	return strategy(topic, role)
}

// Intern adds a local schema to the next prewarm operation. This operation is
// idempotent by subject: interning the same subject twice keeps the first
// schema.
//
// Serdes call this through their RegisterSubject methods; call it directly
// only when prewarming subjects that no local serde owns. messageType is the
// Protobuf message full name and "" for other formats.
func (c *SchemaCache) Intern(subject string, kind Kind, schemaText, messageType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.interned[subject]; !ok {
		c.interned[subject] = internedSchema{kind: kind, schema: schemaText, messageType: messageType}
	}
}

// Prewarm resolves all interned subject IDs with the configured registration
// mode.
//
// All subjects are resolved concurrently. The returned error joins every
// per-subject failure; use [SchemaCache.PrewarmReport] for per-subject
// outcomes. Successfully resolved subjects are cached even when others fail.
func (c *SchemaCache) Prewarm(ctx context.Context) error {
	report := c.PrewarmReport(ctx)
	if report.Successful() {
		return nil
	}
	failures := make([]error, 0, len(report.Failures))
	for subject, err := range report.Failures {
		failures = append(failures, fmt.Errorf("%s: %w", subject, err))
	}
	return errors.Join(failures...)
}

// PrewarmReport resolves every interned subject and reports successes and
// failures independently.
//
// Unlike [SchemaCache.Prewarm], it never fails as a whole; inspect the report
// to find out which subjects failed and why.
func (c *SchemaCache) PrewarmReport(ctx context.Context) PrewarmReport {
	c.mu.Lock()
	subjects := make(map[string]internedSchema, len(c.interned))
	for subject, local := range c.interned {
		subjects[subject] = local
	}
	c.mu.Unlock()

	type outcome struct {
		subject string
		id      int
		err     error
	}
	results := make(chan outcome, len(subjects))
	for subject, local := range subjects {
		go func() {
			id, err := c.resolve(ctx, subject, local)
			results <- outcome{subject: subject, id: id, err: err}
		}()
	}
	report := PrewarmReport{Resolved: map[string]int{}, Failures: map[string]error{}}
	for range subjects {
		result := <-results
		if result.err == nil {
			report.Resolved[result.subject] = result.id
		} else {
			report.Failures[result.subject] = result.err
		}
	}
	return report
}

// IDForSubject returns the resolved schema ID for a subject, if prewarming
// has resolved it.
func (c *SchemaCache) IDForSubject(subject string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.subjectIDs[subject]
	return id, ok
}

// WriterSchema returns a cached writer schema. A cache miss starts one
// background fetch and returns a retriable [*FetchPendingError].
//
// This method never blocks. Only one fetch per schema ID runs at a time;
// concurrent callers for the same ID all receive the pending error until the
// fetch completes. If the background fetch fails, its marker is removed and
// the next call starts a fresh fetch.
func (c *SchemaCache) WriterSchema(schemaID int) (string, error) {
	c.mu.Lock()
	if text, ok := c.writerSchemas[schemaID]; ok {
		c.mu.Unlock()
		return text, nil
	}
	start := !c.fetching[schemaID]
	if start {
		c.fetching[schemaID] = true
	}
	c.mu.Unlock()
	if start {
		go c.fetchWriterSchema(schemaID)
	}
	return "", &FetchPendingError{SchemaID: schemaID}
}

// WriterMessageType returns the cached Protobuf message full name for a
// schema ID, or "" when unknown or not Protobuf.
func (c *SchemaCache) WriterMessageType(schemaID int) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writerMessageTypes[schemaID]
}

// WriterReferences returns the cached referenced schemas for a schema ID as a
// map of reference names to schema texts, empty when the schema has no
// references.
func (c *SchemaCache) WriterReferences(schemaID int) map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[string]string, len(c.writerReferences[schemaID]))
	for name, text := range c.writerReferences[schemaID] {
		result[name] = text
	}
	return result
}

// SeedSubjectID adds a subject ID directly, without I/O. This method supports
// deterministic tests and offline startup.
func (c *SchemaCache) SeedSubjectID(subject string, schemaID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subjectIDs[subject] = schemaID
}

// SeedWriterSchema adds a writer schema directly, without I/O. This method
// supports deterministic tests and offline startup.
func (c *SchemaCache) SeedWriterSchema(schemaID int, schemaText string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writerSchemas[schemaID] = schemaText
}

// SeedWriterMessageType adds Protobuf message metadata directly, without I/O.
func (c *SchemaCache) SeedWriterMessageType(schemaID int, messageType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writerMessageTypes[schemaID] = messageType
}

func (c *SchemaCache) resolve(ctx context.Context, subject string, local internedSchema) (int, error) {
	var id int
	var messageType string
	var err error
	switch c.registerMode {
	case AutoRegister:
		id, err = c.client.Register(ctx, subject, local.kind, local.schema, local.messageType)
		messageType = local.messageType
	case LookupOnly:
		id, err = c.client.Lookup(ctx, subject, local.kind, local.schema, local.messageType)
		messageType = local.messageType
	case UseLatest:
		var latest RegisteredSchema
		latest, err = c.client.Latest(ctx, subject)
		id, messageType = latest.ID, latest.MessageType
	default:
		return 0, fmt.Errorf("unknown register mode %d", c.registerMode)
	}
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subjectIDs[subject] = id
	c.writerSchemas[id] = local.schema
	if messageType != "" {
		c.writerMessageTypes[id] = messageType
	}
	return id, nil
}

func (c *SchemaCache) fetchWriterSchema(schemaID int) {
	fetched, err := c.client.ResolvedSchemaByID(context.Background(), schemaID)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		c.writerSchemas[schemaID] = fetched.Schema
		if fetched.MessageType != "" {
			c.writerMessageTypes[schemaID] = fetched.MessageType
		}
		c.writerReferences[schemaID] = fetched.References
	}
	delete(c.fetching, schemaID)
}
