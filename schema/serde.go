package schema

import (
	"errors"
	"fmt"
	"reflect"
)

// Serde produces and consumes Confluent-framed record bytes for one record
// role.
//
// All serdes in this package share the same lifecycle: construct the serde
// for a fixed [Role], call RegisterSubject for every topic it will serve,
// prewarm the shared [SchemaCache], and then serialize and deserialize
// synchronously. Serialization and deserialization never perform I/O.
//
// A nil-like value (a nil pointer, map, slice, or interface) serializes to
// nil bytes, so Kafka tombstones survive untouched, and nil bytes
// deserialize to the zero value.
type Serde[T any] interface {
	// Serialize encodes a value for a topic into Confluent-framed bytes.
	Serialize(topic string, value T) ([]byte, error)

	// Deserialize decodes Confluent-framed bytes read from a topic.
	Deserialize(topic string, data []byte) (T, error)

	// RegisterSubject interns the serde's schema under the topic's subject
	// for the next prewarm. It performs no I/O and is idempotent per subject.
	RegisterSubject(topic string)
}

// SerdeOption configures serde construction.
type SerdeOption func(*serdeOptions)

type serdeOptions struct {
	strategy SubjectNameStrategy
}

// WithStrategy sets the subject name strategy the serde uses instead of the
// cache default. Each serde accepts its own strategy, so one cache can serve
// topic-name and record-name subjects together.
func WithStrategy(strategy SubjectNameStrategy) SerdeOption {
	return func(o *serdeOptions) { o.strategy = strategy }
}

// schemaSerde implements the shared serde lifecycle: framing, subject
// resolution, and cache access. Format-specific serdes plug in body codecs.
type schemaSerde[T any] struct {
	cache           *SchemaCache
	role            Role
	kind            Kind
	schema          string
	messageType     string
	strategy        SubjectNameStrategy
	serializeBody   func(value T) ([]byte, error)
	deserializeBody func(schemaID int, body []byte) (T, error)
	frame           func(schemaID int, body []byte) ([]byte, error)
	unframe         func(data []byte) (Frame, error)
}

func (s *schemaSerde[T]) RegisterSubject(topic string) {
	s.cache.Intern(s.subject(topic), s.kind, s.schema, s.messageType)
}

func (s *schemaSerde[T]) Serialize(topic string, value T) ([]byte, error) {
	if isNilValue(value) {
		return nil, nil
	}
	subject := s.subject(topic)
	schemaID, ok := s.cache.IDForSubject(subject)
	if !ok {
		return nil, fmt.Errorf(
			"schema ID for %s is not resolved; call RegisterSubject and prewarm first", subject)
	}
	body, err := s.serializeBody(value)
	if err != nil {
		return nil, fmt.Errorf("cannot serialize schema value: %w", err)
	}
	return s.frame(schemaID, body)
}

func (s *schemaSerde[T]) Deserialize(topic string, data []byte) (T, error) {
	var zero T
	if data == nil {
		return zero, nil
	}
	frame, err := s.unframe(data)
	if err != nil {
		return zero, err
	}
	value, err := s.deserializeBody(frame.SchemaID, frame.Body)
	if err != nil {
		var pending *FetchPendingError
		if errors.As(err, &pending) {
			return zero, err
		}
		return zero, fmt.Errorf("cannot deserialize schema value: %w", err)
	}
	return value, nil
}

func (s *schemaSerde[T]) subject(topic string) string {
	if s.strategy != nil {
		return s.cache.SubjectWith(topic, s.role, s.strategy)
	}
	return s.cache.Subject(topic, s.role)
}

func standardFrame(schemaID int, body []byte) ([]byte, error) {
	return Encode(schemaID, body), nil
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Func, reflect.Chan:
		return v.IsNil()
	default:
		return false
	}
}
