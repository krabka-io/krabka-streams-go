package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// JSONSchemaSerde is a [Serde] that encodes values as Confluent-framed JSON
// validated against a JSON Schema.
//
// When validation is enabled, serialization checks the document against the
// serde's local schema and deserialization checks it against the fetched
// writer schema. Compiled validators are cached per schema ID.
//
// The serde detects Draft 4, 6, 7, 2019-09, or 2020-12 from $schema; schemas
// without it default to 2020-12.
type JSONSchemaSerde[T any] struct {
	schemaSerde[T]
	validate       bool
	localValidator *jsonschema.Schema

	mu         sync.Mutex
	validators map[int]*jsonschema.Schema
}

// NewJSONSchemaSerde creates a serde that maps documents onto T with
// encoding/json. The validate flag enables schema validation on both paths.
func NewJSONSchemaSerde[T any](schemaText string, cache *SchemaCache, role Role, validate bool, options ...SerdeOption) (*JSONSchemaSerde[T], error) {
	var applied serdeOptions
	for _, option := range options {
		option(&applied)
	}
	serde := &JSONSchemaSerde[T]{
		validate:   validate,
		validators: map[int]*jsonschema.Schema{},
	}
	if validate {
		localValidator, err := compileJSONSchema(schemaText)
		if err != nil {
			return nil, fmt.Errorf("invalid JSON Schema: %w", err)
		}
		serde.localValidator = localValidator
	} else if !json.Valid([]byte(schemaText)) {
		return nil, fmt.Errorf("invalid JSON Schema")
	}
	serde.schemaSerde = schemaSerde[T]{
		cache:           cache,
		role:            role,
		kind:            KindJSON,
		schema:          schemaText,
		strategy:        applied.strategy,
		serializeBody:   serde.serializeBody,
		deserializeBody: serde.deserializeBody,
		frame:           standardFrame,
		unframe:         Decode,
	}
	return serde, nil
}

func (s *JSONSchemaSerde[T]) serializeBody(value T) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if s.validate {
		if err := validateJSON(s.localValidator, body); err != nil {
			return nil, err
		}
	}
	return body, nil
}

func (s *JSONSchemaSerde[T]) deserializeBody(schemaID int, body []byte) (T, error) {
	var zero T
	writerSchema, err := s.cache.WriterSchema(schemaID)
	if err != nil {
		return zero, err
	}
	if s.validate {
		validator, err := s.validatorFor(schemaID, writerSchema)
		if err != nil {
			return zero, err
		}
		if err := validateJSON(validator, body); err != nil {
			return zero, err
		}
	}
	var value T
	if err := json.Unmarshal(body, &value); err != nil {
		return zero, err
	}
	return value, nil
}

func (s *JSONSchemaSerde[T]) validatorFor(schemaID int, writerSchema string) (*jsonschema.Schema, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if validator, ok := s.validators[schemaID]; ok {
		return validator, nil
	}
	validator, err := compileJSONSchema(writerSchema)
	if err != nil {
		return nil, fmt.Errorf("invalid writer JSON Schema %d: %w", schemaID, err)
	}
	s.validators[schemaID] = validator
	return validator, nil
}

func compileJSONSchema(schemaText string) (*jsonschema.Schema, error) {
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader([]byte(schemaText)))
	if err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const resource = "schema.json"
	if err := compiler.AddResource(resource, document); err != nil {
		return nil, err
	}
	return compiler.Compile(resource)
}

func validateJSON(validator *jsonschema.Schema, body []byte) error {
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(body))
	if err != nil {
		return err
	}
	if err := validator.Validate(document); err != nil {
		message := err.Error()
		var validation *jsonschema.ValidationError
		if ok := asValidationError(err, &validation); ok {
			message = firstLeafMessage(validation)
		}
		return fmt.Errorf("JSON Schema validation failed: %s", message)
	}
	return nil
}

func asValidationError(err error, target **jsonschema.ValidationError) bool {
	validation, ok := err.(*jsonschema.ValidationError)
	if ok {
		*target = validation
	}
	return ok
}

func firstLeafMessage(err *jsonschema.ValidationError) string {
	for len(err.Causes) > 0 {
		err = err.Causes[0]
	}
	return err.ErrorKind.LocalizedString(message.NewPrinter(language.English))
}
