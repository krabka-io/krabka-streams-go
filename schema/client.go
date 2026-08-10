package schema

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const contentType = "application/vnd.schemaregistry.v1+json"

// SchemaReference is a reference from one schema to a version of another
// subject.
type SchemaReference struct {
	// Name is the name the referencing schema uses, for example an Avro full
	// name or a Protobuf import path.
	Name string `json:"name"`

	// Subject is the referenced subject.
	Subject string `json:"subject"`

	// Version is the referenced version within that subject.
	Version int `json:"version"`
}

// RegisteredSchema is one registered version of a subject.
type RegisteredSchema struct {
	// ID is the schema's global registry ID.
	ID int

	// Version is the version number within the subject.
	Version int

	// Schema is the schema text.
	Schema string

	// SchemaType is the registry schema type name, or "" for Avro.
	SchemaType string

	// MessageType is the Protobuf message full name, or "" for other formats.
	MessageType string

	// References are the schemas this schema refers to.
	References []SchemaReference
}

// FetchedSchema is a schema fetched by ID, with unresolved references.
type FetchedSchema struct {
	// Schema is the schema text.
	Schema string

	// MessageType is the Protobuf message full name, or "" for other formats.
	MessageType string

	// References are the schemas this schema refers to, by name and subject
	// version.
	References []SchemaReference
}

// ResolvedSchema is a schema fetched by ID with every transitive reference
// resolved to its text.
type ResolvedSchema struct {
	// Schema is the schema text.
	Schema string

	// MessageType is the Protobuf message full name, or "" for other formats.
	MessageType string

	// References maps reference names to schema texts, in dependency-first
	// order of discovery.
	References map[string]string
}

// RegistryClient is a client for the Confluent Schema Registry REST API.
//
// Transport errors, HTTP 429, and 5xx responses are retried up to the
// configured retry count. Failures are reported as [*RegistryError].
//
// Applications rarely call this client directly for serialization; they hand
// it to a [SchemaCache], which layers caching and prewarming on top. Direct
// use suits tooling: registering schemas from a pipeline, checking
// compatibility levels, or cleaning up subjects.
//
// The client is safe for concurrent use.
type RegistryClient struct {
	baseURL       string
	httpClient    *http.Client
	maxRetries    int
	authorization string
}

// RegistryClientOption configures a [RegistryClient].
type RegistryClientOption func(*RegistryClient)

// WithHTTPClient sets the HTTP client that executes requests, which is how
// you configure TLS, proxies, and timeouts.
func WithHTTPClient(httpClient *http.Client) RegistryClientOption {
	return func(c *RegistryClient) { c.httpClient = httpClient }
}

// WithMaxRetries sets how many times a failed request is retried; zero
// disables retries. The default is two retries.
func WithMaxRetries(maxRetries int) RegistryClientOption {
	return func(c *RegistryClient) { c.maxRetries = maxRetries }
}

// WithBasicAuth sends HTTP basic authentication with every request.
func WithBasicAuth(username, password string) RegistryClientOption {
	return func(c *RegistryClient) {
		c.authorization = "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}
}

// NewRegistryClient creates a client for the registry at baseURL, for example
// "http://localhost:8081". Trailing slashes are normalized and context paths
// are preserved.
func NewRegistryClient(baseURL string, options ...RegistryClientOption) (*RegistryClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid registry base URL %q: %w", baseURL, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("registry base URL %q has no scheme or host", baseURL)
	}
	client := &RegistryClient{
		baseURL:    strings.TrimRight(parsed.String(), "/"),
		httpClient: http.DefaultClient,
		maxRetries: 2,
	}
	for _, option := range options {
		option(client)
	}
	if client.maxRetries < 0 {
		return nil, fmt.Errorf("maxRetries must not be negative")
	}
	return client, nil
}

// Register registers a schema under a subject, optionally with schema
// references, and returns its ID.
//
// Registration is idempotent: registering a schema the subject already has
// returns the existing ID. messageType is the Protobuf message full name and
// "" for other formats.
func (c *RegistryClient) Register(ctx context.Context, subject string, kind Kind, schemaText, messageType string, references ...SchemaReference) (int, error) {
	node, err := c.post(ctx, "/subjects/"+pathSegment(subject)+"/versions", payload(kind, schemaText, messageType, references))
	if err != nil {
		return 0, err
	}
	return requiredInt(node, "id")
}

// Lookup looks up the ID of an already registered schema, optionally with
// schema references. It fails with HTTP 404 when the schema is not registered
// under the subject.
func (c *RegistryClient) Lookup(ctx context.Context, subject string, kind Kind, schemaText, messageType string, references ...SchemaReference) (int, error) {
	node, err := c.post(ctx, "/subjects/"+pathSegment(subject), payload(kind, schemaText, messageType, references))
	if err != nil {
		return 0, err
	}
	return requiredInt(node, "id")
}

// Latest fetches the latest registered version of a subject.
func (c *RegistryClient) Latest(ctx context.Context, subject string) (RegisteredSchema, error) {
	node, err := c.get(ctx, "/subjects/"+pathSegment(subject)+"/versions/latest")
	if err != nil {
		return RegisteredSchema{}, err
	}
	return registeredSchema(node)
}

// LatestID fetches the schema ID of a subject's latest version.
func (c *RegistryClient) LatestID(ctx context.Context, subject string) (int, error) {
	latest, err := c.Latest(ctx, subject)
	if err != nil {
		return 0, err
	}
	return latest.ID, nil
}

// SchemaByID fetches a schema by its global ID. References are not resolved
// to their own schema texts; use [RegistryClient.ResolvedSchemaByID] for that.
func (c *RegistryClient) SchemaByID(ctx context.Context, schemaID int) (FetchedSchema, error) {
	node, err := c.get(ctx, fmt.Sprintf("/schemas/ids/%d", uint32(schemaID)))
	if err != nil {
		return FetchedSchema{}, err
	}
	text, err := requiredText(node, "schema")
	if err != nil {
		return FetchedSchema{}, err
	}
	refs, err := references(node)
	if err != nil {
		return FetchedSchema{}, err
	}
	return FetchedSchema{Schema: text, MessageType: optionalText(node, "messageType"), References: refs}, nil
}

// ResolvedSchemaByID fetches a schema by ID and recursively resolves its
// references.
//
// Each referenced subject version is fetched, and its own references are
// resolved transitively. Reference cycles are tolerated: a subject version
// already being resolved on the current path is skipped.
func (c *RegistryClient) ResolvedSchemaByID(ctx context.Context, schemaID int) (ResolvedSchema, error) {
	fetched, err := c.SchemaByID(ctx, schemaID)
	if err != nil {
		return ResolvedSchema{}, err
	}
	resolved := make(map[string]string)
	if err := c.resolveReferences(ctx, fetched.References, map[string]bool{}, resolved); err != nil {
		return ResolvedSchema{}, err
	}
	return ResolvedSchema{Schema: fetched.Schema, MessageType: fetched.MessageType, References: resolved}, nil
}

func (c *RegistryClient) resolveReferences(ctx context.Context, refs []SchemaReference, ancestors map[string]bool, resolved map[string]string) error {
	for _, reference := range refs {
		key := fmt.Sprintf("%s@%d", reference.Subject, reference.Version)
		if ancestors[key] {
			continue
		}
		ancestors[key] = true
		value, err := c.Version(ctx, reference.Subject, reference.Version)
		if err != nil {
			return err
		}
		if err := c.resolveReferences(ctx, value.References, ancestors, resolved); err != nil {
			return err
		}
		resolved[reference.Name] = value.Schema
		delete(ancestors, key)
	}
	return nil
}

// Subjects lists all subject names.
func (c *RegistryClient) Subjects(ctx context.Context) ([]string, error) {
	node, err := c.get(ctx, "/subjects")
	if err != nil {
		return nil, err
	}
	items, err := arrayItems(node)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(items))
	for i, item := range items {
		if err := json.Unmarshal(item, &result[i]); err != nil {
			return nil, registryError("cannot parse schema registry response", err)
		}
	}
	return result, nil
}

// Versions lists a subject's version numbers in registry order.
func (c *RegistryClient) Versions(ctx context.Context, subject string) ([]int, error) {
	node, err := c.get(ctx, "/subjects/"+pathSegment(subject)+"/versions")
	if err != nil {
		return nil, err
	}
	return intArray(node)
}

// Version fetches one specific version of a subject.
func (c *RegistryClient) Version(ctx context.Context, subject string, version int) (RegisteredSchema, error) {
	node, err := c.get(ctx, fmt.Sprintf("/subjects/%s/versions/%d", pathSegment(subject), version))
	if err != nil {
		return RegisteredSchema{}, err
	}
	return registeredSchema(node)
}

// SubjectCompatibility reads a subject's compatibility level, for example
// "BACKWARD".
func (c *RegistryClient) SubjectCompatibility(ctx context.Context, subject string) (string, error) {
	node, err := c.get(ctx, "/config/"+pathSegment(subject))
	if err != nil {
		return "", err
	}
	return requiredText(node, "compatibilityLevel")
}

// Compatibility reads the registry's global compatibility level.
func (c *RegistryClient) Compatibility(ctx context.Context) (string, error) {
	node, err := c.get(ctx, "/config")
	if err != nil {
		return "", err
	}
	return requiredText(node, "compatibilityLevel")
}

// SetSubjectCompatibility sets a subject's compatibility level and returns the
// level the registry acknowledged.
func (c *RegistryClient) SetSubjectCompatibility(ctx context.Context, subject, level string) (string, error) {
	node, err := c.put(ctx, "/config/"+pathSegment(subject), map[string]any{"compatibility": level})
	if err != nil {
		return "", err
	}
	return requiredText(node, "compatibility")
}

// SetCompatibility sets the registry's global compatibility level and returns
// the level the registry acknowledged.
func (c *RegistryClient) SetCompatibility(ctx context.Context, level string) (string, error) {
	node, err := c.put(ctx, "/config", map[string]any{"compatibility": level})
	if err != nil {
		return "", err
	}
	return requiredText(node, "compatibility")
}

// DeleteSubject deletes a subject and all of its versions, returning the
// deleted version numbers. When permanent is true the subject is hard-deleted
// instead of soft-deleted.
func (c *RegistryClient) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	path := "/subjects/" + pathSegment(subject)
	if permanent {
		path += "?permanent=true"
	}
	node, err := c.delete(ctx, path)
	if err != nil {
		return nil, err
	}
	return intArray(node)
}

// DeleteVersion deletes one version of a subject, returning the deleted
// version number. When permanent is true the version is hard-deleted instead
// of soft-deleted.
func (c *RegistryClient) DeleteVersion(ctx context.Context, subject string, version int, permanent bool) (int, error) {
	path := fmt.Sprintf("/subjects/%s/versions/%d", pathSegment(subject), version)
	if permanent {
		path += "?permanent=true"
	}
	node, err := c.delete(ctx, path)
	if err != nil {
		return 0, err
	}
	var result int
	if err := json.Unmarshal(node, &result); err != nil {
		return 0, registryError("cannot parse schema registry response", err)
	}
	return result, nil
}

func payload(kind Kind, schemaText, messageType string, refs []SchemaReference) map[string]any {
	body := map[string]any{"schema": schemaText}
	if kind.wireName() != "" {
		body["schemaType"] = kind.wireName()
	}
	if messageType != "" {
		body["messageType"] = messageType
	}
	if len(refs) > 0 {
		body["references"] = refs
	}
	return body
}

func (c *RegistryClient) get(ctx context.Context, path string) (json.RawMessage, error) {
	return c.send(ctx, http.MethodGet, path, nil)
}

func (c *RegistryClient) post(ctx context.Context, path string, body map[string]any) (json.RawMessage, error) {
	return c.send(ctx, http.MethodPost, path, body)
}

func (c *RegistryClient) put(ctx context.Context, path string, body map[string]any) (json.RawMessage, error) {
	return c.send(ctx, http.MethodPut, path, body)
}

func (c *RegistryClient) delete(ctx context.Context, path string) (json.RawMessage, error) {
	return c.send(ctx, http.MethodDelete, path, nil)
}

func (c *RegistryClient) send(ctx context.Context, method, path string, body map[string]any) (json.RawMessage, error) {
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			return nil, registryError("cannot encode registry request", err)
		}
	}
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		node, retriable, err := c.sendOnce(ctx, method, path, encoded)
		if err == nil {
			return node, nil
		}
		lastErr = err
		if !retriable || ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}

func (c *RegistryClient) sendOnce(ctx context.Context, method, path string, body []byte) (json.RawMessage, bool, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, false, registryError("schema registry request failed", err)
	}
	request.Header.Set("Accept", contentType)
	if body != nil {
		request.Header.Set("Content-Type", contentType)
	}
	if c.authorization != "" {
		request.Header.Set("Authorization", c.authorization)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, true, registryError("schema registry request failed", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, true, registryError("schema registry request failed", err)
	}
	if response.StatusCode == 429 || response.StatusCode >= 500 {
		return nil, true, registryStatusError(response.StatusCode, string(responseBody))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, false, registryStatusError(response.StatusCode, string(responseBody))
	}
	if !json.Valid(responseBody) {
		return nil, false, registryError("cannot parse schema registry response", nil)
	}
	return json.RawMessage(responseBody), false, nil
}

func registeredSchema(node json.RawMessage) (RegisteredSchema, error) {
	id, err := requiredInt(node, "id")
	if err != nil {
		return RegisteredSchema{}, err
	}
	refs, err := references(node)
	if err != nil {
		return RegisteredSchema{}, err
	}
	var fields struct {
		Version int    `json:"version"`
		Schema  string `json:"schema"`
	}
	_ = json.Unmarshal(node, &fields)
	return RegisteredSchema{
		ID:          id,
		Version:     fields.Version,
		Schema:      fields.Schema,
		SchemaType:  optionalText(node, "schemaType"),
		MessageType: optionalText(node, "messageType"),
		References:  refs,
	}, nil
}

func objectFields(node json.RawMessage) map[string]json.RawMessage {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(node, &fields); err != nil {
		return nil
	}
	return fields
}

func requiredInt(node json.RawMessage, name string) (int, error) {
	value, ok := objectFields(node)[name]
	if ok {
		var result int
		if err := json.Unmarshal(value, &result); err == nil {
			return result, nil
		}
	}
	return 0, registryError("schema registry response has no integer "+name, nil)
}

func requiredText(node json.RawMessage, name string) (string, error) {
	value := optionalText(node, name)
	if value == "" {
		return "", registryError("schema registry response has no text "+name, nil)
	}
	return value, nil
}

func optionalText(node json.RawMessage, name string) string {
	value, ok := objectFields(node)[name]
	if !ok {
		return ""
	}
	var result string
	if err := json.Unmarshal(value, &result); err != nil {
		return ""
	}
	return result
}

func arrayItems(node json.RawMessage) ([]json.RawMessage, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(node, &items); err != nil {
		return nil, registryError("schema registry response is not an array", err)
	}
	return items, nil
}

func intArray(node json.RawMessage) ([]int, error) {
	items, err := arrayItems(node)
	if err != nil {
		return nil, err
	}
	result := make([]int, len(items))
	for i, item := range items {
		if err := json.Unmarshal(item, &result[i]); err != nil {
			return nil, registryError("cannot parse schema registry response", err)
		}
	}
	return result, nil
}

func references(node json.RawMessage) ([]SchemaReference, error) {
	value, ok := objectFields(node)["references"]
	if !ok {
		return nil, nil
	}
	var refs []SchemaReference
	if err := json.Unmarshal(value, &refs); err != nil {
		return nil, nil
	}
	for _, reference := range refs {
		if reference.Name == "" || reference.Subject == "" {
			return nil, registryError("schema registry response has no text name", nil)
		}
	}
	return refs, nil
}

func pathSegment(value string) string {
	return url.PathEscape(value)
}
