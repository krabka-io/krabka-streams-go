// Package krabkatest bundles test helpers for the schema and columnar
// packages: an in-memory schema registry server and an in-process driver for
// columnar topologies. Neither needs a broker or a real registry.
package krabkatest

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// SchemaRegistryStub is a real HTTP server implementing the registry
// endpoints the client uses, backed by in-memory state. It binds to
// 127.0.0.1 on an ephemeral port.
//
// Schema identity is the triple (schema, schemaType, messageType), so an Avro
// schema and a JSON schema with the same text receive different IDs. IDs are
// assigned from 1; identical schemas reuse an ID and the subject's version
// list grows. Request handling is synchronized, so counts are stable to read
// from the test.
type SchemaRegistryStub struct {
	server   *http.Server
	listener net.Listener

	mu              sync.Mutex
	nextID          int
	idsBySchema     map[schemaKey]int
	schemasByID     map[int]schemaValue
	subjectVersions map[string][]int
	requestCounts   map[string]int
}

type schemaKey struct {
	schema      string
	schemaType  string
	messageType string
}

type schemaValue struct {
	Schema      string `json:"schema"`
	SchemaType  string `json:"schemaType,omitempty"`
	MessageType string `json:"messageType,omitempty"`
}

func (v schemaValue) key() schemaKey {
	return schemaKey{schema: v.Schema, schemaType: v.SchemaType, messageType: v.MessageType}
}

// NewSchemaRegistryStub starts the server.
func NewSchemaRegistryStub() (*SchemaRegistryStub, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("cannot start the schema registry stub: %w", err)
	}
	stub := &SchemaRegistryStub{
		listener:        listener,
		nextID:          1,
		idsBySchema:     map[schemaKey]int{},
		schemasByID:     map[int]schemaValue{},
		subjectVersions: map[string][]int{},
		requestCounts:   map[string]int{},
	}
	stub.server = &http.Server{Handler: http.HandlerFunc(stub.handle)}
	go func() { _ = stub.server.Serve(listener) }()
	return stub, nil
}

// URL returns the base URL of the running server.
func (s *SchemaRegistryStub) URL() string {
	return "http://" + s.listener.Addr().String()
}

// RequestCount counts requests by method and raw path, which is how you
// assert that prewarming resolved a subject exactly once.
func (s *SchemaRegistryStub) RequestCount(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestCounts[method+" "+path]
}

// Close stops the server.
func (s *SchemaRegistryStub) Close() error {
	return s.server.Close()
}

func (s *SchemaRegistryStub) handle(writer http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	method := request.Method
	path := request.URL.EscapedPath()
	s.requestCounts[method+" "+path]++
	switch {
	case method == http.MethodPost && strings.HasSuffix(path, "/versions"):
		s.register(writer, request, subjectOf(path, "/versions"))
	case method == http.MethodPost && strings.HasPrefix(path, "/subjects/"):
		s.lookup(writer, request, subjectOf(path, ""))
	case method == http.MethodGet && strings.HasSuffix(path, "/versions/latest"):
		s.latest(writer, subjectOf(path, "/versions/latest"))
	case method == http.MethodGet && strings.HasPrefix(path, "/schemas/ids/"):
		id, err := strconv.Atoi(strings.TrimPrefix(path, "/schemas/ids/"))
		if err != nil {
			replyError(writer, 422, 42201, "invalid schema id")
			return
		}
		s.byID(writer, id)
	default:
		replyError(writer, 404, 40401, "subject or schema not found")
	}
}

func (s *SchemaRegistryStub) register(writer http.ResponseWriter, request *http.Request, subject string) {
	value, ok := readSchema(writer, request)
	if !ok {
		return
	}
	id, exists := s.idsBySchema[value.key()]
	if !exists {
		id = s.nextID
		s.nextID++
		s.idsBySchema[value.key()] = id
		s.schemasByID[id] = value
	}
	if !containsInt(s.subjectVersions[subject], id) {
		s.subjectVersions[subject] = append(s.subjectVersions[subject], id)
	}
	reply(writer, 200, map[string]any{"id": id})
}

func (s *SchemaRegistryStub) lookup(writer http.ResponseWriter, request *http.Request, subject string) {
	value, ok := readSchema(writer, request)
	if !ok {
		return
	}
	id, exists := s.idsBySchema[value.key()]
	versions := s.subjectVersions[subject]
	if !exists || !containsInt(versions, id) {
		replyError(writer, 404, 40403, "schema not found")
		return
	}
	body := schemaBody(id, value)
	body["subject"] = subject
	body["version"] = indexOf(versions, id) + 1
	reply(writer, 200, body)
}

func (s *SchemaRegistryStub) latest(writer http.ResponseWriter, subject string) {
	versions := s.subjectVersions[subject]
	if len(versions) == 0 {
		replyError(writer, 404, 40401, "subject not found")
		return
	}
	id := versions[len(versions)-1]
	body := schemaBody(id, s.schemasByID[id])
	body["subject"] = subject
	body["version"] = len(versions)
	reply(writer, 200, body)
}

func (s *SchemaRegistryStub) byID(writer http.ResponseWriter, id int) {
	value, ok := s.schemasByID[id]
	if !ok {
		replyError(writer, 404, 40403, "schema not found")
		return
	}
	reply(writer, 200, schemaBody(id, value))
}

func readSchema(writer http.ResponseWriter, request *http.Request) (schemaValue, bool) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		replyError(writer, 422, 42201, "cannot read the request body")
		return schemaValue{}, false
	}
	var value schemaValue
	if err := json.Unmarshal(body, &value); err != nil || value.Schema == "" {
		replyError(writer, 422, 42201, "request has no text schema")
		return schemaValue{}, false
	}
	return value, true
}

func schemaBody(id int, value schemaValue) map[string]any {
	body := map[string]any{"id": id, "schema": value.Schema}
	if value.SchemaType != "" {
		body["schemaType"] = value.SchemaType
	}
	if value.MessageType != "" {
		body["messageType"] = value.MessageType
	}
	return body
}

func subjectOf(path, suffix string) string {
	encoded := strings.TrimSuffix(strings.TrimPrefix(path, "/subjects/"), suffix)
	subject, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}
	return subject
}

func reply(writer http.ResponseWriter, status int, body map[string]any) {
	encoded, _ := json.Marshal(body)
	writer.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	writer.WriteHeader(status)
	_, _ = writer.Write(encoded)
}

func replyError(writer http.ResponseWriter, status, code int, message string) {
	reply(writer, status, map[string]any{"error_code": code, "message": message})
}

func containsInt(values []int, target int) bool {
	return indexOf(values, target) >= 0
}

func indexOf(values []int, target int) int {
	for i, value := range values {
		if value == target {
			return i
		}
	}
	return -1
}
