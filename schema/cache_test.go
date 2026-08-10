package schema

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestAutoRegisterUsesVersionsEndpoint(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("POST", "/subjects/orders-value/versions", 200, `{"id":50}`)
	cache := newTestCache(t, stub)
	cache.Intern("orders-value", KindAvro, `"string"`, "")

	if err := cache.Prewarm(t.Context()); err != nil {
		t.Fatal(err)
	}

	if id, ok := cache.IDForSubject("orders-value"); !ok || id != 50 {
		t.Fatalf("unexpected id %d (%v)", id, ok)
	}
	if stub.count("POST", "/subjects/orders-value/versions") != 1 {
		t.Fatal("expected exactly one registration request")
	}
}

func TestLookupOnlyUsesSubjectEndpoint(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("POST", "/subjects/orders-value", 200, `{"id":51}`)
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}
	cache := NewSchemaCache(client, WithRegisterMode(LookupOnly))
	cache.Intern("orders-value", KindJSON, `{"type":"object"}`, "")

	if err := cache.Prewarm(t.Context()); err != nil {
		t.Fatal(err)
	}

	if id, ok := cache.IDForSubject("orders-value"); !ok || id != 51 {
		t.Fatalf("unexpected id %d (%v)", id, ok)
	}
	if stub.count("POST", "/subjects/orders-value") != 1 {
		t.Fatal("expected exactly one lookup request")
	}
}

func TestUseLatestKeepsRegistryMessageType(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("GET", "/subjects/orders-value/versions/latest", 200,
		`{"id":52,"messageType":"demo.Latest"}`)
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}
	cache := NewSchemaCache(client, WithRegisterMode(UseLatest))
	cache.Intern("orders-value", KindProtobuf, `syntax = "proto3";`, "demo.Local")

	if err := cache.Prewarm(t.Context()); err != nil {
		t.Fatal(err)
	}

	if id, ok := cache.IDForSubject("orders-value"); !ok || id != 52 {
		t.Fatalf("unexpected id %d (%v)", id, ok)
	}
	if cache.WriterMessageType(52) != "demo.Latest" {
		t.Fatalf("unexpected message type %q", cache.WriterMessageType(52))
	}
}

func TestUnknownWriterSchemaStartsOneBackgroundFetch(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("GET", "/schemas/ids/7", 200,
		`{"schema":"\"string\"","messageType":"demo.Value"}`)
	cache := newTestCache(t, stub)

	if _, err := cache.WriterSchema(7); !isFetchPending(err) {
		t.Fatalf("expected FetchPendingError, got %v", err)
	}
	if _, err := cache.WriterSchema(7); !isFetchPending(err) {
		t.Fatalf("expected FetchPendingError, got %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		text, err := cache.WriterSchema(7)
		if err == nil {
			if text != `"string"` {
				t.Fatalf("unexpected writer schema %q", text)
			}
			break
		}
		if !isFetchPending(err) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("background fetch did not complete in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if cache.WriterMessageType(7) != "demo.Value" {
		t.Fatalf("unexpected message type %q", cache.WriterMessageType(7))
	}
	if stub.count("GET", "/schemas/ids/7") != 1 {
		t.Fatalf("expected one fetch, got %d", stub.count("GET", "/schemas/ids/7"))
	}
}

func TestPrewarmReportsPartialSuccess(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("POST", "/subjects/good-value/versions", 200, `{"id":8}`)
	cache := newTestCache(t, stub)
	cache.Intern("good-value", KindAvro, `"string"`, "")
	cache.Intern("bad-value", KindAvro, `"string"`, "")

	report := cache.PrewarmReport(t.Context())

	if !reflect.DeepEqual(report.Resolved, map[string]int{"good-value": 8}) {
		t.Fatalf("unexpected resolved map %v", report.Resolved)
	}
	if _, ok := report.Failures["bad-value"]; !ok {
		t.Fatalf("expected bad-value to fail, got %v", report.Failures)
	}
	if report.Successful() {
		t.Fatal("report must not be successful")
	}
}

func newTestCache(t *testing.T, stub *registryStub) *SchemaCache {
	t.Helper()
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}
	return NewSchemaCache(client)
}

func isFetchPending(err error) bool {
	var pending *FetchPendingError
	return errors.As(err, &pending)
}
