package schema

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestRegistersAvroWithoutSchemaType(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("POST", "/subjects/orders-value/versions", 200, `{"id":42}`)
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}

	id, err := client.Register(t.Context(), "orders-value", KindAvro, `"string"`, "")
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("unexpected id %d", id)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(stub.body("POST", "/subjects/orders-value/versions")), &body); err != nil {
		t.Fatal(err)
	}
	if body["schema"] != `"string"` {
		t.Fatalf("unexpected schema %v", body["schema"])
	}
	if _, ok := body["schemaType"]; ok {
		t.Fatal("Avro must be sent without a schemaType field")
	}
}

func TestSendsProtobufMetadataAndReadsLatest(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("POST", "/subjects/orders-value/versions", 200, `{"id":43}`)
	stub.reply("GET", "/subjects/orders-value/versions/latest", 200,
		`{"id":43,"version":2,"schema":"syntax = \"proto3\";","schemaType":"PROTOBUF","messageType":"demo.Order"}`)
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Register(t.Context(), "orders-value", KindProtobuf, `syntax = "proto3";`, "demo.Order"); err != nil {
		t.Fatal(err)
	}
	latest, err := client.Latest(t.Context(), "orders-value")
	if err != nil {
		t.Fatal(err)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(stub.body("POST", "/subjects/orders-value/versions")), &body); err != nil {
		t.Fatal(err)
	}
	if body["schemaType"] != "PROTOBUF" || body["messageType"] != "demo.Order" {
		t.Fatalf("unexpected body %v", body)
	}
	expected := RegisteredSchema{
		ID: 43, Version: 2, Schema: `syntax = "proto3";`,
		SchemaType: "PROTOBUF", MessageType: "demo.Order",
	}
	if !reflect.DeepEqual(latest, expected) {
		t.Fatalf("unexpected latest %+v", latest)
	}
}

func TestReportsRegistryStatusAndBody(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("GET", "/schemas/ids/7", 404, "missing schema")
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.SchemaByID(t.Context(), 7)

	var registryFailure *RegistryError
	if !errors.As(err, &registryFailure) {
		t.Fatalf("expected RegistryError, got %v", err)
	}
	if registryFailure.StatusCode != 404 {
		t.Fatalf("unexpected status %d", registryFailure.StatusCode)
	}
	if !strings.Contains(registryFailure.Error(), "missing schema") {
		t.Fatalf("message should contain the response body: %s", registryFailure.Error())
	}
}

func TestPreservesContextPathAndSupportsRegistryManagement(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("GET", "/registry/subjects", 200, `["orders-value"]`)
	stub.reply("GET", "/registry/subjects/orders-value/versions", 200, `[1,2]`)
	stub.reply("GET", "/registry/config/orders-value", 200, `{"compatibilityLevel":"BACKWARD"}`)
	stub.reply("PUT", "/registry/config/orders-value", 200, `{"compatibility":"FULL"}`)
	stub.reply("DELETE", "/registry/subjects/orders-value", 200, `[1,2]`)
	client, err := NewRegistryClient(stub.url() + "/registry")
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	subjects, err := client.Subjects(ctx)
	if err != nil || !reflect.DeepEqual(subjects, []string{"orders-value"}) {
		t.Fatalf("unexpected subjects %v (%v)", subjects, err)
	}
	versions, err := client.Versions(ctx, "orders-value")
	if err != nil || !reflect.DeepEqual(versions, []int{1, 2}) {
		t.Fatalf("unexpected versions %v (%v)", versions, err)
	}
	level, err := client.SubjectCompatibility(ctx, "orders-value")
	if err != nil || level != "BACKWARD" {
		t.Fatalf("unexpected level %q (%v)", level, err)
	}
	updated, err := client.SetSubjectCompatibility(ctx, "orders-value", "FULL")
	if err != nil || updated != "FULL" {
		t.Fatalf("unexpected level %q (%v)", updated, err)
	}
	deleted, err := client.DeleteSubject(ctx, "orders-value", true)
	if err != nil || !reflect.DeepEqual(deleted, []int{1, 2}) {
		t.Fatalf("unexpected deleted versions %v (%v)", deleted, err)
	}
}

func TestRetriesServerErrorsAndTransportFailures(t *testing.T) {
	stub := newRegistryStub()
	defer stub.close()
	stub.reply("GET", "/subjects", 500, "boom")
	client, err := NewRegistryClient(stub.url())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Subjects(t.Context())

	var registryFailure *RegistryError
	if !errors.As(err, &registryFailure) || registryFailure.StatusCode != 500 {
		t.Fatalf("expected a 500 RegistryError, got %v", err)
	}
	if stub.count("GET", "/subjects") != 3 {
		t.Fatalf("expected 1 attempt + 2 retries, got %d", stub.count("GET", "/subjects"))
	}
}
