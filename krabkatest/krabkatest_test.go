package krabkatest

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/krabka-io/krabka-streams-go/columnar"
	"github.com/krabka-io/krabka-streams-go/schema"
)

func TestRegistryStubServesTheFullPrewarmAndSerdeCycle(t *testing.T) {
	stub, err := NewSchemaRegistryStub()
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	client, err := schema.NewRegistryClient(stub.URL())
	if err != nil {
		t.Fatal(err)
	}
	cache := schema.NewSchemaCache(client)
	type Order struct {
		ID string `json:"id"`
	}
	serde, err := schema.NewJSONSchemaSerde[Order](
		`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`,
		cache, schema.RoleValue, true)
	if err != nil {
		t.Fatal(err)
	}

	serde.RegisterSubject("orders")
	if err := cache.Prewarm(t.Context()); err != nil {
		t.Fatal(err)
	}

	data, err := serde.Serialize("orders", Order{ID: "o-1"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.Deserialize("orders", data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, Order{ID: "o-1"}) {
		t.Fatalf("unexpected round trip %+v", decoded)
	}
	if stub.RequestCount("POST", "/subjects/orders-value/versions") != 1 {
		t.Fatal("prewarming must register the subject exactly once")
	}
}

func TestRegistryStubAssignsStableIDsAndServesThem(t *testing.T) {
	stub, err := NewSchemaRegistryStub()
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	client, err := schema.NewRegistryClient(stub.URL())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	first, err := client.Register(ctx, "orders-value", schema.KindAvro, `"string"`, "")
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.Register(ctx, "orders-value", schema.KindAvro, `"string"`, "")
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.Register(ctx, "orders-value", schema.KindJSON, `"string"`, "")
	if err != nil {
		t.Fatal(err)
	}

	if first != again {
		t.Fatal("identical schemas must reuse an ID")
	}
	if other == first {
		t.Fatal("schema identity must include the schema type")
	}
	fetched, err := client.SchemaByID(ctx, first)
	if err != nil || fetched.Schema != `"string"` {
		t.Fatalf("unexpected fetched schema %+v (%v)", fetched, err)
	}
	latest, err := client.Latest(ctx, "orders-value")
	if err != nil || latest.ID != other || latest.Version != 2 {
		t.Fatalf("unexpected latest %+v (%v)", latest, err)
	}
	if _, err := client.Lookup(ctx, "missing-value", schema.KindAvro, `"string"`, ""); err == nil {
		t.Fatal("an unknown subject must fail the lookup")
	}
}

func TestColumnarTestDriverPipesRecordsAndQueuesOutputs(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	codec := columnar.NewRowCodec[string](passThroughSerde{}, columnar.NewJSONRowBridge[string](), mem)
	topology := columnar.NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, source); err != nil {
		t.Fatal(err)
	}
	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	driver := NewColumnarTestDriver(built)

	if err := driver.PipeInput("in", 0, []byte("a"), []byte("first"), 10); err != nil {
		t.Fatal(err)
	}
	if err := driver.PipeInput("in", 0, []byte("b"), []byte("second"), 11); err != nil {
		t.Fatal(err)
	}

	if driver.OutputSize("out") != 2 {
		t.Fatalf("unexpected queue depth %d", driver.OutputSize("out"))
	}
	record, err := driver.ReadOutput("out")
	if err != nil {
		t.Fatal(err)
	}
	if !record.Equal(columnar.NewProduceRecord([]byte("a"), []byte("first"), 10)) {
		t.Fatalf("unexpected first output %+v", record)
	}
	drained := driver.DrainOutput("out")
	if len(drained) != 1 || !drained[0].Equal(columnar.NewProduceRecord([]byte("b"), []byte("second"), 11)) {
		t.Fatalf("unexpected drained output %+v", drained)
	}
	if !driver.IsOutputEmpty("out") {
		t.Fatal("the queue must be empty after draining")
	}
	if _, err := driver.ReadOutput("out"); err == nil {
		t.Fatal("reading an empty queue must fail")
	}

	driver.FailNext(fmt.Errorf("deterministic fault"))
	if err := driver.PipeInput("in", 0, nil, []byte("x"), 12); err == nil {
		t.Fatal("the scheduled fault must surface")
	}
	if err := driver.PipeInput("in", 0, nil, []byte("x"), 12); err != nil {
		t.Fatal(err)
	}
}

type passThroughSerde struct{}

func (passThroughSerde) Serialize(topic string, value string) ([]byte, error) {
	return []byte(value), nil
}

func (passThroughSerde) Deserialize(topic string, data []byte) (string, error) {
	return string(data), nil
}
