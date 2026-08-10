package schema_test

import (
	"fmt"

	"github.com/krabka-io/krabka-streams-go/schema"
)

func ExampleEncode() {
	frame := schema.Encode(258, []byte("body"))

	decoded, err := schema.Decode(frame)
	if err != nil {
		panic(err)
	}
	fmt.Println(decoded.SchemaID, string(decoded.Body))
	// Output: 258 body
}

// Seeding the cache makes serdes fully deterministic: no registry is ever
// contacted, so the unreachable URL is intentional.
func ExampleSchemaCache_seeding() {
	client, err := schema.NewRegistryClient("http://127.0.0.1:1")
	if err != nil {
		panic(err)
	}
	cache := schema.NewSchemaCache(client)

	type Order struct {
		ID string `json:"id"`
	}
	schemaText := `{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`
	serde, err := schema.NewJSONSchemaSerde[Order](schemaText, cache, schema.RoleValue, true)
	if err != nil {
		panic(err)
	}
	cache.SeedSubjectID("orders-value", 11)
	cache.SeedWriterSchema(11, schemaText)

	data, err := serde.Serialize("orders", Order{ID: "o-1"})
	if err != nil {
		panic(err)
	}
	back, err := serde.Deserialize("orders", data)
	if err != nil {
		panic(err)
	}
	fmt.Println(back.ID)
	// Output: o-1
}

func ExampleTopicNameStrategy() {
	fmt.Println(schema.TopicNameStrategy("orders", schema.RoleKey))
	fmt.Println(schema.TopicNameStrategy("orders", schema.RoleValue))
	// Output:
	// orders-key
	// orders-value
}
