package columnar_test

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/krabka-io/krabka-streams-go/columnar"
)

// rawSerde passes record values through unchanged.
type rawSerde struct{}

func (rawSerde) Serialize(topic string, value string) ([]byte, error) {
	return []byte(value), nil
}

func (rawSerde) Deserialize(topic string, data []byte) (string, error) {
	return string(data), nil
}

// A topology decodes fetched records into one Arrow batch, runs operators on
// whole batches, and encodes the survivors back into records at a sink.
func ExampleNewTopology() {
	mem := memory.NewGoAllocator()
	codec := columnar.NewRowCodec[string](rawSerde{}, columnar.NewJSONRowBridge[string](), mem)

	topology := columnar.NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		panic(err)
	}
	keep, err := topology.AddOperator("keep", columnar.Filter(mem,
		func(batch arrow.Record, row int) bool {
			values := batch.Column(0).(*array.String)
			return values.Value(row) != "drop"
		}), source)
	if err != nil {
		panic(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, keep); err != nil {
		panic(err)
	}
	built, err := topology.Build()
	if err != nil {
		panic(err)
	}

	produced, err := built.RunBatch("in", []columnar.ConsumedRecord{
		columnar.NewConsumedRecord(nil, []byte("keep-me"), 1, 0, 0),
		columnar.NewConsumedRecord(nil, []byte("drop"), 2, 0, 1),
	})
	if err != nil {
		panic(err)
	}
	for _, output := range produced {
		fmt.Println(output.Topic, string(output.Record.Value))
	}
	// Output: out keep-me
}
