package krabkatest_test

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/krabka-io/krabka-streams-go/columnar"
	"github.com/krabka-io/krabka-streams-go/krabkatest"
)

type echoSerde struct{}

func (echoSerde) Serialize(topic string, value string) ([]byte, error) {
	return []byte(value), nil
}

func (echoSerde) Deserialize(topic string, data []byte) (string, error) {
	return string(data), nil
}

func ExampleColumnarTestDriver() {
	mem := memory.NewGoAllocator()
	codec := columnar.NewRowCodec[string](echoSerde{}, columnar.NewJSONRowBridge[string](), mem)
	topology := columnar.NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		panic(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, source); err != nil {
		panic(err)
	}
	built, err := topology.Build()
	if err != nil {
		panic(err)
	}
	driver := krabkatest.NewColumnarTestDriver(built)

	if err := driver.PipeInput("in", 0, []byte("a"), []byte("first"), 10); err != nil {
		panic(err)
	}
	record, err := driver.ReadOutput("out")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(record.Key), string(record.Value), record.Timestamp)
	// Output: a first 10
}
