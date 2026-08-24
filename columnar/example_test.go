package columnar_test

import (
	"encoding/binary"
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

// A cut manifest on the internal barrier state topic names the offset of one
// epoch's marker in every partition of a barrier group. All integers are
// big-endian, and a string is an int16 byte length and then UTF-8 bytes.
func ExampleDecodeBarrierCut() {
	key := binary.BigEndian.AppendUint16(nil, 0) // key version
	key = binary.BigEndian.AppendUint16(key, 2)  // kind: cut
	key = binary.BigEndian.AppendUint16(key, 5)  // group name length
	key = append(key, "audit"...)                // group name
	key = binary.BigEndian.AppendUint64(key, 7)  // epoch

	value := binary.BigEndian.AppendUint16(nil, 0)     // value version
	value = binary.BigEndian.AppendUint64(value, 1000) // triggered at
	value = binary.BigEndian.AppendUint64(value, 1200) // completed at
	value = append(value, 0)                           // status: complete
	value = binary.BigEndian.AppendUint32(value, 1)    // one topic
	value = binary.BigEndian.AppendUint16(value, 6)    // topic name length
	value = append(value, "orders"...)                 // topic name
	value = binary.BigEndian.AppendUint32(value, 1)    // one partition
	value = binary.BigEndian.AppendUint32(value, 0)    // partition 0
	value = binary.BigEndian.AppendUint64(value, 4200) // marker offset
	value = binary.BigEndian.AppendUint32(value, 0)    // no missing partition

	cut, err := columnar.DecodeBarrierCut(key, value)
	if err != nil {
		panic(err)
	}
	offset, _ := cut.Offset(columnar.TopicPartition{Topic: "orders", Partition: 0})
	fmt.Println(cut.Group, cut.Epoch, cut.Status, offset)
	// Output: audit 7 complete 4200
}
