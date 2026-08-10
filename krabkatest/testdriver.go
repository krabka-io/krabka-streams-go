package krabkatest

import (
	"fmt"

	"github.com/krabka-io/krabka-streams-go/columnar"
)

// ColumnarTestDriver runs a built columnar topology in-process and queues the
// produced records per topic. It is not safe for concurrent use.
//
// PipeInput and PipeBatch differ in an important way for BlobCodec
// topologies: each PipeInput call is its own batch, so per-batch operators
// such as GroupBy see one record at a time. Use PipeBatch when the test is
// about batch behavior.
//
// The driver holds no Arrow memory of its own, since every batch is created
// and released inside RunBatch.
type ColumnarTestDriver struct {
	topology    *columnar.BuiltTopology
	outputs     map[string][]columnar.ProduceRecord
	nextOffsets map[topicPartition]int64
	faults      []error
}

type topicPartition struct {
	topic     string
	partition int
}

// NewColumnarTestDriver creates a driver over a built topology.
func NewColumnarTestDriver(topology *columnar.BuiltTopology) *ColumnarTestDriver {
	return &ColumnarTestDriver{
		topology:    topology,
		outputs:     map[string][]columnar.ProduceRecord{},
		nextOffsets: map[topicPartition]int64{},
	}
}

// PipeInput runs one record as a single-record batch. Offsets start at 0 per
// topic-partition and increment.
func (d *ColumnarTestDriver) PipeInput(topic string, partition int, key, value []byte, timestamp int64, headers ...columnar.RecordHeader) error {
	position := topicPartition{topic: topic, partition: partition}
	offset := d.nextOffsets[position]
	err := d.PipeBatch(topic, []columnar.ConsumedRecord{
		columnar.NewConsumedRecord(key, value, timestamp, partition, offset, headers...)})
	if err != nil {
		return err
	}
	d.nextOffsets[position] = offset + 1
	return nil
}

// PipeBatch runs a whole record list as one batch. Offsets are whatever the
// records carry.
func (d *ColumnarTestDriver) PipeBatch(topic string, records []columnar.ConsumedRecord) error {
	if len(d.faults) > 0 {
		fault := d.faults[0]
		d.faults = d.faults[1:]
		return fault
	}
	produced, err := d.topology.RunBatch(topic, records)
	if err != nil {
		return err
	}
	for _, output := range produced {
		d.outputs[output.Topic] = append(d.outputs[output.Topic], output.Record)
	}
	return nil
}

// FailNext returns one deterministic fault before the next batch evaluation.
func (d *ColumnarTestDriver) FailNext(fault error) {
	d.faults = append(d.faults, fault)
}

// IsOutputEmpty reports whether a sink topic's queue is empty.
func (d *ColumnarTestDriver) IsOutputEmpty(topic string) bool {
	return d.OutputSize(topic) == 0
}

// OutputSize returns the queue depth of a sink topic.
func (d *ColumnarTestDriver) OutputSize(topic string) int {
	return len(d.outputs[topic])
}

// ReadOutput removes and returns the oldest record queued for a topic.
func (d *ColumnarTestDriver) ReadOutput(topic string) (columnar.ProduceRecord, error) {
	queue := d.outputs[topic]
	if len(queue) == 0 {
		return columnar.ProduceRecord{}, fmt.Errorf("topic `%s` has no output", topic)
	}
	d.outputs[topic] = queue[1:]
	return queue[0], nil
}

// DrainOutput removes and returns everything queued for a topic.
func (d *ColumnarTestDriver) DrainOutput(topic string) []columnar.ProduceRecord {
	drained := d.outputs[topic]
	delete(d.outputs, topic)
	return drained
}
