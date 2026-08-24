package columnar

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

// barrierWriter builds the frozen big-endian layout of the barrier state
// topic. The tests encode with it, so a decoder that drifts from the design
// document fails here.
type barrierWriter struct {
	data []byte
}

func (w *barrierWriter) int8(value int8) *barrierWriter {
	w.data = append(w.data, byte(value))
	return w
}

func (w *barrierWriter) int16(value int16) *barrierWriter {
	w.data = binary.BigEndian.AppendUint16(w.data, uint16(value))
	return w
}

func (w *barrierWriter) int32(value int32) *barrierWriter {
	w.data = binary.BigEndian.AppendUint32(w.data, uint32(value))
	return w
}

func (w *barrierWriter) int64(value int64) *barrierWriter {
	w.data = binary.BigEndian.AppendUint64(w.data, uint64(value))
	return w
}

func (w *barrierWriter) string(value string) *barrierWriter {
	w.int16(int16(len(value)))
	w.data = append(w.data, value...)
	return w
}

// cutPartitionOffset is one partition and the offset of its marker.
type cutPartitionOffset struct {
	partition int
	offset    int64
}

// cutTopicOffsets is one topic of a cut and its partition offsets.
type cutTopicOffsets struct {
	topic   string
	offsets []cutPartitionOffset
}

func barrierStateKey(kind int16, group string, epoch int64) []byte {
	writer := &barrierWriter{}
	writer.int16(barrierStateKeyVersion).int16(kind).string(group).int64(epoch)
	return writer.data
}

func barrierCutValue(triggeredAt, completedAt int64, status CutStatus, topics []cutTopicOffsets, missing []TopicPartition) []byte {
	writer := &barrierWriter{}
	writer.int16(barrierCutValueVersion).int64(triggeredAt).int64(completedAt).int8(int8(status))
	writer.int32(int32(len(topics)))
	for _, topic := range topics {
		writer.string(topic.topic).int32(int32(len(topic.offsets)))
		for _, entry := range topic.offsets {
			writer.int32(int32(entry.partition)).int64(entry.offset)
		}
	}
	writer.int32(int32(len(missing)))
	for _, partition := range missing {
		writer.string(partition.Topic).int32(int32(partition.Partition))
	}
	return writer.data
}

// testCut describes one cut record of the barrier state topic.
type testCut struct {
	group       string
	epoch       int64
	status      CutStatus
	triggeredAt int64
	completedAt int64
	topics      []cutTopicOffsets
	missing     []TopicPartition
}

// record encodes the cut into a record of the barrier state topic.
func (c testCut) record(partition int, offset int64) ConsumedRecord {
	return NewConsumedRecord(
		barrierStateKey(barrierKindCut, c.group, c.epoch),
		barrierCutValue(c.triggeredAt, c.completedAt, c.status, c.topics, c.missing),
		c.triggeredAt, partition, offset)
}

// tombstone encodes the deletion of the cut.
func (c testCut) tombstone(partition int, offset int64) ConsumedRecord {
	return NewConsumedRecord(
		barrierStateKey(barrierKindCut, c.group, c.epoch), nil, c.triggeredAt, partition, offset)
}

// expected is the cut a reader must produce for the record.
func (c testCut) expected() BarrierCut {
	offsets := map[TopicPartition]int64{}
	for _, topic := range c.topics {
		for _, entry := range topic.offsets {
			offsets[TopicPartition{Topic: topic.topic, Partition: entry.partition}] = entry.offset
		}
	}
	return BarrierCut{
		Group:       c.group,
		Epoch:       c.epoch,
		TriggeredAt: c.triggeredAt,
		CompletedAt: c.completedAt,
		Status:      c.status,
		Offsets:     offsets,
		Missing:     c.missing,
	}
}

// oneTopicCut is a complete cut over one topic with one partition.
func oneTopicCut(group string, epoch int64, topic string, partition int, offset int64) testCut {
	return testCut{
		group: group, epoch: epoch, status: CutComplete, triggeredAt: epoch * 10, completedAt: epoch*10 + 1,
		topics: []cutTopicOffsets{{topic: topic, offsets: []cutPartitionOffset{{partition: partition, offset: offset}}}},
	}
}

func TestDecodesBarrierStateRecords(t *testing.T) {
	completeValue := barrierCutValue(11, 22, CutComplete, []cutTopicOffsets{
		{topic: "orders", offsets: []cutPartitionOffset{{partition: 0, offset: 100}, {partition: 1, offset: 250}}},
		{topic: "payments", offsets: []cutPartitionOffset{{partition: 0, offset: 7}}},
	}, nil)
	partialValue := barrierCutValue(31, 42, CutPartial, []cutTopicOffsets{
		{topic: "orders", offsets: []cutPartitionOffset{{partition: 0, offset: 100}}},
	}, []TopicPartition{{Topic: "orders", Partition: 1}})

	cases := []struct {
		name     string
		key      []byte
		value    []byte
		expected *BarrierCut
	}{
		{
			name:  "complete cut",
			key:   barrierStateKey(barrierKindCut, "audit", 5),
			value: completeValue,
			expected: &BarrierCut{
				Group: "audit", Epoch: 5, TriggeredAt: 11, CompletedAt: 22, Status: CutComplete,
				Offsets: map[TopicPartition]int64{
					{Topic: "orders", Partition: 0}:   100,
					{Topic: "orders", Partition: 1}:   250,
					{Topic: "payments", Partition: 0}: 7,
				},
			},
		},
		{
			name:  "partial cut names its missing partitions",
			key:   barrierStateKey(barrierKindCut, "audit", 6),
			value: partialValue,
			expected: &BarrierCut{
				Group: "audit", Epoch: 6, TriggeredAt: 31, CompletedAt: 42, Status: CutPartial,
				Offsets: map[TopicPartition]int64{{Topic: "orders", Partition: 0}: 100},
				Missing: []TopicPartition{{Topic: "orders", Partition: 1}},
			},
		},
		{
			name:  "group record",
			key:   barrierStateKey(barrierKindGroup, "audit", -1),
			value: (&barrierWriter{}).int16(0).int32(0).int64(-1).int32(3).int64(4).data,
		},
		{
			name:  "injection start record",
			key:   barrierStateKey(barrierKindInjectionStart, "audit", 7),
			value: (&barrierWriter{}).int16(0).int32(2).int64(11).int32(0).data,
		},
		{
			name: "cut tombstone",
			key:  barrierStateKey(barrierKindCut, "audit", 8),
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cut, err := DecodeBarrierCut(testCase.key, testCase.value)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cut, testCase.expected) {
				t.Fatalf("unexpected cut %+v", cut)
			}
		})
	}
}

func TestRejectsMalformedBarrierStateRecords(t *testing.T) {
	validKey := barrierStateKey(barrierKindCut, "audit", 5)
	validValue := barrierCutValue(1, 2, CutComplete, []cutTopicOffsets{
		{topic: "orders", offsets: []cutPartitionOffset{{partition: 0, offset: 9}}},
	}, nil)

	cases := []struct {
		name    string
		key     []byte
		value   []byte
		part    string
		message string
	}{
		{
			name: "truncated key", key: []byte{0}, value: validValue,
			part: "key", message: "truncated barrier state key",
		},
		{
			name: "unsupported key version",
			key:  (&barrierWriter{}).int16(1).int16(barrierKindCut).string("audit").int64(5).data,
			part: "key", message: "unsupported barrier state key version 1",
		},
		{
			name: "trailing bytes in the key", key: append(append([]byte{}, validKey...), 0), value: validValue,
			part: "key", message: "trailing bytes in barrier state key",
		},
		{
			name: "negative group length",
			key:  (&barrierWriter{}).int16(0).int16(barrierKindCut).int16(-1).int64(5).data,
			part: "key", message: "negative string length -1 in barrier state key",
		},
		{
			name: "truncated cut value", key: validKey, value: []byte{0},
			part: "cut value", message: "truncated barrier state cut value",
		},
		{
			name: "unsupported cut value version", key: validKey,
			value: (&barrierWriter{}).int16(1).int64(1).int64(2).int8(0).int32(0).int32(0).data,
			part:  "cut value", message: "unsupported barrier state cut value version 1",
		},
		{
			name: "unknown cut status", key: validKey,
			value: (&barrierWriter{}).int16(0).int64(1).int64(2).int8(2).int32(0).int32(0).data,
			part:  "cut value", message: "unknown barrier cut status 2",
		},
		{
			name: "negative topic count", key: validKey,
			value: (&barrierWriter{}).int16(0).int64(1).int64(2).int8(0).int32(-1).data,
			part:  "cut value", message: "negative array length -1 in barrier state cut value",
		},
		{
			name: "trailing bytes in the cut value", key: validKey,
			value: append(append([]byte{}, validValue...), 0),
			part:  "cut value", message: "trailing bytes in barrier state cut value",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			cut, err := DecodeBarrierCut(testCase.key, testCase.value)
			if cut != nil {
				t.Fatalf("malformed bytes must decode to no cut, got %+v", cut)
			}
			var formatError *BarrierFormatError
			if !errors.As(err, &formatError) {
				t.Fatalf("expected a barrier format error, got %v", err)
			}
			if formatError.Part != testCase.part || formatError.Error() != testCase.message {
				t.Fatalf("unexpected error %q on part %q", formatError.Error(), formatError.Part)
			}
		})
	}
}

func TestCutStatusNamesItself(t *testing.T) {
	cases := []struct {
		status   CutStatus
		expected string
	}{
		{status: CutComplete, expected: "complete"},
		{status: CutPartial, expected: "partial"},
		{status: CutStatus(9), expected: "CutStatus(9)"},
	}
	for _, testCase := range cases {
		if testCase.status.String() != testCase.expected {
			t.Fatalf("unexpected status name %q", testCase.status.String())
		}
	}
}

func TestCutReportsCompletenessAndOffsets(t *testing.T) {
	cut := oneTopicCut("audit", 5, "in", 1, 40).expected()

	if !cut.Complete() {
		t.Fatal("a cut with no missing partition is complete")
	}
	offset, ok := cut.Offset(TopicPartition{Topic: "in", Partition: 1})
	if !ok || offset != 40 {
		t.Fatalf("unexpected offset %d present %v", offset, ok)
	}
	if _, ok := cut.Offset(TopicPartition{Topic: "in", Partition: 2}); ok {
		t.Fatal("a partition outside the cut has no offset")
	}
}
