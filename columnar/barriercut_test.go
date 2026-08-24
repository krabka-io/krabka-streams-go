package columnar

import (
	"reflect"
	"strings"
	"testing"
)

// seekCountingConsumer counts the seeks of the reader, so a test can state
// that a second read resumes instead of reading the topic again.
type seekCountingConsumer struct {
	*mockConsumer
	seeks int
}

func (c *seekCountingConsumer) Seek(partition TopicPartition, offset int64) error {
	c.seeks++
	return c.mockConsumer.Seek(partition, offset)
}

func barrierStatePartition(partition int) TopicPartition {
	return TopicPartition{Topic: BarrierStateTopic, Partition: partition}
}

func epochsOf(cuts []BarrierCut) []int64 {
	epochs := make([]int64, len(cuts))
	for index, cut := range cuts {
		epochs[index] = cut.Epoch
	}
	return epochs
}

func TestReadsCompleteCutsAndSkipsTheOtherRecords(t *testing.T) {
	partialCut := testCut{
		group: "audit", epoch: 2, status: CutPartial, triggeredAt: 20, completedAt: 21,
		topics:  []cutTopicOffsets{{topic: "in", offsets: []cutPartitionOffset{{partition: 0, offset: 30}}}},
		missing: []TopicPartition{{Topic: "in", Partition: 1}},
	}
	consumer := newMockConsumer()
	consumer.polls = []map[TopicPartition][]ConsumedRecord{{
		barrierStatePartition(0): {
			NewConsumedRecord(barrierStateKey(barrierKindGroup, "audit", -1), []byte{0, 0}, 1, 0, 0),
			oneTopicCut("audit", 1, "in", 0, 10).record(0, 1),
			partialCut.record(0, 2),
			oneTopicCut("shadow", 3, "in", 0, 70).record(0, 3),
		},
		barrierStatePartition(1): {
			NewConsumedRecord(barrierStateKey(barrierKindInjectionStart, "audit", 4), []byte{0, 0}, 1, 1, 0),
			oneTopicCut("audit", 4, "in", 0, 90).record(1, 1),
		},
	}}
	reader, err := NewCutReader(consumer, 2)
	if err != nil {
		t.Fatal(err)
	}

	latest, err := reader.LatestCompleteCut(t.Context(), "audit")
	if err != nil {
		t.Fatal(err)
	}

	expected := oneTopicCut("audit", 4, "in", 0, 90).expected()
	if !reflect.DeepEqual(*latest, expected) {
		t.Fatalf("unexpected latest cut %+v", *latest)
	}
	assigned := []TopicPartition{barrierStatePartition(0), barrierStatePartition(1)}
	if !reflect.DeepEqual(consumer.assigned, assigned) {
		t.Fatalf("unexpected assignment %v", consumer.assigned)
	}
	expectedSeeks := map[TopicPartition]int64{barrierStatePartition(0): 0, barrierStatePartition(1): 0}
	if !reflect.DeepEqual(consumer.sought, expectedSeeks) {
		t.Fatalf("unexpected seeks %v", consumer.sought)
	}
	all, err := reader.CompleteCutsAfter(t.Context(), "audit", -1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochsOf(all), []int64{1, 4}) {
		t.Fatalf("unexpected epochs %v, the partial cut must not be alignable", epochsOf(all))
	}
	after, err := reader.CompleteCutsAfter(t.Context(), "audit", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochsOf(after), []int64{4}) {
		t.Fatalf("unexpected epochs after epoch 1 %v", epochsOf(after))
	}
	other, err := reader.CompleteCutsAfter(t.Context(), "shadow", -1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochsOf(other), []int64{3}) {
		t.Fatalf("unexpected epochs of the other group %v", epochsOf(other))
	}
}

func TestTombstoneDropsACut(t *testing.T) {
	cut := oneTopicCut("audit", 1, "in", 0, 10)
	consumer := newMockConsumer()
	consumer.polls = []map[TopicPartition][]ConsumedRecord{{
		barrierStatePartition(0): {cut.record(0, 0), cut.tombstone(0, 1)},
	}}
	reader, err := NewCutReader(consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	latest, err := reader.LatestCompleteCut(t.Context(), "audit")
	if err != nil {
		t.Fatal(err)
	}

	if latest != nil {
		t.Fatalf("a deleted cut must not be returned, got %+v", *latest)
	}
}

func TestSecondReadResumesWhereTheFirstStopped(t *testing.T) {
	consumer := &seekCountingConsumer{mockConsumer: newMockConsumer()}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{{
		barrierStatePartition(0): {oneTopicCut("audit", 1, "in", 0, 10).record(0, 0)},
	}}
	reader, err := NewCutReader(consumer, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.LatestCompleteCut(t.Context(), "audit"); err != nil {
		t.Fatal(err)
	}
	seeksAfterFirstRead := consumer.seeks
	consumer.polls = []map[TopicPartition][]ConsumedRecord{{
		barrierStatePartition(0): {oneTopicCut("audit", 2, "in", 0, 20).record(0, 1)},
	}}

	latest, err := reader.LatestCompleteCut(t.Context(), "audit")
	if err != nil {
		t.Fatal(err)
	}

	if latest == nil || latest.Epoch != 2 {
		t.Fatalf("the reader must pick up the newer cut, got %+v", latest)
	}
	if seeksAfterFirstRead != 1 || consumer.seeks != 1 {
		t.Fatalf("the reader must seek once, then resume; seeks %d", consumer.seeks)
	}
	both, err := reader.CompleteCutsAfter(t.Context(), "audit", -1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(epochsOf(both), []int64{1, 2}) {
		t.Fatalf("the reader must keep the cuts it read, got %v", epochsOf(both))
	}
}

func TestMalformedCutRecordFailsTheRead(t *testing.T) {
	consumer := newMockConsumer()
	consumer.polls = []map[TopicPartition][]ConsumedRecord{{
		barrierStatePartition(0): {
			NewConsumedRecord(barrierStateKey(barrierKindCut, "audit", 1), []byte{0, 0, 1}, 1, 0, 0),
		},
	}}
	reader, err := NewCutReader(consumer, 1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reader.LatestCompleteCut(t.Context(), "audit")

	if err == nil || !strings.Contains(err.Error(), "truncated barrier state cut value") {
		t.Fatalf("expected the format error, got %v", err)
	}
}

func TestRejectsAnUnusableCutReader(t *testing.T) {
	cases := []struct {
		name       string
		consumer   Consumer
		partitions int
		options    []CutReaderOption
		message    string
	}{
		{name: "no consumer", partitions: 1, message: "a cut reader needs a consumer"},
		{
			name: "no partition", consumer: newMockConsumer(),
			message: "a cut reader needs at least one partition",
		},
		{
			name: "no idle poll", consumer: newMockConsumer(), partitions: 1,
			options: []CutReaderOption{WithCutIdlePolls(0)},
			message: "a cut reader needs at least one idle poll",
		},
		{
			name: "no topic", consumer: newMockConsumer(), partitions: 1,
			options: []CutReaderOption{WithCutTopic("")},
			message: "a cut reader needs a topic",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			reader, err := NewCutReader(testCase.consumer, testCase.partitions, testCase.options...)

			if reader != nil {
				t.Fatal("an unusable reader must not be returned")
			}
			if err == nil || err.Error() != testCase.message {
				t.Fatalf("unexpected error %v", err)
			}
		})
	}
}
