package columnar

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type mockConsumer struct {
	assigned   []TopicPartition
	sought     map[TopicPartition]int64
	polls      []map[TopicPartition][]ConsumedRecord
	committed  map[TopicPartition]int64
	subscribed []string
	listener   RebalanceListener
}

func newMockConsumer() *mockConsumer {
	return &mockConsumer{sought: map[TopicPartition]int64{}, committed: map[TopicPartition]int64{}}
}

func (c *mockConsumer) Assign(partitions []TopicPartition) error {
	c.assigned = partitions
	return nil
}

func (c *mockConsumer) Seek(partition TopicPartition, offset int64) error {
	c.sought[partition] = offset
	return nil
}

func (c *mockConsumer) Poll(ctx context.Context, timeout time.Duration) (map[TopicPartition][]ConsumedRecord, error) {
	if len(c.polls) == 0 {
		return map[TopicPartition][]ConsumedRecord{}, nil
	}
	next := c.polls[0]
	c.polls = c.polls[1:]
	return next, nil
}

func (c *mockConsumer) CommitOffsets(ctx context.Context, offsets map[TopicPartition]int64) error {
	maps.Copy(c.committed, offsets)
	return nil
}

func (c *mockConsumer) Subscribe(topics []string, listener RebalanceListener) error {
	c.subscribed = topics
	c.listener = listener
	return nil
}

func (c *mockConsumer) GroupMetadata() any { return "group-metadata" }

type mockProducer struct {
	history  []ProducedToTopic
	failNext error
}

func (p *mockProducer) Send(topic string, record ProduceRecord, done func(error)) {
	if p.failNext != nil {
		err := p.failNext
		p.failNext = nil
		done(err)
		return
	}
	p.history = append(p.history, ProducedToTopic{Topic: topic, Record: record})
	done(nil)
}

func filteringTopology(t *testing.T, mem memory.Allocator, codec BatchCodec) *Topology {
	t.Helper()
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	filter, err := topology.AddOperator("filter", Filter(mem, func(batch arrow.Record, row int) bool {
		return int64Column(t, batch, "amount").Value(row) > 4
	}), source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, filter); err != nil {
		t.Fatal(err)
	}
	return topology
}

func TestFetchesProcessesProducesAndAdvancesOffset(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"a", "b", "c"}, []int64{1, 5, 9})
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	payload.Release()
	consumer := newMockConsumer()
	producer := &mockProducer{}
	codec := NewBlobCodec(mem)
	built, err := filteringTopology(t, mem, codec).Build()
	if err != nil {
		t.Fatal(err)
	}
	partition := TopicPartition{Topic: "in", Partition: 0}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{partition: {NewConsumedRecord(nil, data, 0, 0, 100)}},
	}

	next, err := RunPartitionOnce(t.Context(), built, consumer, producer, "in", 0, 100, 0)
	if err != nil {
		t.Fatal(err)
	}

	if next != 101 {
		t.Fatalf("unexpected next offset %d", next)
	}
	if consumer.committed[partition] != 101 {
		t.Fatalf("unexpected committed offset %d", consumer.committed[partition])
	}
	if consumer.sought[partition] != 100 {
		t.Fatal("the runner must seek to the requested offset")
	}
	if len(producer.history) != 1 || producer.history[0].Topic != "out" {
		t.Fatalf("unexpected producer history %+v", producer.history)
	}
	result, err := NewIPCSerde(mem).Deserialize("", producer.history[0].Record.Value)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.NumRows() != 2 {
		t.Fatalf("unexpected result rows %d", result.NumRows())
	}
}

func TestEmptyPollKeepsOffsetAndDoesNotProduce(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	producer := &mockProducer{}
	built, err := filteringTopology(t, mem, NewBlobCodec(mem)).Build()
	if err != nil {
		t.Fatal(err)
	}

	next, err := RunPartitionOnce(t.Context(), built, consumer, producer, "in", 0, 42, 0)
	if err != nil {
		t.Fatal(err)
	}

	if next != 42 {
		t.Fatalf("unexpected next offset %d", next)
	}
	if len(producer.history) != 0 {
		t.Fatal("an empty poll must not produce")
	}
}

func TestGroupRunnerSubscribesToAllSources(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in-a", "in-b"}, NewBlobCodec(mem))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddPassThroughSink("sink", "out", source); err != nil {
		t.Fatal(err)
	}

	runner, err := NewGroupRunner(topology, consumer, &mockProducer{})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()

	if !reflect.DeepEqual(consumer.subscribed, []string{"in-a", "in-b"}) {
		t.Fatalf("unexpected subscription %v", consumer.subscribed)
	}
	if consumer.listener != RebalanceListener(runner) {
		t.Fatal("the runner must be the rebalance listener")
	}
}

type recordingStateStore struct {
	loaded []int
	saved  []int
}

func (s *recordingStateStore) Load(partition int) (map[string][]byte, error) {
	s.loaded = append(s.loaded, partition)
	return map[string][]byte{}, nil
}

func (s *recordingStateStore) Save(partition int, snapshot map[string][]byte) error {
	s.saved = append(s.saved, partition)
	return nil
}

func TestGroupRunnerLoadsAndSavesStateAtRebalanceBoundaries(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, NewBlobCodec(mem))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddPassThroughSink("sink", "out", source); err != nil {
		t.Fatal(err)
	}
	store := &recordingStateStore{}
	runner, err := NewGroupRunner(topology, consumer, &mockProducer{}, WithStateStore(store))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	partition := TopicPartition{Topic: "in", Partition: 3}

	runner.OnPartitionsAssigned([]TopicPartition{partition})
	runner.OnPartitionsRevoked([]TopicPartition{partition})

	if !reflect.DeepEqual(store.loaded, []int{3}) {
		t.Fatalf("unexpected loads %v", store.loaded)
	}
	if !reflect.DeepEqual(store.saved, []int{3}) {
		t.Fatalf("unexpected saves %v", store.saved)
	}
}

func TestSendAllSurfacesBrokerErrors(t *testing.T) {
	producer := &mockProducer{failNext: fmt.Errorf("send failed")}

	err := SendAll([]ProducedToTopic{
		{Topic: "out", Record: NewProduceRecord(nil, []byte("value"), 1)}}, producer)

	if err == nil || !strings.Contains(err.Error(), "send failed") {
		t.Fatalf("expected the broker error, got %v", err)
	}
}

type failingStatefulProcessor struct {
	calls byte
}

func (p *failingStatefulProcessor) Process(ctx *Context, batch arrow.Record) error {
	p.calls++
	return fmt.Errorf("broken batch")
}

func (p *failingStatefulProcessor) Snapshot() ([]byte, error) { return []byte{p.calls}, nil }

func (p *failingStatefulProcessor) Restore(snapshot []byte) error {
	p.calls = snapshot[0]
	return nil
}

func TestDeadLettersFailedBatchesAndReportsMetrics(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"a"}, []int64{1})
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	payload.Release()
	codec := NewBlobCodec(mem)
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	failing, err := topology.AddProcessor("failing",
		func() Processor { return &failingStatefulProcessor{} }, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, failing); err != nil {
		t.Fatal(err)
	}
	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	consumer := newMockConsumer()
	producer := &mockProducer{}
	partition := TopicPartition{Topic: "in", Partition: 0}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{partition: {NewConsumedRecord([]byte("k"), data, 0, 0, 0,
			RecordHeader{Key: "trace-id", Value: []byte("abc")})}},
	}
	metrics := NewMetrics()

	offsets, err := runGroupOnce(t.Context(), built, consumer, producer, 0,
		DeadLetterPolicy("dlq"), metrics)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(offsets, map[TopicPartition]int64{partition: 1}) {
		t.Fatalf("unexpected offsets %v", offsets)
	}
	if len(producer.history) != 1 || producer.history[0].Topic != "dlq" {
		t.Fatalf("unexpected producer history %+v", producer.history)
	}
	deadLettered := producer.history[0].Record
	if !bytes.Equal(deadLettered.Value, data) {
		t.Fatal("the dead letter must carry the original value")
	}
	foundTrace := false
	for _, header := range deadLettered.Headers {
		if header.Key == "trace-id" && bytes.Equal(header.Value, []byte("abc")) {
			foundTrace = true
		}
	}
	if !foundTrace {
		t.Fatal("original headers must be preserved on the dead letter")
	}
	snapshot := metrics.Snapshot()
	snapshot.ProcessingNanos = 0
	expected := MetricsSnapshot{Batches: 1, InputRecords: 1, Failures: 1, DeadLetterRecords: 1}
	if snapshot != expected {
		t.Fatalf("unexpected metrics %+v", snapshot)
	}
	state, err := built.SnapshotPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(state) != 0 {
		t.Fatal("failed partition state must be rolled back")
	}
}

type retriableError struct{}

func (retriableError) Error() string   { return "registry fetch pending" }
func (retriableError) Retriable() bool { return true }

func TestRethrowsRetriableFailuresInsteadOfDeadLettering(t *testing.T) {
	mem := checkedAllocator(t)
	payload := transactions(mem, []string{"a"}, []int64{1})
	data, _ := NewIPCSerde(mem).Serialize("", payload)
	payload.Release()
	codec := NewBlobCodec(mem)
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", []string{"in"}, codec)
	if err != nil {
		t.Fatal(err)
	}
	failing, err := topology.AddProcessor("retriable", func() Processor {
		return ProcessorFunc(func(ctx *Context, batch arrow.Record) error {
			return fmt.Errorf("decode: %w", retriableError{})
		})
	}, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, failing); err != nil {
		t.Fatal(err)
	}
	built, err := topology.Build()
	if err != nil {
		t.Fatal(err)
	}
	consumer := newMockConsumer()
	producer := &mockProducer{}
	partition := TopicPartition{Topic: "in", Partition: 0}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{partition: {NewConsumedRecord(nil, data, 0, 0, 0)}},
	}

	_, err = runGroupOnce(t.Context(), built, consumer, producer, 0,
		DeadLetterPolicy("dlq"), NewMetrics())

	if err == nil || !strings.Contains(err.Error(), "registry fetch pending") {
		t.Fatalf("expected the retriable error to surface, got %v", err)
	}
	if len(producer.history) != 0 {
		t.Fatal("a retriable failure must not produce")
	}
	if len(consumer.committed) != 0 {
		t.Fatal("a retriable failure must not commit")
	}
}
