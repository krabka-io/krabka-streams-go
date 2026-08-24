package columnar

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// countingProcessor counts the rows it processed and snapshots the count, so
// a test can read the state a barrier stored.
type countingProcessor struct {
	rows int64
}

func (p *countingProcessor) Process(ctx *Context, batch arrow.Record) error {
	p.rows += batch.NumRows()
	ctx.Forward(batch)
	return nil
}

func (p *countingProcessor) Snapshot() ([]byte, error) {
	return binary.BigEndian.AppendUint64(nil, uint64(p.rows)), nil
}

func (p *countingProcessor) Restore(snapshot []byte) error {
	if len(snapshot) != 8 {
		return fmt.Errorf("unexpected counter snapshot of %d bytes", len(snapshot))
	}
	p.rows = int64(binary.BigEndian.Uint64(snapshot))
	return nil
}

func counterState(rows int64) map[string][]byte {
	return map[string][]byte{"counter": binary.BigEndian.AppendUint64(nil, uint64(rows))}
}

// countingTopology counts every row it reads and copies it to the sink.
func countingTopology(t *testing.T, mem memory.Allocator, topics ...string) *Topology {
	t.Helper()
	codec := NewBlobCodec(mem)
	topology := NewTopology(mem)
	source, err := topology.AddSource("source", topics, codec)
	if err != nil {
		t.Fatal(err)
	}
	counter, err := topology.AddProcessor("counter",
		func() Processor { return &countingProcessor{} }, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := topology.AddSink("sink", "out", codec, counter); err != nil {
		t.Fatal(err)
	}
	return topology
}

// oneRowRecord is one consumed record that carries a single-row batch.
func oneRowRecord(t *testing.T, mem memory.Allocator, topic string, partition int, offset int64) ConsumedRecord {
	t.Helper()
	payload := transactions(mem, []string{"a"}, []int64{1})
	data, err := NewIPCSerde(mem).Serialize("", payload)
	payload.Release()
	if err != nil {
		t.Fatal(err)
	}
	return NewConsumedRecord([]byte(topic), data, 0, partition, offset)
}

func rowRecords(t *testing.T, mem memory.Allocator, topic string, partition int, offsets ...int64) []ConsumedRecord {
	t.Helper()
	records := make([]ConsumedRecord, len(offsets))
	for index, offset := range offsets {
		records[index] = oneRowRecord(t, mem, topic, partition, offset)
	}
	return records
}

// cutReaderWith serves the cuts from a consumer of its own, as a runner needs.
func cutReaderWith(t *testing.T, cuts ...testCut) *CutReader {
	t.Helper()
	consumer := newMockConsumer()
	records := make([]ConsumedRecord, len(cuts))
	for index, cut := range cuts {
		records[index] = cut.record(0, int64(index))
	}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{barrierStatePartition(0): records},
	}
	reader, err := NewCutReader(consumer, 1)
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

func TestHoldsTheRecordsAfterTheCutAndSnapshotsAtTheBarrier(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	store := newRecordingStateStore()
	cut := oneTopicCut("audit", 5, "in", 0, 2)
	var barriers []Barrier
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, &mockProducer{},
		WithStateStore(store),
		WithBarrierGroup("audit", cutReaderWith(t, cut)),
		WithBarrierListener(func(barrier Barrier) { barriers = append(barriers, barrier) }))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	partition := TopicPartition{Topic: "in", Partition: 0}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{partition: rowRecords(t, mem, "in", 0, 0, 1, 2, 3)},
	}

	first, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(first, map[TopicPartition]int64{partition: 2}) {
		t.Fatalf("the committed position must be the cut, got %v", first)
	}
	expectedBarrier := Barrier{
		Cut:        cut.expected(),
		Partitions: []int{0},
		Offsets:    map[TopicPartition]int64{partition: 2},
	}
	if !reflect.DeepEqual(barriers, []Barrier{expectedBarrier}) {
		t.Fatalf("unexpected barriers %+v", barriers)
	}
	if !reflect.DeepEqual(store.saved, []snapshotKey{{partition: 0, epoch: 5}}) {
		t.Fatalf("the snapshot must be keyed by the epoch, got %v", store.saved)
	}
	if !reflect.DeepEqual(store.snapshots[snapshotKey{partition: 0, epoch: 5}], counterState(2)) {
		t.Fatal("the snapshot must hold the state at the cut, which is two rows")
	}

	second, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(second, map[TopicPartition]int64{partition: 4}) {
		t.Fatalf("the held records must be processed next, got %v", second)
	}
	state, err := runner.Topology().SnapshotPartition(0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state, counterState(4)) {
		t.Fatal("every record must be processed once the barrier passed")
	}
	if len(store.saved) != 1 {
		t.Fatalf("a round without a barrier must not snapshot, got %v", store.saved)
	}
}

func TestBarrierWaitsForEveryPartitionOfTheCut(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	store := newRecordingStateStore()
	cut := testCut{
		group: "audit", epoch: 5, status: CutComplete, triggeredAt: 50, completedAt: 51,
		topics: []cutTopicOffsets{{topic: "in", offsets: []cutPartitionOffset{
			{partition: 0, offset: 2}, {partition: 1, offset: 1}}}},
	}
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, &mockProducer{},
		WithStateStore(store), WithBarrierGroup("audit", cutReaderWith(t, cut)))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	first := TopicPartition{Topic: "in", Partition: 0}
	second := TopicPartition{Topic: "in", Partition: 1}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{first: rowRecords(t, mem, "in", 0, 0, 1, 2)},
		{second: rowRecords(t, mem, "in", 1, 0)},
	}

	early, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(store.saved) != 0 {
		t.Fatalf("a partition short of its cut must hold the barrier, saved %v", store.saved)
	}
	if !reflect.DeepEqual(early, map[TopicPartition]int64{first: 2}) {
		t.Fatalf("unexpected offsets before the barrier %v", early)
	}

	fired, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[TopicPartition]int64{first: 2, second: 1}
	if !reflect.DeepEqual(fired, expected) {
		t.Fatalf("the barrier must commit the cut of every aligned partition, got %v", fired)
	}
	if !reflect.DeepEqual(store.saved, []snapshotKey{{partition: 0, epoch: 5}, {partition: 1, epoch: 5}}) {
		t.Fatalf("every partition must snapshot at the barrier, got %v", store.saved)
	}
}

func TestBarrierDropsAPartitionTheRunnerLost(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	store := newRecordingStateStore()
	cut := testCut{
		group: "audit", epoch: 5, status: CutComplete, triggeredAt: 50, completedAt: 51,
		topics: []cutTopicOffsets{{topic: "in", offsets: []cutPartitionOffset{
			{partition: 0, offset: 2}, {partition: 1, offset: 1}}}},
	}
	var barriers []Barrier
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, &mockProducer{},
		WithStateStore(store),
		WithBarrierGroup("audit", cutReaderWith(t, cut)),
		WithBarrierListener(func(barrier Barrier) { barriers = append(barriers, barrier) }))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	first := TopicPartition{Topic: "in", Partition: 0}
	second := TopicPartition{Topic: "in", Partition: 1}
	runner.OnPartitionsAssigned([]TopicPartition{first, second})
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{first: rowRecords(t, mem, "in", 0, 0, 1, 2)},
	}
	if _, err := runner.RunOnce(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if len(barriers) != 0 {
		t.Fatal("the barrier must wait for the second partition")
	}
	runner.OnPartitionsRevoked([]TopicPartition{second})

	offsets, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(offsets, map[TopicPartition]int64{first: 2}) {
		t.Fatalf("a lost partition must not hold the barrier, got %v", offsets)
	}
	if len(barriers) != 1 || !reflect.DeepEqual(barriers[0].Partitions, []int{0}) {
		t.Fatalf("unexpected barriers %+v", barriers)
	}
	if !reflect.DeepEqual(store.saved[len(store.saved)-1], snapshotKey{partition: 0, epoch: 5}) {
		t.Fatalf("unexpected saves %v", store.saved)
	}
}

func TestPartialCutNeverAligns(t *testing.T) {
	mem := checkedAllocator(t)
	consumer := newMockConsumer()
	store := newRecordingStateStore()
	partial := testCut{
		group: "audit", epoch: 5, status: CutPartial, triggeredAt: 50, completedAt: 51,
		topics:  []cutTopicOffsets{{topic: "in", offsets: []cutPartitionOffset{{partition: 0, offset: 2}}}},
		missing: []TopicPartition{{Topic: "in", Partition: 1}},
	}
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, &mockProducer{},
		WithStateStore(store), WithBarrierGroup("audit", cutReaderWith(t, partial)))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	partition := TopicPartition{Topic: "in", Partition: 0}
	consumer.polls = []map[TopicPartition][]ConsumedRecord{
		{partition: rowRecords(t, mem, "in", 0, 0, 1, 2, 3)},
	}

	offsets, err := runner.RunOnce(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(offsets, map[TopicPartition]int64{partition: 4}) {
		t.Fatalf("a partial cut must not hold any record, got %v", offsets)
	}
	if len(store.saved) != 0 {
		t.Fatalf("a partial cut must not snapshot, saved %v", store.saved)
	}
}

func TestRestoresTheStateAndSeeksToTheCut(t *testing.T) {
	cases := []struct {
		name    string
		restore func(*testing.T, *GroupRunner) (*BarrierCut, error)
	}{
		{
			name: "to a named epoch",
			restore: func(t *testing.T, runner *GroupRunner) (*BarrierCut, error) {
				return runner.RestoreToEpoch(t.Context(), 5)
			},
		},
		{
			name: "to the latest complete cut",
			restore: func(t *testing.T, runner *GroupRunner) (*BarrierCut, error) {
				return runner.RestoreToLatestCut(t.Context())
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			mem := checkedAllocator(t)
			consumer := newMockConsumer()
			store := newRecordingStateStore()
			store.snapshots[snapshotKey{partition: 0, epoch: 5}] = counterState(9)
			cut := oneTopicCut("audit", 5, "in", 0, 2)
			runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, &mockProducer{},
				WithStateStore(store), WithBarrierGroup("audit", cutReaderWith(t, cut)))
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close()

			restored, err := testCase.restore(t, runner)
			if err != nil {
				t.Fatal(err)
			}

			expected := cut.expected()
			if !reflect.DeepEqual(*restored, expected) {
				t.Fatalf("unexpected cut %+v", *restored)
			}
			partition := TopicPartition{Topic: "in", Partition: 0}
			if !reflect.DeepEqual(consumer.sought, map[TopicPartition]int64{partition: 2}) {
				t.Fatalf("the runner must seek to the cut, got %v", consumer.sought)
			}
			state, err := runner.Topology().SnapshotPartition(0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(state, counterState(9)) {
				t.Fatalf("the runner must restore the snapshot of the epoch, got %v", state)
			}
			if !reflect.DeepEqual(store.loaded, []snapshotKey{{partition: 0, epoch: 5}}) {
				t.Fatalf("unexpected loads %v", store.loaded)
			}
		})
	}
}

func TestRejectsARestoreWithoutACut(t *testing.T) {
	mem := checkedAllocator(t)
	partial := testCut{
		group: "audit", epoch: 5, status: CutPartial, triggeredAt: 50, completedAt: 51,
		topics:  []cutTopicOffsets{{topic: "in", offsets: []cutPartitionOffset{{partition: 0, offset: 2}}}},
		missing: []TopicPartition{{Topic: "in", Partition: 1}},
	}
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), newMockConsumer(), &mockProducer{},
		WithBarrierGroup("audit", cutReaderWith(t, partial)))
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()

	if _, err := runner.RestoreToEpoch(t.Context(), 5); err == nil ||
		!strings.Contains(err.Error(), "no complete cut at epoch 5") {
		t.Fatalf("a partial cut must not be restorable, got %v", err)
	}
	if _, err := runner.RestoreToLatestCut(t.Context()); err == nil ||
		!strings.Contains(err.Error(), "has no complete cut") {
		t.Fatalf("expected the missing-cut error, got %v", err)
	}
}

func TestRestoreRequiresABarrierGroup(t *testing.T) {
	mem := checkedAllocator(t)
	runner, err := NewGroupRunner(countingTopology(t, mem, "in"), newMockConsumer(), &mockProducer{})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()

	_, err = runner.RestoreToLatestCut(t.Context())

	if err == nil || !strings.Contains(err.Error(), "requires a barrier group") {
		t.Fatalf("unexpected error %v", err)
	}
}

// recordingTransactionalProducer notes the transaction calls in the order
// they arrive.
type recordingTransactionalProducer struct {
	mockProducer
	events     *[]string
	failCommit error
}

func (p *recordingTransactionalProducer) BeginTransaction() error {
	*p.events = append(*p.events, "begin")
	return nil
}

func (p *recordingTransactionalProducer) SendOffsets(offsets map[TopicPartition]int64, metadata any) error {
	*p.events = append(*p.events, "send-offsets")
	return nil
}

func (p *recordingTransactionalProducer) CommitTransaction() error {
	*p.events = append(*p.events, "commit")
	return p.failCommit
}

func (p *recordingTransactionalProducer) AbortTransaction() error {
	*p.events = append(*p.events, "abort")
	return nil
}

// orderingStateStore notes its saves beside the transaction calls.
type orderingStateStore struct {
	*recordingStateStore
	events *[]string
}

func (s *orderingStateStore) Save(partition int, epoch int64, snapshot map[string][]byte) error {
	*s.events = append(*s.events, "snapshot")
	return s.recordingStateStore.Save(partition, epoch, snapshot)
}

func TestBarrierCommitRidesInsideTheTransaction(t *testing.T) {
	cases := []struct {
		name       string
		failCommit error
		expected   []string
	}{
		{
			name:     "committed",
			expected: []string{"begin", "snapshot", "send-offsets", "commit"},
		},
		{
			name:       "rolled back",
			failCommit: fmt.Errorf("broker refused the commit"),
			expected:   []string{"begin", "snapshot", "send-offsets", "commit", "abort"},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			mem := checkedAllocator(t)
			var events []string
			consumer := newMockConsumer()
			producer := &recordingTransactionalProducer{events: &events, failCommit: testCase.failCommit}
			store := &orderingStateStore{recordingStateStore: newRecordingStateStore(), events: &events}
			cut := oneTopicCut("audit", 5, "in", 0, 2)
			var barriers []Barrier
			runner, err := NewGroupRunner(countingTopology(t, mem, "in"), consumer, producer,
				WithStateStore(store),
				WithBarrierGroup("audit", cutReaderWith(t, cut)),
				WithBarrierListener(func(barrier Barrier) { barriers = append(barriers, barrier) }))
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close()
			partition := TopicPartition{Topic: "in", Partition: 0}
			consumer.polls = []map[TopicPartition][]ConsumedRecord{
				{partition: rowRecords(t, mem, "in", 0, 0, 1, 2)},
			}

			offsets, err := runner.RunOnceTransactional(t.Context(), 0)

			if !reflect.DeepEqual(events, testCase.expected) {
				t.Fatalf("unexpected transaction events %v", events)
			}
			if testCase.failCommit != nil {
				if err == nil || !strings.Contains(err.Error(), "broker refused the commit") {
					t.Fatalf("expected the commit failure, got %v", err)
				}
				if len(barriers) != 0 {
					t.Fatal("an aborted transaction must not report a barrier")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(offsets, map[TopicPartition]int64{partition: 2}) {
				t.Fatalf("the transaction must commit the cut, got %v", offsets)
			}
			if len(barriers) != 1 || barriers[0].Cut.Epoch != 5 {
				t.Fatalf("unexpected barriers %+v", barriers)
			}
		})
	}
}

func TestRejectsIncompleteBarrierOptions(t *testing.T) {
	mem := checkedAllocator(t)
	reader := cutReaderWith(t)
	cases := []struct {
		name    string
		options []GroupRunnerOption
		message string
	}{
		{
			name:    "a listener without a group",
			options: []GroupRunnerOption{WithBarrierListener(func(Barrier) {})},
			message: "a barrier listener requires a barrier group",
		},
		{
			name:    "a group without a reader",
			options: []GroupRunnerOption{WithBarrierGroup("audit", nil)},
			message: "a barrier group needs both a name and a cut reader",
		},
		{
			name:    "a reader without a group name",
			options: []GroupRunnerOption{WithBarrierGroup("", reader)},
			message: "a barrier group needs both a name and a cut reader",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			runner, err := NewGroupRunner(countingTopology(t, mem, "in"),
				newMockConsumer(), &mockProducer{}, testCase.options...)

			if runner != nil {
				t.Fatal("an unusable runner must not be returned")
			}
			if err == nil || err.Error() != testCase.message {
				t.Fatalf("unexpected error %v", err)
			}
		})
	}
}
