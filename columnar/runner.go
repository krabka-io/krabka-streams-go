package columnar

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// TopicPartition identifies one partition of one topic.
type TopicPartition struct {
	// Topic is the topic name.
	Topic string

	// Partition is the partition number.
	Partition int
}

// RebalanceListener receives partition assignment changes from a consumer.
// [GroupRunner] implements it.
type RebalanceListener interface {
	// OnPartitionsAssigned is called after partitions are assigned.
	OnPartitionsAssigned(partitions []TopicPartition)

	// OnPartitionsRevoked is called before partitions are revoked
	// cooperatively.
	OnPartitionsRevoked(partitions []TopicPartition)

	// OnPartitionsLost is called when partitions were lost without a clean
	// revocation.
	OnPartitionsLost(partitions []TopicPartition)
}

// Consumer is the minimal Kafka consumer surface the runner needs. Adapters
// over franz-go or confluent-kafka-go implement it.
//
// Poll returns the fetched records grouped by topic partition, in offset
// order within each partition. A record with a nil value is normalized by
// the runner to an empty value.
type Consumer interface {
	// Assign replaces the consumer's assignment with exactly these
	// partitions.
	Assign(partitions []TopicPartition) error

	// Seek positions the next fetch of a partition at an offset.
	Seek(partition TopicPartition, offset int64) error

	// Poll fetches records, waiting up to the timeout.
	Poll(ctx context.Context, timeout time.Duration) (map[TopicPartition][]ConsumedRecord, error)

	// CommitOffsets synchronously commits the given next-record offsets.
	CommitOffsets(ctx context.Context, offsets map[TopicPartition]int64) error

	// Subscribe joins the consumer group for the topics, delivering
	// assignment changes to the listener.
	Subscribe(topics []string, listener RebalanceListener) error

	// GroupMetadata returns the opaque consumer group metadata handed to
	// transactional offset commits.
	GroupMetadata() any
}

// Producer is the minimal Kafka producer surface the runner needs. Send
// dispatches a record asynchronously and calls done exactly once with the
// acknowledgement outcome.
type Producer interface {
	Send(topic string, record ProduceRecord, done func(error))
}

// TransactionalProducer extends [Producer] with the transactional flow used
// by [GroupRunner.RunOnceTransactional]. Configure and initialize
// transactions before creating the runner.
type TransactionalProducer interface {
	Producer

	// BeginTransaction starts a transaction.
	BeginTransaction() error

	// SendOffsets adds consumed offsets to the open transaction.
	SendOffsets(offsets map[TopicPartition]int64, groupMetadata any) error

	// CommitTransaction commits the open transaction.
	CommitTransaction() error

	// AbortTransaction aborts the open transaction.
	AbortTransaction() error
}

// SendAll dispatches every produced record and waits for all
// acknowledgements, returning the first send error.
func SendAll(outputs []ProducedToTopic, producer Producer) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstError error
	for _, output := range outputs {
		wg.Add(1)
		producer.Send(output.Topic, output.Record, func(err error) {
			if err != nil {
				mu.Lock()
				if firstError == nil {
					firstError = err
				}
				mu.Unlock()
			}
			wg.Done()
		})
	}
	wg.Wait()
	return firstError
}

// Subscribe subscribes a consumer to every source topic of a topology
// without a rebalance listener.
func Subscribe(topology *Topology, consumer Consumer) error {
	return consumer.Subscribe(topology.SourceTopics(), nil)
}

// RunPartitionOnce runs one explicit single-partition cycle: assign, seek,
// poll once, run the topology, send, wait for acknowledgements, commit.
//
// It returns offset unchanged when the poll was empty, without producing
// anything, and otherwise the highest consumed offset plus one. Failed
// processor state is rolled back before the error is returned.
func RunPartitionOnce(ctx context.Context, topology *BuiltTopology, consumer Consumer, producer Producer, topic string, partition int, offset int64, pollTimeout time.Duration) (int64, error) {
	topicPartition := TopicPartition{Topic: topic, Partition: partition}
	if err := consumer.Assign([]TopicPartition{topicPartition}); err != nil {
		return offset, err
	}
	if err := consumer.Seek(topicPartition, offset); err != nil {
		return offset, err
	}
	polled, err := consumer.Poll(ctx, pollTimeout)
	if err != nil {
		return offset, err
	}
	records := normalizeRecords(polled[topicPartition])
	if len(records) == 0 {
		return offset, nil
	}
	nextOffset := offset
	for _, record := range records {
		if record.Offset+1 > nextOffset {
			nextOffset = record.Offset + 1
		}
	}
	prior, err := priorStateOf(topology, partition)
	if err != nil {
		return offset, err
	}
	fail := func(err error) (int64, error) {
		restorePrior(topology, partition, prior)
		return offset, err
	}
	produced, err := topology.RunPartitionBatches(partition, map[string][]ConsumedRecord{topic: records})
	if err != nil {
		return fail(err)
	}
	if err := SendAll(produced, producer); err != nil {
		return fail(err)
	}
	if err := consumer.CommitOffsets(ctx, map[TopicPartition]int64{topicPartition: nextOffset}); err != nil {
		return fail(err)
	}
	return nextOffset, nil
}

// RunGroupOnce polls a subscribed consumer once, runs every fetched
// partition, sends the outputs, and commits the consumed offsets. It returns
// the committed next offsets by partition.
func RunGroupOnce(ctx context.Context, topology *BuiltTopology, consumer Consumer, producer Producer, pollTimeout time.Duration) (map[TopicPartition]int64, error) {
	return runGroupOnce(ctx, topology, consumer, producer, pollTimeout, FailPolicy(), NewMetrics())
}

func runGroupOnce(ctx context.Context, topology *BuiltTopology, consumer Consumer, producer Producer, pollTimeout time.Duration, policy ErrorPolicy, metrics *Metrics) (map[TopicPartition]int64, error) {
	poll, err := processPoll(ctx, topology, consumer, pollTimeout, policy, metrics)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (map[TopicPartition]int64, error) {
		poll.rollback(topology)
		return nil, err
	}
	if err := SendAll(poll.outputs, producer); err != nil {
		return fail(err)
	}
	if len(poll.offsets) > 0 {
		if err := consumer.CommitOffsets(ctx, poll.offsets); err != nil {
			return fail(err)
		}
	}
	return poll.offsets, nil
}

func runGroupOnceTransactional(ctx context.Context, topology *BuiltTopology, consumer Consumer, producer TransactionalProducer, pollTimeout time.Duration, policy ErrorPolicy, metrics *Metrics) (map[TopicPartition]int64, error) {
	if err := producer.BeginTransaction(); err != nil {
		return nil, err
	}
	var poll *processedPoll
	fail := func(err error) (map[TopicPartition]int64, error) {
		abortErr := producer.AbortTransaction()
		if poll != nil {
			poll.rollback(topology)
		}
		return nil, errors.Join(err, abortErr)
	}
	poll, err := processPoll(ctx, topology, consumer, pollTimeout, policy, metrics)
	if err != nil {
		poll = nil
		return fail(err)
	}
	if err := SendAll(poll.outputs, producer); err != nil {
		return fail(err)
	}
	if len(poll.offsets) > 0 {
		if err := producer.SendOffsets(poll.offsets, consumer.GroupMetadata()); err != nil {
			return fail(err)
		}
	}
	if err := producer.CommitTransaction(); err != nil {
		return fail(err)
	}
	return poll.offsets, nil
}

type priorState struct {
	existed  bool
	snapshot map[string][]byte
}

type processedPoll struct {
	offsets map[TopicPartition]int64
	outputs []ProducedToTopic
	prior   map[int]priorState
}

func (p *processedPoll) rollback(topology *BuiltTopology) {
	for partition, state := range p.prior {
		restorePrior(topology, partition, state)
	}
}

func processPoll(ctx context.Context, topology *BuiltTopology, consumer Consumer, pollTimeout time.Duration, policy ErrorPolicy, metrics *Metrics) (*processedPoll, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	polled, err := consumer.Poll(ctx, pollTimeout)
	if err != nil {
		return nil, err
	}
	offsets := map[TopicPartition]int64{}
	byPartition := map[int]map[string][]ConsumedRecord{}
	for topicPartition, records := range polled {
		if len(records) == 0 {
			continue
		}
		normalized := normalizeRecords(records)
		partitionInput, ok := byPartition[topicPartition.Partition]
		if !ok {
			partitionInput = map[string][]ConsumedRecord{}
			byPartition[topicPartition.Partition] = partitionInput
		}
		partitionInput[topicPartition.Topic] = normalized
		offsets[topicPartition] = normalized[len(normalized)-1].Offset + 1
	}
	partitions := make([]int, 0, len(byPartition))
	for partition := range byPartition {
		partitions = append(partitions, partition)
	}
	sort.Ints(partitions)

	var outputs []ProducedToTopic
	prior := map[int]priorState{}
	fail := func(err error) (*processedPoll, error) {
		for partition, state := range prior {
			restorePrior(topology, partition, state)
		}
		return nil, err
	}
	for _, partition := range partitions {
		input := byPartition[partition]
		before, err := priorStateOf(topology, partition)
		if err != nil {
			return fail(err)
		}
		prior[partition] = before
		inputCount := 0
		for _, records := range input {
			inputCount += len(records)
		}
		started := time.Now()
		partitionOutput, err := topology.RunPartitionBatches(partition, input)
		if err != nil {
			restorePrior(topology, partition, before)
			delete(prior, partition)
			if policy.Action == ActionFail || retriable(err) {
				return fail(err)
			}
			deadLetters := 0
			if policy.Action == ActionDeadLetter {
				for _, topic := range sortedTopics(input) {
					for _, record := range input[topic] {
						outputs = append(outputs, deadLetter(policy.DeadLetterTopic, topic, record, err))
						deadLetters++
					}
				}
			}
			metrics.recordFailure(inputCount, deadLetters, time.Since(started).Nanoseconds())
			continue
		}
		outputs = append(outputs, partitionOutput...)
		metrics.recordBatch(inputCount, len(partitionOutput), time.Since(started).Nanoseconds())
	}
	return &processedPoll{offsets: offsets, outputs: outputs, prior: prior}, nil
}

// retriable reports whether the error chain carries a retriable error, such
// as the schema cache's pending-fetch error.
func retriable(err error) bool {
	var marker interface{ Retriable() bool }
	return errors.As(err, &marker) && marker.Retriable()
}

func deadLetter(deadLetterTopic, sourceTopic string, record ConsumedRecord, cause error) ProducedToTopic {
	headers := append([]RecordHeader{}, record.Headers...)
	headers = append(headers,
		RecordHeader{Key: "krabka.error.class", Value: []byte(fmt.Sprintf("%T", cause))},
		RecordHeader{Key: "krabka.error.message", Value: []byte(cause.Error())},
		RecordHeader{Key: "krabka.source.topic", Value: []byte(sourceTopic)},
		RecordHeader{Key: "krabka.source.partition", Value: []byte(strconv.Itoa(record.Partition))},
		RecordHeader{Key: "krabka.source.offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
	)
	return ProducedToTopic{
		Topic:  deadLetterTopic,
		Record: NewProduceRecord(record.Key, record.Value, record.Timestamp, headers...),
	}
}

func priorStateOf(topology *BuiltTopology, partition int) (priorState, error) {
	existed := topology.HasPartition(partition)
	snapshot, err := topology.SnapshotPartition(partition)
	if err != nil {
		return priorState{}, err
	}
	return priorState{existed: existed, snapshot: snapshot}, nil
}

func restorePrior(topology *BuiltTopology, partition int, prior priorState) {
	topology.ReleasePartition(partition)
	if prior.existed {
		_ = topology.RestorePartition(partition, prior.snapshot)
	}
}

func normalizeRecords(records []ConsumedRecord) []ConsumedRecord {
	normalized := make([]ConsumedRecord, len(records))
	for i, record := range records {
		normalized[i] = record
		if normalized[i].Value == nil {
			normalized[i].Value = []byte{}
		}
	}
	return normalized
}

// GroupRunner is a reusable consumer-group runner. It subscribes to every
// source topic, retains one built topology across polls, and implements
// [RebalanceListener] so partition state follows assignments: newly assigned
// logical partitions restore from the state store, revoked partitions save
// before release, and lost partitions are released without saving.
type GroupRunner struct {
	mu       sync.Mutex
	topology *BuiltTopology
	consumer Consumer
	producer Producer
	policy   ErrorPolicy
	store    StateStore
	metrics  *Metrics
	owned    map[TopicPartition]bool
}

// GroupRunnerOption configures a [GroupRunner].
type GroupRunnerOption func(*GroupRunner)

// WithErrorPolicy sets the error policy; the default fails fast.
func WithErrorPolicy(policy ErrorPolicy) GroupRunnerOption {
	return func(r *GroupRunner) { r.policy = policy }
}

// WithStateStore sets the partition state store; the default is ephemeral.
func WithStateStore(store StateStore) GroupRunnerOption {
	return func(r *GroupRunner) { r.store = store }
}

// WithMetrics sets the metrics recorder.
func WithMetrics(metrics *Metrics) GroupRunnerOption {
	return func(r *GroupRunner) { r.metrics = metrics }
}

// NewGroupRunner builds the topology, subscribes the consumer to its source
// topics with the runner as the rebalance listener, and returns the runner.
func NewGroupRunner(topology *Topology, consumer Consumer, producer Producer, options ...GroupRunnerOption) (*GroupRunner, error) {
	built, err := topology.Build()
	if err != nil {
		return nil, err
	}
	runner := &GroupRunner{
		topology: built,
		consumer: consumer,
		producer: producer,
		policy:   FailPolicy(),
		store:    NoStateStore(),
		metrics:  NewMetrics(),
		owned:    map[TopicPartition]bool{},
	}
	for _, option := range options {
		option(runner)
	}
	if err := runner.policy.validate(); err != nil {
		return nil, err
	}
	if err := consumer.Subscribe(topology.SourceTopics(), runner); err != nil {
		return nil, err
	}
	return runner, nil
}

// RunOnce polls once, processes, sends, and commits.
func (r *GroupRunner) RunOnce(ctx context.Context, pollTimeout time.Duration) (map[TopicPartition]int64, error) {
	return runGroupOnce(ctx, r.topology, r.consumer, r.producer, pollTimeout, r.policy, r.metrics)
}

// RunOnceTransactional sends produced records and consumed offsets in one
// producer transaction. The producer must implement
// [TransactionalProducer].
func (r *GroupRunner) RunOnceTransactional(ctx context.Context, pollTimeout time.Duration) (map[TopicPartition]int64, error) {
	transactional, ok := r.producer.(TransactionalProducer)
	if !ok {
		return nil, fmt.Errorf("producer does not support transactions")
	}
	return runGroupOnceTransactional(ctx, r.topology, r.consumer, transactional, pollTimeout, r.policy, r.metrics)
}

// Metrics returns the runner's metrics recorder.
func (r *GroupRunner) Metrics() *Metrics { return r.metrics }

// Topology returns the built topology the runner drives.
func (r *GroupRunner) Topology() *BuiltTopology { return r.topology }

// OnPartitionsAssigned implements [RebalanceListener].
func (r *GroupRunner) OnPartitionsAssigned(partitions []TopicPartition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	priorLogical := map[int]bool{}
	for owned := range r.owned {
		priorLogical[owned.Partition] = true
	}
	restored := map[int]bool{}
	for _, partition := range partitions {
		r.owned[partition] = true
		if !priorLogical[partition.Partition] && !restored[partition.Partition] {
			restored[partition.Partition] = true
			if snapshot, err := r.store.Load(partition.Partition); err == nil {
				_ = r.topology.RestorePartition(partition.Partition, snapshot)
			}
		}
	}
}

// OnPartitionsRevoked implements [RebalanceListener]; state is saved before
// release.
func (r *GroupRunner) OnPartitionsRevoked(partitions []TopicPartition) {
	r.release(partitions, true)
}

// OnPartitionsLost implements [RebalanceListener]; state is released without
// saving.
func (r *GroupRunner) OnPartitionsLost(partitions []TopicPartition) {
	r.release(partitions, false)
}

// Close saves and releases every owned partition, then closes the topology.
func (r *GroupRunner) Close() error {
	r.mu.Lock()
	owned := make([]TopicPartition, 0, len(r.owned))
	for partition := range r.owned {
		owned = append(owned, partition)
	}
	r.mu.Unlock()
	r.release(owned, true)
	return r.topology.Close()
}

func (r *GroupRunner) release(partitions []TopicPartition, save bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, partition := range partitions {
		delete(r.owned, partition)
	}
	released := map[int]bool{}
	for _, partition := range partitions {
		logical := partition.Partition
		if released[logical] {
			continue
		}
		released[logical] = true
		stillOwned := false
		for owned := range r.owned {
			if owned.Partition == logical {
				stillOwned = true
				break
			}
		}
		if stillOwned {
			continue
		}
		if save {
			if snapshot, err := r.topology.SnapshotPartition(logical); err == nil {
				_ = r.store.Save(logical, snapshot)
			}
		}
		r.topology.ReleasePartition(logical)
	}
}
