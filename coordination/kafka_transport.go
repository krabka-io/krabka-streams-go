package coordination

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaTransport implements Transport with franz-go.
type KafkaTransport struct {
	brokers []string
	plain   *kgo.Client
	mu      sync.Mutex
	roles   map[Role]kafkaRoleProducer
}

type kafkaRoleProducer struct {
	client *kgo.Client
	token  FencingToken
}

// NewKafkaTransport connects a coordination transport to brokers.
func NewKafkaTransport(brokers ...string) (*KafkaTransport, error) {
	if len(brokers) == 0 {
		return nil, errors.New("a Kafka transport needs at least one broker")
	}
	plain, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka client: %w", err)
	}
	return &KafkaTransport{brokers: append([]string(nil), brokers...), plain: plain, roles: make(map[Role]kafkaRoleProducer)}, nil
}

// AcquireEpoch initializes a transactional producer whose id is the role.
func (t *KafkaTransport) AcquireEpoch(ctx context.Context, role Role) (FencingToken, error) {
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(t.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.TransactionalID(role.String()),
		kgo.TransactionTimeout(DefaultLeaseDuration),
	)
	if err != nil {
		return FencingToken{}, fmt.Errorf("create transactional producer for %s: %w", role, err)
	}
	id, epoch, err := producer.ProducerID(ctx)
	if err != nil {
		producer.Close()
		return FencingToken{}, fmt.Errorf("acquire epoch for %s: %w", role, err)
	}
	token, err := NewFencingToken(id, epoch)
	if err != nil {
		producer.Close()
		return FencingToken{}, err
	}
	t.mu.Lock()
	previous, found := t.roles[role]
	t.roles[role] = kafkaRoleProducer{client: producer, token: token}
	t.mu.Unlock()
	if found {
		previous.client.Close()
	}
	return token, nil
}

// DescribeEpoch reads the producer id and epoch held by role.
func (t *KafkaTransport) DescribeEpoch(ctx context.Context, role Role) (FencingToken, bool, error) {
	req := kmsg.NewPtrDescribeTransactionsRequest()
	req.TransactionalIDs = []string{role.String()}
	response, err := t.plain.Request(ctx, req)
	if err != nil {
		return FencingToken{}, false, fmt.Errorf("describe epoch for %s: %w", role, err)
	}
	described := response.(*kmsg.DescribeTransactionsResponse)
	if len(described.TransactionStates) != 1 {
		return FencingToken{}, false, fmt.Errorf("describe epoch for %s returned %d states", role, len(described.TransactionStates))
	}
	state := described.TransactionStates[0]
	if err := kerr.ErrorForCode(state.ErrorCode); err != nil {
		if errors.Is(err, kerr.TransactionalIDNotFound) {
			return NoEpoch, false, nil
		}
		return FencingToken{}, false, fmt.Errorf("describe epoch for %s: %w", role, err)
	}
	if state.ProducerID < 0 || state.ProducerEpoch < 0 {
		return NoEpoch, false, nil
	}
	token, err := NewFencingToken(state.ProducerID, state.ProducerEpoch)
	return token, err == nil, err
}

// ReadPartition returns committed records from the beginning through the current idle edge.
func (t *KafkaTransport) ReadPartition(ctx context.Context, partition TopicPartition) ([]StateRecord, error) {
	reader, err := kgo.NewClient(
		kgo.SeedBrokers(t.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			partition.Topic: {int32(partition.Partition): kgo.NewOffset().AtStart()},
		}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("create state reader: %w", err)
	}
	defer reader.Close()
	var records []StateRecord
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		fetches := reader.PollFetches(readCtx)
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("read %s-%d: %w", partition.Topic, partition.Partition, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, StateRecord{Offset: record.Offset, Key: append([]byte(nil), record.Key...), Value: append([]byte(nil), record.Value...)})
		})
		if readCtx.Err() != nil {
			return records, nil
		}
	}
}

// Append writes a registration outside a transaction.
func (t *KafkaTransport) Append(ctx context.Context, partition TopicPartition, key, value []byte) error {
	return t.produce(ctx, t.plain, partition, key, value)
}

// WriteLease writes and commits one lease record under token.
func (t *KafkaTransport) WriteLease(ctx context.Context, partition TopicPartition, token FencingToken, key, value []byte) error {
	t.mu.Lock()
	roleProducer, found := roleProducerForToken(t.roles, token)
	t.mu.Unlock()
	if !found {
		return fmt.Errorf("Kafka transport did not mint token %s", token)
	}
	if err := roleProducer.client.BeginTransaction(); err != nil {
		return mapFence(err)
	}
	if err := t.produce(ctx, roleProducer.client, partition, key, value); err != nil {
		_ = roleProducer.client.EndTransaction(ctx, kgo.TryAbort)
		return mapFence(err)
	}
	return mapFence(roleProducer.client.EndTransaction(ctx, kgo.TryCommit))
}

func roleProducerForToken(producers map[Role]kafkaRoleProducer, token FencingToken) (kafkaRoleProducer, bool) {
	for _, producer := range producers {
		if producer.token == token {
			return producer, true
		}
	}
	return kafkaRoleProducer{}, false
}

func (t *KafkaTransport) produce(ctx context.Context, client *kgo.Client, partition TopicPartition, key, value []byte) error {
	record := &kgo.Record{Topic: partition.Topic, Partition: int32(partition.Partition), Key: key, Value: value}
	if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce %s-%d: %w", partition.Topic, partition.Partition, err)
	}
	return nil
}

func mapFence(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kerr.ProducerFenced) || errors.Is(err, kerr.InvalidProducerEpoch) {
		return fmt.Errorf("%w: %v", ErrFenced, err)
	}
	return err
}

// Close releases every producer owned by the transport.
func (t *KafkaTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, producer := range t.roles {
		producer.client.Close()
	}
	t.roles = nil
	t.plain.Close()
}
