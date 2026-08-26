package coordination

import (
	"context"
	"errors"
)

// TopicPartition identifies one partition of one topic. This package declares
// its own copy, because it depends on no other package of this library.
type TopicPartition struct {
	// Topic is the topic name.
	Topic string

	// Partition is the partition number.
	Partition int
}

// ErrFenced reports that the broker refused a write, because another member
// holds the role now.
//
// This is the expected end of a leadership. A [LeaseWriter] wraps this error
// for broker error code 47 INVALID_PRODUCER_EPOCH and for broker error code
// 90 PRODUCER_FENCED. [AcquireLeadership] and [Leadership.Renew] find it with
// [errors.Is].
//
// A deposed leader learns that it lost the role from this error, and from
// nothing else. No clock takes part in the check. The epoch it held is
// superseded, so the cluster already rejects every write it makes. The caller
// stops the work of the role at once.
var ErrFenced = errors.New("fenced: another member holds the role")

// ErrNotHeld reports that a caller asked a [*Leadership] to act after the
// leadership ended.
var ErrNotHeld = errors.New("the member no longer holds the role")

// Coordinator mints and reports the leadership epoch of a role. An adapter
// implements it over the transaction coordinator of a cluster.
//
// The two calls carry different weight. AcquireEpoch takes the role, and
// DescribeEpoch only looks.
type Coordinator interface {
	// AcquireEpoch mints a new epoch for the role and fences the previous
	// holder. The implementation calls InitProducerId with
	// transactional.id = <role>, and it keeps the producer that the epoch
	// binds, because [LeaseWriter.WriteLease] writes through that producer.
	//
	// The transaction coordinator picks the epoch, so the quorum mints the
	// value and the value only grows. The implementation should pass the
	// lease duration as transaction_timeout_ms, so the coordinator aborts the
	// open transaction of a dead holder without a client.
	AcquireEpoch(ctx context.Context, role Role) (FencingToken, error)

	// DescribeEpoch reports the token that the transaction coordinator holds
	// for the role. It reports false for a role that no member has ever
	// taken. The implementation calls DescribeTransactions, which joins no
	// group and takes no epoch.
	DescribeEpoch(ctx context.Context, role Role) (FencingToken, bool, error)
}

// StateReader reads one partition of [StateTopic]. An adapter implements it
// with a plain assign, seek, and poll loop over the partition.
type StateReader interface {
	// ReadPartition returns every committed record of the partition, oldest
	// first. The read takes committed records only, so an aborted lease write
	// stays invisible. The reader walks from the first offset of the
	// partition to the last stable offset.
	ReadPartition(ctx context.Context, partition TopicPartition) ([]StateRecord, error)
}

// Registrar appends the registration record of one candidate.
//
// This call carries no authority. A candidate holds no epoch when it
// announces itself, and it must take no epoch to do so, so the append sits
// outside a transaction. The offset that the broker assigns is the join
// sequence that the succession rules rank on.
type Registrar interface {
	// Append appends one record to the partition. The producer behind it
	// pins the partition, and it applies no partitioner of its own.
	Append(ctx context.Context, partition TopicPartition, key, value []byte) error
}

// LeaseWriter writes the lease record of a role in a transaction under the
// epoch of the role.
//
// A lease record authenticates itself. The broker fences any writer that does
// not hold the current epoch, so a lease record that reached the log is proof
// that its author held the epoch.
type LeaseWriter interface {
	// WriteLease writes one record under the token, inside a transaction of
	// the producer that minted the token. A nil value clears the lease. The
	// producer pins the partition, and it applies no partitioner of its own.
	//
	// The implementation returns an error that wraps [ErrFenced] when the
	// broker rejects the token. That rejection is how a deposed holder learns
	// that it lost the role.
	WriteLease(ctx context.Context, partition TopicPartition, token FencingToken, key, value []byte) error
}

// Transport is every cluster operation that this package performs, and it is
// nothing else. One adapter type satisfies all four interfaces.
//
// The library pins no Kafka client, so a caller writes the adapter over the
// client it already runs. The adapter needs two producers for one role. The
// transactional producer carries transactional.id = <role>, and it writes the
// lease records of that role. A second, plain producer appends the
// registration records, because Kafka puts every send of a transactional
// producer inside a transaction, and a candidate holds no epoch to open one
// with.
type Transport interface {
	Coordinator
	StateReader
	Registrar
	LeaseWriter
}
