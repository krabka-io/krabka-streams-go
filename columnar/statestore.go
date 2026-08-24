package columnar

import (
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
)

// NoEpoch keys the snapshot a runner saves outside a barrier, such as the
// one it writes when a rebalance revokes a partition. A barrier keys its
// snapshot by the epoch of the cut instead.
const NoEpoch int64 = -1

// StateStore persists partition snapshots across rebalances and restarts.
// The key of a snapshot is the partition and the epoch, so the snapshots a
// barrier group takes at its cuts stay apart from each other and from the
// [NoEpoch] snapshot of a rebalance.
type StateStore interface {
	// Load returns the saved snapshot of a partition at an epoch, empty when
	// none exists.
	Load(partition int, epoch int64) (map[string][]byte, error)

	// Save persists the snapshot of a partition at an epoch.
	Save(partition int, epoch int64, snapshot map[string][]byte) error
}

// NoStateStore returns a store that loads nothing and saves nothing, for
// intentionally ephemeral state.
func NoStateStore() StateStore { return noStateStore{} }

type noStateStore struct{}

func (noStateStore) Load(int, int64) (map[string][]byte, error) {
	return map[string][]byte{}, nil
}

func (noStateStore) Save(int, int64, map[string][]byte) error { return nil }

const fileStateStoreVersion = 1

// FileStateStore atomically saves partition snapshots to files in a
// directory, one file per partition and epoch.
type FileStateStore struct {
	directory string
}

// NewFileStateStore creates a store rooted at directory, which is created on
// the first save.
func NewFileStateStore(directory string) *FileStateStore {
	return &FileStateStore{directory: directory}
}

// Load implements [StateStore].
func (s *FileStateStore) Load(partition int, epoch int64) (map[string][]byte, error) {
	data, err := os.ReadFile(s.file(partition, epoch))
	if os.IsNotExist(err) {
		return map[string][]byte{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot load partition %d state: %w", partition, err)
	}
	reader := &valueReader{data: data}
	version, err := reader.uint32()
	if err != nil || version != fileStateStoreVersion {
		return nil, fmt.Errorf("unsupported state snapshot version")
	}
	count, err := reader.uint32()
	if err != nil {
		return nil, fmt.Errorf("cannot load partition %d state: %w", partition, err)
	}
	result := make(map[string][]byte, count)
	for range count {
		name, err := reader.sized()
		if err != nil {
			return nil, fmt.Errorf("cannot load partition %d state: %w", partition, err)
		}
		value, err := reader.sized()
		if err != nil {
			return nil, fmt.Errorf("cannot load partition %d state: %w", partition, err)
		}
		result[string(name)] = value
	}
	if !reader.empty() {
		return nil, fmt.Errorf("trailing bytes in state snapshot")
	}
	return result, nil
}

// Save implements [StateStore]. The snapshot is written to a temporary file
// and atomically renamed over the target.
func (s *FileStateStore) Save(partition int, epoch int64, snapshot map[string][]byte) error {
	if err := os.MkdirAll(s.directory, 0o755); err != nil {
		return fmt.Errorf("cannot save partition %d state: %w", partition, err)
	}
	var buffer []byte
	buffer = binary.BigEndian.AppendUint32(buffer, fileStateStoreVersion)
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(snapshot)))
	for _, name := range slices.Sorted(maps.Keys(snapshot)) {
		buffer = appendSized(buffer, []byte(name))
		buffer = appendSized(buffer, snapshot[name])
	}
	temporary, err := os.CreateTemp(s.directory, fmt.Sprintf("partition-%d-*.tmp", partition))
	if err != nil {
		return fmt.Errorf("cannot save partition %d state: %w", partition, err)
	}
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(buffer); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("cannot save partition %d state: %w", partition, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("cannot save partition %d state: %w", partition, err)
	}
	if err := os.Rename(temporary.Name(), s.file(partition, epoch)); err != nil {
		return fmt.Errorf("cannot save partition %d state: %w", partition, err)
	}
	return nil
}

// file names the snapshot of one partition and epoch. The container inside
// the file is the same in both cases: only the key carries the epoch.
func (s *FileStateStore) file(partition int, epoch int64) string {
	if epoch == NoEpoch {
		return filepath.Join(s.directory, fmt.Sprintf("partition-%d.snapshot", partition))
	}
	return filepath.Join(s.directory, fmt.Sprintf("partition-%d-epoch-%d.snapshot", partition, epoch))
}

// ErrorPolicyAction selects what a group runner does with a failed batch.
type ErrorPolicyAction int

const (
	// ActionFail stops processing and surfaces the error.
	ActionFail ErrorPolicyAction = iota

	// ActionSkip drops the failed batch and continues.
	ActionSkip

	// ActionDeadLetter forwards the failed batch's records to a dead-letter
	// topic and continues.
	ActionDeadLetter
)

// ErrorPolicy decides what a group runner does when a partition's batch
// fails. Failed processor state is rolled back before the policy is applied,
// and retriable errors always fail the poll so the records are retried.
type ErrorPolicy struct {
	// Action is the policy action.
	Action ErrorPolicyAction

	// DeadLetterTopic is the dead-letter topic; required for
	// [ActionDeadLetter] and empty otherwise.
	DeadLetterTopic string
}

// FailPolicy stops on the first failed batch.
func FailPolicy() ErrorPolicy { return ErrorPolicy{Action: ActionFail} }

// SkipPolicy drops failed batches.
func SkipPolicy() ErrorPolicy { return ErrorPolicy{Action: ActionSkip} }

// DeadLetterPolicy forwards failed batches' records to a dead-letter topic
// with error-describing headers.
func DeadLetterPolicy(topic string) ErrorPolicy {
	return ErrorPolicy{Action: ActionDeadLetter, DeadLetterTopic: topic}
}

func (p ErrorPolicy) validate() error {
	if p.Action == ActionDeadLetter && p.DeadLetterTopic == "" {
		return fmt.Errorf("deadLetterTopic requires ActionDeadLetter")
	}
	if p.Action != ActionDeadLetter && p.DeadLetterTopic != "" {
		return fmt.Errorf("deadLetterTopic requires ActionDeadLetter")
	}
	return nil
}

// Metrics counts the batches, records, failures, and processing time of a
// runner. All methods are safe for concurrent use.
type Metrics struct {
	batches           atomic.Int64
	inputRecords      atomic.Int64
	outputRecords     atomic.Int64
	failures          atomic.Int64
	deadLetterRecords atomic.Int64
	processingNanos   atomic.Int64
}

// NewMetrics creates a zeroed metrics recorder.
func NewMetrics() *Metrics { return &Metrics{} }

func (m *Metrics) recordBatch(input, output int, nanos int64) {
	m.batches.Add(1)
	m.inputRecords.Add(int64(input))
	m.outputRecords.Add(int64(output))
	m.processingNanos.Add(nanos)
}

func (m *Metrics) recordFailure(input, deadLetters int, nanos int64) {
	m.batches.Add(1)
	m.inputRecords.Add(int64(input))
	m.failures.Add(1)
	m.deadLetterRecords.Add(int64(deadLetters))
	m.processingNanos.Add(nanos)
}

// MetricsSnapshot is a point-in-time reading of a [Metrics].
type MetricsSnapshot struct {
	// Batches is the number of partition batches processed, including failed
	// ones.
	Batches int64

	// InputRecords is the number of consumed records seen.
	InputRecords int64

	// OutputRecords is the number of records produced by successful batches.
	OutputRecords int64

	// Failures is the number of failed partition batches.
	Failures int64

	// DeadLetterRecords is the number of records forwarded to a dead-letter
	// topic.
	DeadLetterRecords int64

	// ProcessingNanos is the total processing time in nanoseconds.
	ProcessingNanos int64
}

// Snapshot reads the current totals.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Batches:           m.batches.Load(),
		InputRecords:      m.inputRecords.Load(),
		OutputRecords:     m.outputRecords.Load(),
		Failures:          m.failures.Load(),
		DeadLetterRecords: m.deadLetterRecords.Load(),
		ProcessingNanos:   m.processingNanos.Load(),
	}
}
