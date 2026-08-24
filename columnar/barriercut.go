package columnar

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

const (
	defaultCutPollTimeout = 250 * time.Millisecond
	defaultCutIdlePolls   = 1
)

// CutReader reads the cuts of [BarrierStateTopic] with a manual assign,
// seek, and poll loop. It joins no consumer group.
//
// Give it a consumer of its own. Assign replaces the assignment of the
// consumer it drives, which revokes the partitions of a subscribed group
// runner.
//
// The reader keeps every cut it read and resumes where the previous call
// stopped, so only the first call reads the whole topic. It is safe for
// concurrent use.
type CutReader struct {
	mu          sync.Mutex
	consumer    Consumer
	topic       string
	partitions  []TopicPartition
	pollTimeout time.Duration
	idlePolls   int
	positioned  bool
	cuts        map[string]map[int64]BarrierCut
}

// CutReaderOption configures a [CutReader].
type CutReaderOption func(*CutReader)

// WithCutPollTimeout sets the timeout of one poll; the default is 250
// milliseconds.
func WithCutPollTimeout(timeout time.Duration) CutReaderOption {
	return func(r *CutReader) { r.pollTimeout = timeout }
}

// WithCutIdlePolls sets how many empty polls in a row end a read; the
// default is one.
func WithCutIdlePolls(polls int) CutReaderOption {
	return func(r *CutReader) { r.idlePolls = polls }
}

// WithCutTopic reads the cuts from another topic than [BarrierStateTopic].
func WithCutTopic(topic string) CutReaderOption {
	return func(r *CutReader) { r.topic = topic }
}

// NewCutReader creates a reader over the partitions of the barrier state
// topic. partitions is the partition count of that topic.
func NewCutReader(consumer Consumer, partitions int, options ...CutReaderOption) (*CutReader, error) {
	if consumer == nil {
		return nil, fmt.Errorf("a cut reader needs a consumer")
	}
	if partitions < 1 {
		return nil, fmt.Errorf("a cut reader needs at least one partition")
	}
	reader := &CutReader{
		consumer:    consumer,
		topic:       BarrierStateTopic,
		pollTimeout: defaultCutPollTimeout,
		idlePolls:   defaultCutIdlePolls,
		cuts:        map[string]map[int64]BarrierCut{},
	}
	for _, option := range options {
		option(reader)
	}
	if reader.idlePolls < 1 {
		return nil, fmt.Errorf("a cut reader needs at least one idle poll")
	}
	if reader.topic == "" {
		return nil, fmt.Errorf("a cut reader needs a topic")
	}
	reader.partitions = make([]TopicPartition, partitions)
	for partition := range partitions {
		reader.partitions[partition] = TopicPartition{Topic: reader.topic, Partition: partition}
	}
	return reader, nil
}

// LatestCompleteCut returns the complete cut of a barrier group with the
// highest epoch. It returns a nil cut when the topic holds no complete cut
// of that group.
func (r *CutReader) LatestCompleteCut(ctx context.Context, group string) (*BarrierCut, error) {
	cuts, err := r.CompleteCutsAfter(ctx, group, -1)
	if err != nil {
		return nil, err
	}
	if len(cuts) == 0 {
		return nil, nil
	}
	latest := cuts[len(cuts)-1]
	return &latest, nil
}

// CompleteCutsAfter returns the complete cuts of a barrier group with an
// epoch above epoch, oldest first. Partial cuts are left out: a partial cut
// has partitions that never receive the marker, so a task that waits for one
// waits forever.
func (r *CutReader) CompleteCutsAfter(ctx context.Context, group string, epoch int64) ([]BarrierCut, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	var result []BarrierCut
	for _, cut := range r.cuts[group] {
		if cut.Epoch > epoch && cut.Complete() {
			result = append(result, cut.clone())
		}
	}
	slices.SortFunc(result, func(left, right BarrierCut) int {
		return cmp.Compare(left.Epoch, right.Epoch)
	})
	return result, nil
}

// completeCutAt returns the complete cut of one epoch, or a nil cut when the
// topic holds no such cut.
func (r *CutReader) completeCutAt(ctx context.Context, group string, epoch int64) (*BarrierCut, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}
	cut, ok := r.cuts[group][epoch]
	if !ok || !cut.Complete() {
		return nil, nil
	}
	found := cut.clone()
	return &found, nil
}

// refresh polls every partition of the barrier state topic up to the end.
// The first call seeks to the start of each partition; a later call resumes
// at the position the previous one left.
func (r *CutReader) refresh(ctx context.Context) error {
	if !r.positioned {
		if err := r.consumer.Assign(r.partitions); err != nil {
			return err
		}
		for _, partition := range r.partitions {
			if err := r.consumer.Seek(partition, 0); err != nil {
				return err
			}
		}
		r.positioned = true
	}
	for idle := 0; idle < r.idlePolls; {
		polled, err := r.consumer.Poll(ctx, r.pollTimeout)
		if err != nil {
			return err
		}
		fetched := 0
		for _, records := range polled {
			fetched += len(records)
			for _, record := range records {
				if err := r.accept(record); err != nil {
					return err
				}
			}
		}
		if fetched == 0 {
			idle++
			continue
		}
		idle = 0
	}
	return nil
}

// accept applies one record of the barrier state topic. A record of another
// kind is skipped, and a tombstone drops the cut of its epoch.
func (r *CutReader) accept(record ConsumedRecord) error {
	key, err := decodeBarrierKey(record.Key)
	if err != nil {
		return err
	}
	if key.kind != barrierKindCut {
		return nil
	}
	if len(record.Value) == 0 {
		delete(r.cuts[key.group], key.epoch)
		return nil
	}
	cut, err := decodeBarrierCutValue(key, record.Value)
	if err != nil {
		return err
	}
	group, ok := r.cuts[key.group]
	if !ok {
		group = map[int64]BarrierCut{}
		r.cuts[key.group] = group
	}
	group[key.epoch] = cut
	return nil
}
