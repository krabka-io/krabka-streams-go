package columnar

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
)

// Barrier is the barrier a group runner reached: the cut it aligned on, the
// partitions whose state it snapshotted, and the offsets it committed.
type Barrier struct {
	// Cut is the cut the runner aligned on.
	Cut BarrierCut

	// Partitions are the logical partition numbers whose state the runner
	// snapshotted, in ascending order.
	Partitions []int

	// Offsets are the committed offsets of the aligned topic partitions.
	// Each one is the marker offset of the cut.
	Offsets map[TopicPartition]int64
}

// BarrierListener runs after a group runner commits at a barrier. The
// snapshot of every partition is already in the state store, and the
// committed position of every aligned partition is the cut. The runner calls
// the listener on its own goroutine, so a slow listener slows the poll loop.
type BarrierListener func(Barrier)

// WithBarrierGroup aligns the runner on the cuts of a barrier group. The
// reader must drive a consumer of its own, not the one the runner
// subscribes.
//
// The runner then holds the records after each cut, snapshots the state of
// every partition at the cut, and commits the cut offsets.
func WithBarrierGroup(group string, reader *CutReader) GroupRunnerOption {
	return func(r *GroupRunner) {
		r.barrierGroup = group
		r.barrierReader = reader
	}
}

// WithBarrierListener sets the callback the runner calls after each barrier
// commit. It requires [WithBarrierGroup].
func WithBarrierListener(listener BarrierListener) GroupRunnerOption {
	return func(r *GroupRunner) { r.barrierListener = listener }
}

// RestoreToEpoch rewinds the runner to the cut of one epoch: it restores the
// state of every partition from that epoch's snapshot and seeks every input
// partition to its marker offset. It returns the cut it restored to.
//
// The runner acts on the partitions it owns, or on every partition of the
// cut when it has no assignment yet.
func (r *GroupRunner) RestoreToEpoch(ctx context.Context, epoch int64) (*BarrierCut, error) {
	if r.barrier == nil {
		return nil, fmt.Errorf("restoring to a cut requires a barrier group")
	}
	cut, err := r.barrier.reader.completeCutAt(ctx, r.barrier.group, epoch)
	if err != nil {
		return nil, err
	}
	if cut == nil {
		return nil, fmt.Errorf("barrier group %s has no complete cut at epoch %d", r.barrier.group, epoch)
	}
	if err := r.restoreToCut(*cut); err != nil {
		return nil, err
	}
	return cut, nil
}

// RestoreToLatestCut rewinds the runner to the complete cut of the barrier
// group with the highest epoch, as [GroupRunner.RestoreToEpoch] does for a
// named epoch.
func (r *GroupRunner) RestoreToLatestCut(ctx context.Context) (*BarrierCut, error) {
	if r.barrier == nil {
		return nil, fmt.Errorf("restoring to a cut requires a barrier group")
	}
	cut, err := r.barrier.reader.LatestCompleteCut(ctx, r.barrier.group)
	if err != nil {
		return nil, err
	}
	if cut == nil {
		return nil, fmt.Errorf("barrier group %s has no complete cut", r.barrier.group)
	}
	if err := r.restoreToCut(*cut); err != nil {
		return nil, err
	}
	return cut, nil
}

func (r *GroupRunner) restoreToCut(cut BarrierCut) error {
	targets := r.barrierTargets(cut)
	logical := map[int]bool{}
	for _, partition := range targets {
		logical[partition.Partition] = true
	}
	for _, partition := range slices.Sorted(maps.Keys(logical)) {
		snapshot, err := r.store.Load(partition, cut.Epoch)
		if err != nil {
			return err
		}
		r.topology.ReleasePartition(partition)
		if err := r.topology.RestorePartition(partition, snapshot); err != nil {
			return err
		}
	}
	for _, partition := range targets {
		if err := r.consumer.Seek(partition, cut.Offsets[partition]); err != nil {
			return err
		}
	}
	r.barrier.restored(cut, targets)
	return nil
}

// barrierTargets are the partitions of a cut the runner acts on: the ones it
// owns, or every partition of the cut when it owns none yet.
func (r *GroupRunner) barrierTargets(cut BarrierCut) []TopicPartition {
	assigned := r.assignment()
	var targets []TopicPartition
	for partition := range cut.Offsets {
		if len(assigned) == 0 || assigned[partition] {
			targets = append(targets, partition)
		}
	}
	slices.SortFunc(targets, compareTopicPartitions)
	return targets
}

func (r *GroupRunner) assignment() map[TopicPartition]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.owned)
}

// startBarrier builds the aligner from the barrier options, and rejects an
// incomplete set of them.
func (r *GroupRunner) startBarrier() error {
	switch {
	case (r.barrierReader == nil) != (r.barrierGroup == ""):
		return fmt.Errorf("a barrier group needs both a name and a cut reader")
	case r.barrierReader == nil:
		if r.barrierListener != nil {
			return fmt.Errorf("a barrier listener requires a barrier group")
		}
		return nil
	default:
		r.barrier = newBarrierAligner(r.barrierGroup, r.barrierReader, r.barrierListener)
		return nil
	}
}

// barrierAligner holds the barrier state of one group runner. It tracks the
// pending cut, the records it holds past that cut, and the position of every
// partition it waits for.
type barrierAligner struct {
	group     string
	reader    *CutReader
	listener  BarrierListener
	lastEpoch int64
	pending   *BarrierCut
	aligned   map[TopicPartition]int64
	reached   map[TopicPartition]bool
	held      map[TopicPartition][]ConsumedRecord
	positions map[TopicPartition]int64
	assigned  map[TopicPartition]bool
}

func newBarrierAligner(group string, reader *CutReader, listener BarrierListener) *barrierAligner {
	return &barrierAligner{
		group:     group,
		reader:    reader,
		listener:  listener,
		lastEpoch: -1,
		aligned:   map[TopicPartition]int64{},
		reached:   map[TopicPartition]bool{},
		held:      map[TopicPartition][]ConsumedRecord{},
		positions: map[TopicPartition]int64{},
	}
}

// refresh takes the next complete cut of the group when none is pending.
func (a *barrierAligner) refresh(ctx context.Context, assigned map[TopicPartition]bool) error {
	a.assigned = assigned
	if a.pending != nil {
		a.realign()
		return nil
	}
	cuts, err := a.reader.CompleteCutsAfter(ctx, a.group, a.lastEpoch)
	if err != nil {
		return err
	}
	if len(cuts) == 0 {
		return nil
	}
	a.begin(cuts[0])
	return nil
}

func (a *barrierAligner) begin(cut BarrierCut) {
	pending := cut.clone()
	a.pending = &pending
	a.reached = map[TopicPartition]bool{}
	a.realign()
}

// realign takes the partitions of the pending cut the runner owns now. A
// rebalance that took a partition away drops it, so the cut does not wait
// for a partition the runner lost. A partition whose position is already at
// or past the cut needs no record to reach it.
func (a *barrierAligner) realign() {
	aligned := map[TopicPartition]int64{}
	for partition, offset := range a.pending.Offsets {
		if !a.owns(partition) {
			continue
		}
		aligned[partition] = offset
		if a.positions[partition] >= offset {
			a.reached[partition] = true
		}
	}
	for partition := range a.reached {
		if _, ok := aligned[partition]; !ok {
			delete(a.reached, partition)
		}
	}
	for partition := range a.held {
		if _, ok := aligned[partition]; !ok && !a.owns(partition) {
			delete(a.held, partition)
		}
	}
	a.aligned = aligned
}

// owns reports whether the runner waits for a partition. A runner with no
// assignment yet waits for every partition of the cut.
func (a *barrierAligner) owns(partition TopicPartition) bool {
	if len(a.assigned) == 0 {
		return true
	}
	return a.assigned[partition]
}

// merge puts the records held at the previous barrier round in front of the
// fetched ones.
func (a *barrierAligner) merge(polled map[TopicPartition][]ConsumedRecord) map[TopicPartition][]ConsumedRecord {
	if len(a.held) == 0 {
		return polled
	}
	merged := make(map[TopicPartition][]ConsumedRecord, len(polled)+len(a.held))
	maps.Copy(merged, a.held)
	for partition, records := range polled {
		merged[partition] = append(merged[partition], records...)
	}
	a.held = map[TopicPartition][]ConsumedRecord{}
	return merged
}

// accept truncates one partition's records at the cut offset. The marker
// takes the cut offset itself and reaches no consumer, so the records before
// the cut are the records with a lower offset. The rest wait for the next
// round.
func (a *barrierAligner) accept(partition TopicPartition, records []ConsumedRecord) []ConsumedRecord {
	if len(records) > 0 {
		a.positions[partition] = max(a.positions[partition], records[len(records)-1].Offset+1)
	}
	cutOffset, aligned := a.aligned[partition]
	if !aligned {
		return records
	}
	split := len(records)
	for index, record := range records {
		if record.Offset >= cutOffset {
			split = index
			break
		}
	}
	if split < len(records) {
		a.held[partition] = append(a.held[partition], slices.Clone(records[split:])...)
	}
	if a.positions[partition] >= cutOffset {
		a.reached[partition] = true
	}
	return records[:split]
}

// fired reports whether every partition the runner waits for reached the cut.
func (a *barrierAligner) fired() bool {
	if a.pending == nil {
		return false
	}
	for partition := range a.aligned {
		if !a.reached[partition] {
			return false
		}
	}
	return true
}

// barrier raises the offsets of the aligned partitions to the cut, so the
// committed position becomes the cut, and describes the barrier.
func (a *barrierAligner) barrier(offsets map[TopicPartition]int64) *Barrier {
	committed := map[TopicPartition]int64{}
	logical := map[int]bool{}
	for partition, cutOffset := range a.aligned {
		if current, ok := offsets[partition]; !ok || current < cutOffset {
			offsets[partition] = cutOffset
		}
		committed[partition] = offsets[partition]
		logical[partition.Partition] = true
	}
	return &Barrier{
		Cut:        a.pending.clone(),
		Partitions: slices.Sorted(maps.Keys(logical)),
		Offsets:    committed,
	}
}

// save snapshots the state of every partition of the barrier and stores it
// under the epoch of the cut.
func (a *barrierAligner) save(topology *BuiltTopology, store StateStore, barrier *Barrier) error {
	for _, partition := range barrier.Partitions {
		snapshot, err := topology.SnapshotPartition(partition)
		if err != nil {
			return err
		}
		if err := store.Save(partition, barrier.Cut.Epoch, snapshot); err != nil {
			return err
		}
	}
	return nil
}

// finish closes a barrier the runner committed and calls the listener.
func (a *barrierAligner) finish(barrier *Barrier) {
	a.lastEpoch = barrier.Cut.Epoch
	a.pending = nil
	a.aligned = map[TopicPartition]int64{}
	a.reached = map[TopicPartition]bool{}
	for partition, offset := range barrier.Offsets {
		a.positions[partition] = max(a.positions[partition], offset)
	}
	if a.listener != nil {
		a.listener(*barrier)
	}
}

// restored puts the aligner at a cut the runner rewound to. Held records
// belong to a position the runner left, so they go away.
func (a *barrierAligner) restored(cut BarrierCut, targets []TopicPartition) {
	a.lastEpoch = cut.Epoch
	a.pending = nil
	a.aligned = map[TopicPartition]int64{}
	a.reached = map[TopicPartition]bool{}
	a.held = map[TopicPartition][]ConsumedRecord{}
	for _, partition := range targets {
		a.positions[partition] = cut.Offsets[partition]
	}
}

func compareTopicPartitions(left, right TopicPartition) int {
	if result := cmp.Compare(left.Topic, right.Topic); result != 0 {
		return result
	}
	return cmp.Compare(left.Partition, right.Partition)
}
