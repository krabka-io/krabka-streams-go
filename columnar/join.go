package columnar

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const joinSnapshotVersion = 1

// statefulJoinProcessor keeps each side's batches, serialized as Arrow IPC,
// bounded by the join's event-time window, and emits joined batches for every
// key match within the window.
type statefulJoinProcessor struct {
	join       Join
	mem        memory.Allocator
	serde      *IPCSerde
	left       []timedBatch
	right      []timedBatch
	streamTime int64
}

type timedBatch struct {
	maxTimestamp int64
	data         []byte
}

func newStatefulJoinProcessor(join Join, mem memory.Allocator) *statefulJoinProcessor {
	return &statefulJoinProcessor{
		join:       join,
		mem:        mem,
		serde:      NewIPCSerde(mem),
		streamTime: math.MinInt64,
	}
}

// Process implements [Processor]; joins are driven through processSides.
func (p *statefulJoinProcessor) Process(ctx *Context, batch arrow.Record) error {
	return fmt.Errorf("a join requires two parents")
}

func (p *statefulJoinProcessor) processSides(newLeft, newRight []arrow.Record) ([]arrow.Record, error) {
	for _, batch := range newLeft {
		if err := p.advanceStreamTime(batch); err != nil {
			return nil, err
		}
	}
	for _, batch := range newRight {
		if err := p.advanceStreamTime(batch); err != nil {
			return nil, err
		}
	}
	p.left = p.prune(p.left)
	p.right = p.prune(p.right)
	var outputs []arrow.Record
	fail := func(err error) ([]arrow.Record, error) {
		for _, output := range outputs {
			output.Release()
		}
		return nil, err
	}
	for _, batch := range newLeft {
		if err := p.joinStored(batch, p.right, true, &outputs); err != nil {
			return fail(err)
		}
		for _, other := range newRight {
			if err := p.addMatches(batch, other, &outputs); err != nil {
				return fail(err)
			}
		}
	}
	for _, batch := range newRight {
		if err := p.joinStored(batch, p.left, false, &outputs); err != nil {
			return fail(err)
		}
	}
	for _, batch := range newLeft {
		stored, err := p.stored(batch)
		if err != nil {
			return fail(err)
		}
		p.left = append(p.left, stored)
	}
	for _, batch := range newRight {
		stored, err := p.stored(batch)
		if err != nil {
			return fail(err)
		}
		p.right = append(p.right, stored)
	}
	return outputs, nil
}

// Snapshot implements [StatefulProcessor].
func (p *statefulJoinProcessor) Snapshot() ([]byte, error) {
	var buffer []byte
	buffer = binary.BigEndian.AppendUint32(buffer, joinSnapshotVersion)
	buffer = binary.BigEndian.AppendUint64(buffer, uint64(p.streamTime))
	buffer = appendTimedBatches(buffer, p.left)
	buffer = appendTimedBatches(buffer, p.right)
	return buffer, nil
}

// Restore implements [StatefulProcessor].
func (p *statefulJoinProcessor) Restore(snapshot []byte) error {
	reader := &valueReader{data: snapshot}
	version, err := reader.uint32()
	if err != nil || version != joinSnapshotVersion {
		return fmt.Errorf("unsupported join snapshot version")
	}
	streamTime, err := reader.uint64()
	if err != nil {
		return fmt.Errorf("cannot restore join state: %w", err)
	}
	left, err := readTimedBatches(reader)
	if err != nil {
		return fmt.Errorf("cannot restore join state: %w", err)
	}
	right, err := readTimedBatches(reader)
	if err != nil {
		return fmt.Errorf("cannot restore join state: %w", err)
	}
	if !reader.empty() {
		return fmt.Errorf("trailing bytes in join snapshot")
	}
	p.streamTime = int64(streamTime)
	p.left = left
	p.right = right
	return nil
}

func appendTimedBatches(buffer []byte, batches []timedBatch) []byte {
	buffer = binary.BigEndian.AppendUint32(buffer, uint32(len(batches)))
	for _, batch := range batches {
		buffer = binary.BigEndian.AppendUint64(buffer, uint64(batch.maxTimestamp))
		buffer = appendSized(buffer, batch.data)
	}
	return buffer
}

func readTimedBatches(reader *valueReader) ([]timedBatch, error) {
	count, err := reader.uint32()
	if err != nil {
		return nil, err
	}
	batches := make([]timedBatch, 0, count)
	for i := uint32(0); i < count; i++ {
		maxTimestamp, err := reader.uint64()
		if err != nil {
			return nil, err
		}
		data, err := reader.sized()
		if err != nil {
			return nil, err
		}
		batches = append(batches, timedBatch{maxTimestamp: int64(maxTimestamp), data: data})
	}
	return batches, nil
}

func (p *statefulJoinProcessor) joinStored(batch arrow.Record, stored []timedBatch, batchIsLeft bool, outputs *[]arrow.Record) error {
	for _, value := range stored {
		decoded, err := p.serde.deserialize(value.data)
		if err != nil {
			return err
		}
		if batchIsLeft {
			err = p.addMatches(batch, decoded, outputs)
		} else {
			err = p.addMatches(decoded, batch, outputs)
		}
		decoded.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *statefulJoinProcessor) addMatches(leftBatch, rightBatch arrow.Record, outputs *[]arrow.Record) error {
	leftKeys := columnByName(leftBatch, p.join.LeftKey)
	if leftKeys == nil {
		return fmt.Errorf("join key column does not exist: %s", p.join.LeftKey)
	}
	rightKeys := columnByName(rightBatch, p.join.RightKey)
	if rightKeys == nil {
		return fmt.Errorf("join key column does not exist: %s", p.join.RightKey)
	}
	leftTimestamps, err := joinTimestamps(leftBatch)
	if err != nil {
		return err
	}
	rightTimestamps, err := joinTimestamps(rightBatch)
	if err != nil {
		return err
	}
	var pairs []rowPair
	// The nested scan is bounded by the join window; add a hash index if
	// profiling demands it.
	for leftRow := 0; leftRow < int(leftBatch.NumRows()); leftRow++ {
		leftKey := arrowValue(leftKeys, leftRow)
		if leftKey == nil {
			continue
		}
		encodedLeft, err := encodeValue(leftKey)
		if err != nil {
			return err
		}
		for rightRow := 0; rightRow < int(rightBatch.NumRows()); rightRow++ {
			rightKey := arrowValue(rightKeys, rightRow)
			if rightKey == nil {
				continue
			}
			encodedRight, err := encodeValue(rightKey)
			if err != nil {
				return err
			}
			if string(encodedLeft) == string(encodedRight) &&
				p.withinWindow(leftTimestamps.Value(leftRow), rightTimestamps.Value(rightRow)) {
				pairs = append(pairs, rowPair{leftRow: leftRow, rightRow: rightRow})
			}
		}
	}
	if len(pairs) > 0 {
		joined, err := joinRows(leftBatch, rightBatch, pairs, p.join.LeftPrefix, p.join.RightPrefix, p.mem)
		if err != nil {
			return err
		}
		*outputs = append(*outputs, joined)
	}
	return nil
}

func (p *statefulJoinProcessor) withinWindow(leftTimestamp, rightTimestamp int64) bool {
	difference := leftTimestamp - rightTimestamp
	if (rightTimestamp > 0 && difference > leftTimestamp) ||
		(rightTimestamp < 0 && difference < leftTimestamp) ||
		difference == math.MinInt64 {
		return false
	}
	if difference < 0 {
		difference = -difference
	}
	return difference <= p.join.Window.Milliseconds()
}

func (p *statefulJoinProcessor) advanceStreamTime(batch arrow.Record) error {
	timestamps, err := joinTimestamps(batch)
	if err != nil {
		return err
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		if !timestamps.IsNull(row) && timestamps.Value(row) > p.streamTime {
			p.streamTime = timestamps.Value(row)
		}
	}
	return nil
}

func (p *statefulJoinProcessor) prune(batches []timedBatch) []timedBatch {
	cutoff := saturatingSubtract(p.streamTime, p.join.Window.Milliseconds())
	var kept []timedBatch
	for _, batch := range batches {
		if batch.maxTimestamp >= cutoff {
			kept = append(kept, batch)
		}
	}
	return kept
}

func (p *statefulJoinProcessor) stored(batch arrow.Record) (timedBatch, error) {
	maximum := int64(math.MinInt64)
	timestamps, err := joinTimestamps(batch)
	if err != nil {
		return timedBatch{}, err
	}
	for row := 0; row < int(batch.NumRows()); row++ {
		if !timestamps.IsNull(row) && timestamps.Value(row) > maximum {
			maximum = timestamps.Value(row)
		}
	}
	data, err := p.serde.serialize(batch)
	if err != nil {
		return timedBatch{}, err
	}
	return timedBatch{maxTimestamp: maximum, data: data}, nil
}

func joinTimestamps(batch arrow.Record) (*array.Int64, error) {
	timestamps, ok := columnByName(batch, TimestampColumn).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("join requires %s", TimestampColumn)
	}
	return timestamps, nil
}
