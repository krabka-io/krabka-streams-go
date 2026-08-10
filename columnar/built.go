package columnar

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// BuiltTopology is a validated, reusable columnar topology.
//
// The processor instances created when a topology is built survive across
// calls to RunBatch, so built-in GroupBy accumulates across batches and
// custom processors can keep state. Processor instances are isolated by
// logical partition number, so co-partitioned source topics can join without
// sharing state with another partition.
//
// A built topology serializes concurrent calls so Arrow batches are never
// shared across goroutines. All intermediate batches are released before
// RunBatch returns, including on the error path.
type BuiltTopology struct {
	mu         sync.Mutex
	topology   *Topology
	processors map[int]map[int]Processor
}

// RunBatch evaluates the topology for one source topic's records and returns
// the produced records.
func (b *BuiltTopology) RunBatch(topic string, records []ConsumedRecord) ([]ProducedToTopic, error) {
	return b.RunBatches(map[string][]ConsumedRecord{topic: records})
}

// RunBatches evaluates several source topics together, which lets merges
// express fan-in across topics. Records are partitioned by their logical
// partition number, and each partition runs with its own processor state.
func (b *BuiltTopology) RunBatches(input map[string][]ConsumedRecord) ([]ProducedToTopic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	partitionSet := map[int]bool{}
	for _, records := range input {
		for _, record := range records {
			partitionSet[record.Partition] = true
		}
	}
	partitions := slices.Sorted(maps.Keys(partitionSet))
	var result []ProducedToTopic
	for _, partition := range partitions {
		partitionInput := map[string][]ConsumedRecord{}
		for topic, records := range input {
			var filtered []ConsumedRecord
			for _, record := range records {
				if record.Partition == partition {
					filtered = append(filtered, record)
				}
			}
			partitionInput[topic] = filtered
		}
		produced, err := b.runPartition(partition, partitionInput)
		if err != nil {
			return nil, err
		}
		result = append(result, produced...)
	}
	return result, nil
}

// RunPartitionBatches evaluates one logical partition's records. Every record
// must belong to the given partition.
func (b *BuiltTopology) RunPartitionBatches(partition int, input map[string][]ConsumedRecord) ([]ProducedToTopic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.runPartition(partition, input)
}

func (b *BuiltTopology) runPartition(partition int, input map[string][]ConsumedRecord) ([]ProducedToTopic, error) {
	for _, records := range input {
		for _, record := range records {
			if record.Partition != partition {
				return nil, fmt.Errorf("record does not belong to partition %d", partition)
			}
		}
	}
	frames := map[int][]arrow.Record{}
	defer func() {
		released := map[arrow.Record]bool{}
		for _, batches := range frames {
			for _, batch := range batches {
				if !released[batch] {
					released[batch] = true
					batch.Release()
				}
			}
		}
	}()
	var produced []ProducedToTopic
	nodes := b.topology.nodes
	for index, node := range nodes {
		switch node.kind {
		case nodeSource:
			needsDecodedFrame := false
			for _, candidate := range nodes {
				if candidate.kind == nodeSink && candidate.sinkCodec == nil {
					continue
				}
				for _, parent := range candidate.parents {
					if parent.index == index {
						needsDecodedFrame = true
					}
				}
			}
			var decoded []arrow.Record
			if needsDecodedFrame {
				for _, topic := range sortedTopics(input) {
					records := input[topic]
					if len(records) > 0 && slices.Contains(node.sourceTopics, topic) {
						batch, err := node.sourceCodec.Decode(topic, records)
						if err != nil {
							return nil, err
						}
						decoded = append(decoded, batch)
					}
				}
			}
			frames[index] = decoded
		case nodeOperator:
			outputs, err := b.runOperator(partition, index, node, frames)
			if err != nil {
				return nil, err
			}
			frames[index] = outputs
		case nodeMerge:
			outputs, err := b.merge(node, frames)
			if err != nil {
				return nil, err
			}
			frames[index] = outputs
		case nodeJoin:
			join := b.partitionProcessors(partition)[index].(*statefulJoinProcessor)
			outputs, err := join.processSides(
				frames[node.parents[0].index],
				frames[node.parents[1].index])
			if err != nil {
				return nil, err
			}
			frames[index] = outputs
		case nodeSink:
			if node.sinkCodec == nil {
				parent := nodes[node.parents[0].index]
				for _, topic := range sortedTopics(input) {
					if slices.Contains(parent.sourceTopics, topic) {
						for _, record := range input[topic] {
							produced = append(produced, ProducedToTopic{
								Topic: node.sinkTopic,
								Record: NewProduceRecord(
									record.Key, record.Value, record.Timestamp, record.Headers...),
							})
						}
					}
				}
				continue
			}
			for _, batch := range frames[node.parents[0].index] {
				records, err := node.sinkCodec.Encode(node.sinkTopic, batch)
				if err != nil {
					return nil, err
				}
				for _, record := range records {
					produced = append(produced, ProducedToTopic{Topic: node.sinkTopic, Record: record})
				}
			}
		default:
			return nil, fmt.Errorf("unknown columnar node type %d", node.kind)
		}
	}
	return produced, nil
}

func (b *BuiltTopology) runOperator(partition, nodeIndex int, node nodeDefinition, frames map[int][]arrow.Record) ([]arrow.Record, error) {
	var outputs []arrow.Record
	processor := b.partitionProcessors(partition)[nodeIndex]
	for _, parent := range frames[node.parents[0].index] {
		// Each processor call sees a private slice of the parent batch, so a
		// forwarded input is a distinct handle from the parent frame.
		input := copyRange(parent, 0, int(parent.NumRows()))
		ctx := &Context{}
		if err := processor.Process(ctx, input); err != nil {
			released := map[arrow.Record]bool{input: true}
			input.Release()
			for _, batch := range append(ctx.drain(), outputs...) {
				if !released[batch] {
					released[batch] = true
					batch.Release()
				}
			}
			return nil, err
		}
		forwardedInput := ctx.contains(input)
		outputs = append(outputs, ctx.drain()...)
		if !forwardedInput {
			input.Release()
		}
	}
	return outputs, nil
}

func (b *BuiltTopology) merge(node nodeDefinition, frames map[int][]arrow.Record) ([]arrow.Record, error) {
	var inputs []arrow.Record
	for _, parent := range node.parents {
		inputs = append(inputs, frames[parent.index]...)
	}
	if len(inputs) == 0 {
		return nil, nil
	}
	merged, err := concatenate(inputs, b.topology.mem)
	if err != nil {
		return nil, err
	}
	return []arrow.Record{merged}, nil
}

// SnapshotPartition serializes every stateful processor of a partition, keyed
// by node name.
func (b *BuiltTopology) SnapshotPartition(partition int) (map[string][]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapshotPartitionLocked(partition)
}

func (b *BuiltTopology) snapshotPartitionLocked(partition int) (map[string][]byte, error) {
	partitionProcessors, ok := b.processors[partition]
	if !ok {
		return map[string][]byte{}, nil
	}
	snapshots := map[string][]byte{}
	for index, processor := range partitionProcessors {
		if stateful, ok := processor.(StatefulProcessor); ok {
			snapshot, err := stateful.Snapshot()
			if err != nil {
				return nil, err
			}
			snapshots[b.topology.nodes[index].name] = bytes.Clone(snapshot)
		}
	}
	return snapshots, nil
}

// RestorePartition replaces the state of a partition's stateful processors
// with snapshots keyed by node name.
func (b *BuiltTopology) RestorePartition(partition int, snapshots map[string][]byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.restorePartitionLocked(partition, snapshots)
}

func (b *BuiltTopology) restorePartitionLocked(partition int, snapshots map[string][]byte) error {
	byName := map[string]Processor{}
	for index, processor := range b.partitionProcessors(partition) {
		byName[b.topology.nodes[index].name] = processor
	}
	for _, name := range slices.Sorted(maps.Keys(snapshots)) {
		if stateful, ok := byName[name].(StatefulProcessor); ok {
			if err := stateful.Restore(bytes.Clone(snapshots[name])); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReleasePartition discards a partition's processor instances and state.
func (b *BuiltTopology) ReleasePartition(partition int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.releasePartitionLocked(partition)
}

func (b *BuiltTopology) releasePartitionLocked(partition int) {
	delete(b.processors, partition)
}

// HasPartition reports whether the partition currently has processor state.
func (b *BuiltTopology) HasPartition(partition int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.processors[partition]
	return ok
}

// Close releases every partition.
func (b *BuiltTopology) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.processors = map[int]map[int]Processor{}
	return nil
}

func (b *BuiltTopology) partitionProcessors(partition int) map[int]Processor {
	if existing, ok := b.processors[partition]; ok {
		return existing
	}
	result := map[int]Processor{}
	for index, node := range b.topology.nodes {
		switch node.kind {
		case nodeOperator:
			result[index] = node.processor()
		case nodeJoin:
			result[index] = newStatefulJoinProcessor(node.join, b.topology.mem)
		}
	}
	b.processors[partition] = result
	return result
}

func sortedTopics(input map[string][]ConsumedRecord) []string {
	return slices.Sorted(maps.Keys(input))
}
