package columnar

import (
	"fmt"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Processor transforms one Arrow batch into zero or more Arrow batches by
// calling [Context.Forward] once per output batch.
//
// The input batch belongs to the framework: never release it. Forwarding a
// batch transfers its ownership to the framework; a batch you create and do
// not forward is yours to release.
type Processor interface {
	// Process handles one batch.
	Process(ctx *Context, batch arrow.Record) error
}

// ProcessorFunc adapts a function to the [Processor] interface.
type ProcessorFunc func(ctx *Context, batch arrow.Record) error

// Process implements [Processor].
func (f ProcessorFunc) Process(ctx *Context, batch arrow.Record) error { return f(ctx, batch) }

// StatefulProcessor is a [Processor] that participates in partition snapshot
// and restore.
type StatefulProcessor interface {
	Processor

	// Snapshot serializes the processor state.
	Snapshot() ([]byte, error)

	// Restore replaces the processor state with a snapshot.
	Restore(snapshot []byte) error
}

// Context collects the batches a processor forwards.
type Context struct {
	forwarded []arrow.Record
}

// Forward emits one output batch to every downstream node. Calling it zero
// times drops the input; calling it several times fans the input out.
func (c *Context) Forward(batch arrow.Record) {
	c.forwarded = append(c.forwarded, batch)
}

func (c *Context) drain() []arrow.Record {
	result := c.forwarded
	c.forwarded = nil
	return result
}

func (c *Context) contains(batch arrow.Record) bool {
	return slices.Contains(c.forwarded, batch)
}

// Join configures a stateful co-partitioned inner equi-join within an
// event-time window.
type Join struct {
	// LeftKey is the join key column on the left side.
	LeftKey string

	// RightKey is the join key column on the right side.
	RightKey string

	// Window is the event-time window; rows whose timestamps differ by more
	// than the window never join.
	Window time.Duration

	// LeftPrefix prefixes left payload columns in the output. Empty means
	// "left_".
	LeftPrefix string

	// RightPrefix prefixes right payload columns in the output. Empty means
	// "right_".
	RightPrefix string
}

func (j Join) normalized() (Join, error) {
	if j.LeftPrefix == "" {
		j.LeftPrefix = "left_"
	}
	if j.RightPrefix == "" {
		j.RightPrefix = "right_"
	}
	if j.Window < 0 {
		return j, fmt.Errorf("window must not be negative")
	}
	if j.Window != 0 && j.Window.Milliseconds() == 0 {
		return j, fmt.Errorf("window must use millisecond precision")
	}
	return j, nil
}

// Node is an opaque handle to a topology node, passed as the parent of later
// nodes.
type Node struct {
	owner *Topology
	index int
}

type nodeType int

const (
	nodeSource nodeType = iota
	nodeOperator
	nodeMerge
	nodeJoin
	nodeSink
)

type nodeDefinition struct {
	name         string
	kind         nodeType
	sourceTopics []string
	sourceCodec  BatchCodec
	processor    func() Processor
	sinkTopic    string
	sinkCodec    BatchCodec
	join         Join
	parents      []Node
}

// Topology is a builder for a columnar processing graph. Nodes are added in
// order, each non-source node names a parent that must already exist, and
// [Topology.Build] validates the result. A topology is not safe for
// concurrent use while building.
type Topology struct {
	mem   memory.Allocator
	nodes []nodeDefinition
}

// NewTopology creates a builder whose batches are allocated from mem.
func NewTopology(mem memory.Allocator) *Topology {
	return &Topology{mem: mem}
}

// AddSource adds a source that decodes records for any of the topics. At
// least one topic is required.
func (t *Topology) AddSource(name string, topics []string, codec BatchCodec) (Node, error) {
	if len(topics) == 0 {
		return Node{}, fmt.Errorf("a source needs at least one topic")
	}
	if codec == nil {
		return Node{}, fmt.Errorf("codec must not be nil")
	}
	return t.add(nodeDefinition{
		name:         name,
		kind:         nodeSource,
		sourceTopics: append([]string{}, topics...),
		sourceCodec:  codec,
	}), nil
}

// AddOperator adds a built-in operator.
func (t *Topology) AddOperator(name string, operator *BuiltinOp, parent Node) (Node, error) {
	if operator == nil {
		return Node{}, fmt.Errorf("operator must not be nil")
	}
	return t.AddProcessor(name, func() Processor { return operator.fresh() }, parent)
}

// AddProcessor adds a custom processor; its factory is invoked once per
// logical partition.
func (t *Topology) AddProcessor(name string, processor func() Processor, parent Node) (Node, error) {
	if err := t.requireParent(parent); err != nil {
		return Node{}, err
	}
	if processor == nil {
		return Node{}, fmt.Errorf("processor must not be nil")
	}
	return t.add(nodeDefinition{
		name:      name,
		kind:      nodeOperator,
		processor: processor,
		parents:   []Node{parent},
	}), nil
}

// AddMerge concatenates two or more same-schema upstream branches.
func (t *Topology) AddMerge(name string, parents []Node) (Node, error) {
	if len(parents) < 2 {
		return Node{}, fmt.Errorf("a merge needs at least two parents")
	}
	for _, parent := range parents {
		if err := t.requireParent(parent); err != nil {
			return Node{}, err
		}
	}
	return t.add(nodeDefinition{
		name:    name,
		kind:    nodeMerge,
		parents: append([]Node{}, parents...),
	}), nil
}

// AddJoin adds a stateful co-partitioned inner equi-join within an event-time
// window.
func (t *Topology) AddJoin(name string, join Join, left, right Node) (Node, error) {
	if err := t.requireParent(left); err != nil {
		return Node{}, err
	}
	if err := t.requireParent(right); err != nil {
		return Node{}, err
	}
	if left == right {
		return Node{}, fmt.Errorf("a join needs two different parents")
	}
	normalized, err := join.normalized()
	if err != nil {
		return Node{}, err
	}
	return t.add(nodeDefinition{
		name:    name,
		kind:    nodeJoin,
		join:    normalized,
		parents: []Node{left, right},
	}), nil
}

// AddSink encodes its parent's batches to a topic.
func (t *Topology) AddSink(name, topic string, codec BatchCodec, parent Node) (Node, error) {
	if err := t.requireParent(parent); err != nil {
		return Node{}, err
	}
	if codec == nil {
		return Node{}, fmt.Errorf("codec must not be nil")
	}
	return t.add(nodeDefinition{
		name:      name,
		kind:      nodeSink,
		sinkTopic: topic,
		sinkCodec: codec,
		parents:   []Node{parent},
	}), nil
}

// AddPassThroughSink copies source records byte-for-byte to a topic without
// codec work. It must be attached directly to a source.
func (t *Topology) AddPassThroughSink(name, topic string, source Node) (Node, error) {
	if err := t.requireParent(source); err != nil {
		return Node{}, err
	}
	if t.nodes[source.index].kind != nodeSource {
		return Node{}, fmt.Errorf("a pass-through sink must be attached directly to a source")
	}
	return t.add(nodeDefinition{
		name:      name,
		kind:      nodeSink,
		sinkTopic: topic,
		parents:   []Node{source},
	}), nil
}

// SourceTopics returns every topic named by any source, for subscribing a
// consumer.
func (t *Topology) SourceTopics() []string {
	var result []string
	for _, node := range t.nodes {
		if node.kind == nodeSource {
			result = append(result, node.sourceTopics...)
		}
	}
	return result
}

// Validate checks the graph: node names are unique, every non-source node has
// a parent that was added earlier, and at least one source and one sink
// exist. Because a parent must already exist when a child is added, cycles
// are impossible by construction.
func (t *Topology) Validate() error {
	names := map[string]bool{}
	sourceCount, sinkCount := 0, 0
	for index, node := range t.nodes {
		if names[node.name] {
			return fmt.Errorf("duplicate node name `%s`", node.name)
		}
		names[node.name] = true
		if node.kind == nodeSource {
			sourceCount++
		} else {
			if len(node.parents) == 0 {
				return fmt.Errorf("node `%s` has an invalid parent", node.name)
			}
			for _, parent := range node.parents {
				if parent.index >= index {
					return fmt.Errorf("node `%s` has an invalid parent", node.name)
				}
			}
		}
		if node.kind == nodeSink {
			sinkCount++
		}
	}
	if sourceCount == 0 {
		return fmt.Errorf("topology has no source")
	}
	if sinkCount == 0 {
		return fmt.Errorf("topology has no sink")
	}
	return nil
}

// Build validates the topology and returns a reusable [BuiltTopology].
func (t *Topology) Build() (*BuiltTopology, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return &BuiltTopology{topology: t, processors: map[int]map[int]Processor{}}, nil
}

func (t *Topology) add(definition nodeDefinition) Node {
	node := Node{owner: t, index: len(t.nodes)}
	t.nodes = append(t.nodes, definition)
	return node
}

func (t *Topology) requireParent(parent Node) error {
	if parent.owner != t || parent.index < 0 || parent.index >= len(t.nodes) {
		return fmt.Errorf("parent is not a node in this topology")
	}
	return nil
}
