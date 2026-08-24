package columnar

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
)

// BarrierStateTopic is the internal topic a krabka broker writes the barrier
// group definitions, the injection starts, and the cuts to. Read it with a
// [CutReader].
const BarrierStateTopic = "__barrier_state"

// Record versions and key kinds of the barrier state topic. The formats are
// frozen: the broker, krabka-streams-rs, krabka-streams-java, and this
// library must agree byte for byte.
const (
	barrierStateKeyVersion = 0
	barrierCutValueVersion = 0

	barrierKindGroup          = 0
	barrierKindInjectionStart = 1
	barrierKindCut            = 2
)

// CutStatus reports whether every partition of a barrier group received the
// marker of an epoch.
type CutStatus int8

const (
	// CutComplete marks a cut that names a marker offset for every partition
	// of the group. Only a complete cut is alignable.
	CutComplete CutStatus = 0

	// CutPartial marks a cut that misses at least one partition. The
	// partitions in [BarrierCut.Missing] never receive the marker of that
	// epoch, so a task that waits for one waits forever. Skip a partial cut.
	CutPartial CutStatus = 1
)

// String names the status.
func (s CutStatus) String() string {
	switch s {
	case CutComplete:
		return "complete"
	case CutPartial:
		return "partial"
	default:
		return fmt.Sprintf("CutStatus(%d)", int8(s))
	}
}

// BarrierCut is the manifest of one epoch of a barrier group: the offset of
// that epoch's marker in every partition of the group.
//
// The marker itself is a Kafka control record. No consumer sees it, and the
// offset it holds never carries a data record, so the records before the cut
// of a partition are exactly the records with a lower offset.
type BarrierCut struct {
	// Group is the barrier group the cut belongs to.
	Group string

	// Epoch is the epoch of the cut. A coordinator never reuses one.
	Epoch int64

	// TriggeredAt is the time the injection started, in epoch milliseconds.
	TriggeredAt int64

	// CompletedAt is the time the injection finished, in epoch milliseconds.
	CompletedAt int64

	// Status reports whether every partition received the marker.
	Status CutStatus

	// Offsets is the marker offset of every partition that received one.
	Offsets map[TopicPartition]int64

	// Missing names the partitions that received no marker. It is empty for
	// a complete cut.
	Missing []TopicPartition
}

// Complete reports whether every partition of the group received the marker.
// A runner aligns on a complete cut only.
func (c BarrierCut) Complete() bool { return c.Status == CutComplete }

// Offset returns the marker offset of one partition, and reports whether the
// cut names it.
func (c BarrierCut) Offset(partition TopicPartition) (int64, bool) {
	offset, ok := c.Offsets[partition]
	return offset, ok
}

func (c BarrierCut) clone() BarrierCut {
	clone := c
	clone.Offsets = maps.Clone(c.Offsets)
	clone.Missing = slices.Clone(c.Missing)
	return clone
}

// BarrierFormatError reports a malformed record of [BarrierStateTopic].
type BarrierFormatError struct {
	// Part names the record part that failed to decode, "key" or "cut
	// value".
	Part string

	message string
}

// Error describes the fault and names the record part that carries it.
func (e *BarrierFormatError) Error() string { return e.message }

// DecodeBarrierCut decodes one record of [BarrierStateTopic].
//
// It returns a nil cut and a nil error for a record that carries no cut: a
// group definition, an injection start, and a tombstone. Malformed bytes
// return a [*BarrierFormatError].
func DecodeBarrierCut(key, value []byte) (*BarrierCut, error) {
	decodedKey, err := decodeBarrierKey(key)
	if err != nil {
		return nil, err
	}
	if decodedKey.kind != barrierKindCut || len(value) == 0 {
		return nil, nil
	}
	cut, err := decodeBarrierCutValue(decodedKey, value)
	if err != nil {
		return nil, err
	}
	return &cut, nil
}

// barrierKey is a decoded key of the barrier state topic. The kind
// discriminates the three record kinds.
type barrierKey struct {
	kind  int16
	group string
	epoch int64
}

func decodeBarrierKey(data []byte) (barrierKey, error) {
	reader := &barrierReader{data: data, part: "key"}
	version, err := reader.int16()
	if err != nil {
		return barrierKey{}, err
	}
	if version != barrierStateKeyVersion {
		return barrierKey{}, reader.errorf("unsupported barrier state key version %d", version)
	}
	kind, err := reader.int16()
	if err != nil {
		return barrierKey{}, err
	}
	group, err := reader.string()
	if err != nil {
		return barrierKey{}, err
	}
	epoch, err := reader.int64()
	if err != nil {
		return barrierKey{}, err
	}
	if !reader.empty() {
		return barrierKey{}, reader.errorf("trailing bytes in barrier state key")
	}
	return barrierKey{kind: kind, group: group, epoch: epoch}, nil
}

func decodeBarrierCutValue(key barrierKey, data []byte) (BarrierCut, error) {
	reader := &barrierReader{data: data, part: "cut value"}
	version, err := reader.int16()
	if err != nil {
		return BarrierCut{}, err
	}
	if version != barrierCutValueVersion {
		return BarrierCut{}, reader.errorf("unsupported barrier state cut value version %d", version)
	}
	triggeredAt, err := reader.int64()
	if err != nil {
		return BarrierCut{}, err
	}
	completedAt, err := reader.int64()
	if err != nil {
		return BarrierCut{}, err
	}
	rawStatus, err := reader.int8()
	if err != nil {
		return BarrierCut{}, err
	}
	status := CutStatus(rawStatus)
	if status != CutComplete && status != CutPartial {
		return BarrierCut{}, reader.errorf("unknown barrier cut status %d", rawStatus)
	}
	offsets, err := reader.cutOffsets()
	if err != nil {
		return BarrierCut{}, err
	}
	missing, err := reader.missingPartitions()
	if err != nil {
		return BarrierCut{}, err
	}
	if !reader.empty() {
		return BarrierCut{}, reader.errorf("trailing bytes in barrier state cut value")
	}
	return BarrierCut{
		Group:       key.group,
		Epoch:       key.epoch,
		TriggeredAt: triggeredAt,
		CompletedAt: completedAt,
		Status:      status,
		Offsets:     offsets,
		Missing:     missing,
	}, nil
}

// barrierReader reads the big-endian layout of the barrier state topic. Its
// integers are signed, and a string is an int16 byte length and then UTF-8
// bytes.
type barrierReader struct {
	data []byte
	part string
}

func (r *barrierReader) empty() bool { return len(r.data) == 0 }

func (r *barrierReader) errorf(format string, args ...any) error {
	return &BarrierFormatError{Part: r.part, message: fmt.Sprintf(format, args...)}
}

func (r *barrierReader) truncated() error {
	return r.errorf("truncated barrier state %s", r.part)
}

func (r *barrierReader) int8() (int8, error) {
	if len(r.data) < 1 {
		return 0, r.truncated()
	}
	result := int8(r.data[0])
	r.data = r.data[1:]
	return result, nil
}

func (r *barrierReader) int16() (int16, error) {
	if len(r.data) < 2 {
		return 0, r.truncated()
	}
	result := int16(binary.BigEndian.Uint16(r.data))
	r.data = r.data[2:]
	return result, nil
}

func (r *barrierReader) int32() (int32, error) {
	if len(r.data) < 4 {
		return 0, r.truncated()
	}
	result := int32(binary.BigEndian.Uint32(r.data))
	r.data = r.data[4:]
	return result, nil
}

func (r *barrierReader) int64() (int64, error) {
	if len(r.data) < 8 {
		return 0, r.truncated()
	}
	result := int64(binary.BigEndian.Uint64(r.data))
	r.data = r.data[8:]
	return result, nil
}

func (r *barrierReader) string() (string, error) {
	length, err := r.int16()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", r.errorf("negative string length %d in barrier state %s", length, r.part)
	}
	if len(r.data) < int(length) {
		return "", r.truncated()
	}
	result := string(r.data[:length])
	r.data = r.data[length:]
	return result, nil
}

// count reads an int32 array length. A negative length is malformed, and a
// length beyond the remaining bytes fails on the first entry, so no bogus
// count allocates.
func (r *barrierReader) count() (int, error) {
	length, err := r.int32()
	if err != nil {
		return 0, err
	}
	if length < 0 {
		return 0, r.errorf("negative array length %d in barrier state %s", length, r.part)
	}
	return int(length), nil
}

func (r *barrierReader) cutOffsets() (map[TopicPartition]int64, error) {
	topics, err := r.count()
	if err != nil {
		return nil, err
	}
	offsets := map[TopicPartition]int64{}
	for range topics {
		topic, err := r.string()
		if err != nil {
			return nil, err
		}
		partitions, err := r.count()
		if err != nil {
			return nil, err
		}
		for range partitions {
			partition, err := r.int32()
			if err != nil {
				return nil, err
			}
			offset, err := r.int64()
			if err != nil {
				return nil, err
			}
			offsets[TopicPartition{Topic: topic, Partition: int(partition)}] = offset
		}
	}
	return offsets, nil
}

func (r *barrierReader) missingPartitions() ([]TopicPartition, error) {
	entries, err := r.count()
	if err != nil {
		return nil, err
	}
	var missing []TopicPartition
	for range entries {
		topic, err := r.string()
		if err != nil {
			return nil, err
		}
		partition, err := r.int32()
		if err != nil {
			return nil, err
		}
		missing = append(missing, TopicPartition{Topic: topic, Partition: int(partition)})
	}
	return missing, nil
}
