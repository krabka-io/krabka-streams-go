package coordination

import (
	"encoding/binary"
	"fmt"
)

// DefaultPartitions is the partition count that this package assumes for
// [StateTopic]. Pass the real count of the cluster with [WithPartitions] when
// an operator created the topic with another count.
const DefaultPartitions = 16

// RolePartition returns the partition of [StateTopic] that holds every record
// of role. It rejects a partition count below one.
//
// The rule is Kafka's own key partitioning: murmur2 of the role name in
// UTF-8, masked with Utils.toPositive, and then the remainder of partitions.
// The mask clears the sign bit. It is not an absolute value, and the two
// differ for every negative hash. krabka-client-rs and krabka-streams-java
// write the same topic, so they compute the same rule. A change here needs
// the same change in the two ports.
//
// Both writers of a role pin the partition with this rule. The pin is a
// correctness requirement and not a preference. Kafka's default partitioner
// hashes the record key. The registration key of a role and the lease key of
// the same role differ, because the registration key names a member and the
// lease key does not. A partitioner that reads the key puts the two kinds in
// two partitions, and the total order that the succession rules rank on is
// gone.
func RolePartition(role Role, partitions int) (int, error) {
	if partitions < 1 {
		return 0, fmt.Errorf("topic %s needs at least one partition, got %d", StateTopic, partitions)
	}
	return int(murmur2([]byte(role.name))&0x7fffffff) % partitions, nil
}

// murmur2 is MurmurHash2, the key hash of Kafka's DefaultPartitioner. This is
// the reference implementation, and the length cast to a 32-bit value matches
// the canonical specification.
func murmur2(data []byte) int32 {
	const seed uint32 = 0x9747b28c
	const multiplier uint32 = 0x5bd1e995
	const rotation = 24

	hash := seed ^ uint32(len(data))
	blocks := len(data) / 4
	for block := range blocks {
		chunk := binary.LittleEndian.Uint32(data[block*4:])
		chunk *= multiplier
		chunk ^= chunk >> rotation
		chunk *= multiplier
		hash *= multiplier
		hash ^= chunk
	}

	tail := data[blocks*4:]
	switch len(tail) {
	case 3:
		hash ^= uint32(tail[2]) << 16
		hash ^= uint32(tail[1]) << 8
		hash ^= uint32(tail[0])
		hash *= multiplier
	case 2:
		hash ^= uint32(tail[1]) << 8
		hash ^= uint32(tail[0])
		hash *= multiplier
	case 1:
		hash ^= uint32(tail[0])
		hash *= multiplier
	}

	hash ^= hash >> 13
	hash *= multiplier
	hash ^= hash >> 15
	return int32(hash)
}
