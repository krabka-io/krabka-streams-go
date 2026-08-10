package schema

import (
	"encoding/binary"
	"fmt"
)

// Magic is the first byte of every Confluent-framed record.
const Magic byte = 0

const headerSize = 5

// Frame is a decoded Confluent frame. The body is defensively copied at
// construction, so a frame never aliases a caller's slice.
type Frame struct {
	// SchemaID is the registry schema ID from the header.
	SchemaID int

	// Body is the record body that followed the header.
	Body []byte
}

// ProtobufFrame is a decoded Confluent Protobuf frame. The body is defensively
// copied at construction, so a frame never aliases a caller's slice.
type ProtobufFrame struct {
	// SchemaID is the registry schema ID from the header.
	SchemaID int

	// MessageIndexes is the message-index path, outermost first.
	MessageIndexes []int

	// Body is the serialized Protobuf message that followed the indexes.
	Body []byte
}

// Encode frames body bytes with the magic byte and schema ID.
//
// Every framed record starts with a five-byte header: the [Magic] byte
// followed by the schema ID as a big-endian 32-bit integer.
func Encode(schemaID int, body []byte) []byte {
	result := make([]byte, headerSize+len(body))
	result[0] = Magic
	binary.BigEndian.PutUint32(result[1:], uint32(schemaID))
	copy(result[headerSize:], body)
	return result
}

// Decode splits framed record bytes into the schema ID and body.
//
// It fails if the input is shorter than five bytes or does not start with
// [Magic].
func Decode(data []byte) (Frame, error) {
	if err := checkHeader(data); err != nil {
		return Frame{}, err
	}
	body := make([]byte, len(data)-headerSize)
	copy(body, data[headerSize:])
	return Frame{SchemaID: int(int32(binary.BigEndian.Uint32(data[1:]))), Body: body}, nil
}

// EncodeProtobuf frames Protobuf body bytes, including the message-index list
// encoded as zigzag varints between the header and the body.
//
// The indexes identify the message within its .proto file: each entry is the
// message's position at its nesting level, outermost first. The common [0]
// path (first top-level message) is written in its compact single-byte form.
// The index path must not be empty.
func EncodeProtobuf(schemaID int, messageIndexes []int, body []byte) ([]byte, error) {
	if len(messageIndexes) == 0 {
		return nil, fmt.Errorf("messageIndexes must not be empty")
	}
	result := make([]byte, headerSize, headerSize+len(body)+8)
	result[0] = Magic
	binary.BigEndian.PutUint32(result[1:], uint32(schemaID))
	if len(messageIndexes) == 1 && messageIndexes[0] == 0 {
		result = append(result, 0)
	} else {
		result = appendZigzag(result, int64(len(messageIndexes)))
		for _, index := range messageIndexes {
			result = appendZigzag(result, int64(index))
		}
	}
	return append(result, body...), nil
}

// DecodeProtobuf splits framed Protobuf record bytes into schema ID, message
// indexes, and body.
//
// It fails if the input is shorter than five bytes, does not start with
// [Magic], or carries a malformed message-index list.
func DecodeProtobuf(data []byte) (ProtobufFrame, error) {
	if err := checkHeader(data); err != nil {
		return ProtobufFrame{}, err
	}
	schemaID := int(int32(binary.BigEndian.Uint32(data[1:])))
	rest := data[headerSize:]
	count, rest, err := readZigzag(rest)
	if err != nil {
		return ProtobufFrame{}, err
	}
	var indexes []int
	if count == 0 {
		indexes = []int{0}
	} else {
		if count < 0 || count > int64(^uint32(0)>>1) {
			return ProtobufFrame{}, fmt.Errorf("invalid Protobuf message-index count: %d", count)
		}
		indexes = make([]int, count)
		for i := range indexes {
			var value int64
			value, rest, err = readZigzag(rest)
			if err != nil {
				return ProtobufFrame{}, err
			}
			if value < -1<<31 || value > 1<<31-1 {
				return ProtobufFrame{}, fmt.Errorf("Protobuf message index is outside the int32 range")
			}
			indexes[i] = int(value)
		}
	}
	body := make([]byte, len(rest))
	copy(body, rest)
	return ProtobufFrame{SchemaID: schemaID, MessageIndexes: indexes, Body: body}, nil
}

func checkHeader(data []byte) error {
	if len(data) < headerSize {
		return fmt.Errorf("schema frame is shorter than 5 bytes")
	}
	if data[0] != Magic {
		return fmt.Errorf("invalid schema frame magic byte 0x%02x", data[0])
	}
	return nil
}

func appendZigzag(buffer []byte, value int64) []byte {
	encoded := uint64((value << 1) ^ (value >> 63))
	for encoded&^0x7f != 0 {
		buffer = append(buffer, byte(encoded&0x7f|0x80))
		encoded >>= 7
	}
	return append(buffer, byte(encoded))
}

func readZigzag(data []byte) (int64, []byte, error) {
	var result uint64
	for shift := 0; shift < 64; shift += 7 {
		if len(data) == 0 {
			return 0, nil, fmt.Errorf("truncated Protobuf message-index varint")
		}
		current := data[0]
		data = data[1:]
		result |= uint64(current&0x7f) << shift
		if current&0x80 == 0 {
			return int64(result>>1) ^ -int64(result&1), data, nil
		}
	}
	return 0, nil, fmt.Errorf("Protobuf message-index varint is too long")
}
