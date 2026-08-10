package schema

import (
	"bytes"
	"reflect"
	"testing"
)

func TestEncodeMagicBigEndianIDAndBody(t *testing.T) {
	frame := Encode(258, []byte{'x', 'y'})

	if !bytes.Equal(frame, []byte{0, 0, 0, 1, 2, 'x', 'y'}) {
		t.Fatalf("unexpected frame %v", frame)
	}
	decoded, err := Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaID != 258 || !bytes.Equal(decoded.Body, []byte{'x', 'y'}) {
		t.Fatalf("unexpected decode %+v", decoded)
	}
}

func TestUsesSingleZeroForTopLevelProtobufMessage(t *testing.T) {
	frame, err := EncodeProtobuf(7, []int{0}, []byte{'p', 'b'})
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(frame, []byte{0, 0, 0, 0, 7, 0, 'p', 'b'}) {
		t.Fatalf("unexpected frame %v", frame)
	}
	decoded, err := DecodeProtobuf(frame)
	if err != nil {
		t.Fatal(err)
	}
	expected := ProtobufFrame{SchemaID: 7, MessageIndexes: []int{0}, Body: []byte{'p', 'b'}}
	if !reflect.DeepEqual(decoded, expected) {
		t.Fatalf("unexpected decode %+v", decoded)
	}
}

func TestRoundTripsNestedProtobufIndexes(t *testing.T) {
	frame, err := EncodeProtobuf(9, []int{1, 0}, []byte{3})
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeProtobuf(frame)
	if err != nil {
		t.Fatal(err)
	}
	expected := ProtobufFrame{SchemaID: 9, MessageIndexes: []int{1, 0}, Body: []byte{3}}
	if !reflect.DeepEqual(decoded, expected) {
		t.Fatalf("unexpected decode %+v", decoded)
	}
}

func TestRejectsShortAndInvalidFrames(t *testing.T) {
	if _, err := Decode(make([]byte, 4)); err == nil {
		t.Fatal("expected short-frame error")
	}
	if _, err := Decode([]byte{1, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected magic-byte error")
	}
	if _, err := DecodeProtobuf([]byte{0, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected truncated-varint error")
	}
}
