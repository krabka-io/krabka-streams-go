package columnar

import (
	"encoding/hex"
	"testing"
)

// The cut wire format is frozen across krabka-broker, krabka-streams-rs,
// krabka-streams-java and this package. These bytes are encoded straight from
// the specification in the broker's barrier design document, independently of
// any of the four implementations, so a decoder that drifts fails here.
//
// Key:   version 0, kind 2 (cut), group "orders-cut", epoch 7.
// Value: version 0, triggered 1724500000000, completed 1724500000042,
//        status 0 (complete), topic "orders" with partitions 0 at offset 1024
//        and 1 at offset 2048, and no missing partitions.
const (
	goldenCutKeyHex   = "00000002000a6f72646572732d6375740000000000000007"
	goldenCutValueHex = "0000000001918435bd00000001918435bd2a000000000100066f72" +
		"6465727300000002000000000000000000000400000000010000000000000800" +
		"00000000"
)

func TestDecodeBarrierCutMatchesTheFrozenGoldenBytes(t *testing.T) {
	key, err := hex.DecodeString(goldenCutKeyHex)
	if err != nil {
		t.Fatalf("decode golden key: %v", err)
	}
	value, err := hex.DecodeString(goldenCutValueHex)
	if err != nil {
		t.Fatalf("decode golden value: %v", err)
	}

	cut, err := DecodeBarrierCut(key, value)
	if err != nil {
		t.Fatalf("DecodeBarrierCut: %v", err)
	}
	if cut == nil {
		t.Fatal("DecodeBarrierCut returned no cut for a cut record")
	}

	if cut.Group != "orders-cut" {
		t.Errorf("Group = %q, want %q", cut.Group, "orders-cut")
	}
	if cut.Epoch != 7 {
		t.Errorf("Epoch = %d, want 7", cut.Epoch)
	}
	if cut.TriggeredAt != 1724500000000 {
		t.Errorf("TriggeredAt = %d, want 1724500000000", cut.TriggeredAt)
	}
	if cut.CompletedAt != 1724500000042 {
		t.Errorf("CompletedAt = %d, want 1724500000042", cut.CompletedAt)
	}
	if !cut.Complete() {
		t.Errorf("Complete() = false, want true")
	}
	if len(cut.Missing) != 0 {
		t.Errorf("Missing = %v, want empty", cut.Missing)
	}

	want := map[TopicPartition]int64{
		{Topic: "orders", Partition: 0}: 1024,
		{Topic: "orders", Partition: 1}: 2048,
	}
	if len(cut.Offsets) != len(want) {
		t.Fatalf("Offsets = %v, want %v", cut.Offsets, want)
	}
	for tp, offset := range want {
		if got := cut.Offsets[tp]; got != offset {
			t.Errorf("Offsets[%v] = %d, want %d", tp, got, offset)
		}
	}
}
