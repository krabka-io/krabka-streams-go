package krabkastreams

import "testing"

func TestAppliesStreamsGroupProtocolByDefault(t *testing.T) {
	result := WithDefaults(map[string]any{"application.id": "order-counter"})

	if result[GroupProtocolConfig] != StreamsGroupProtocol {
		t.Fatalf("unexpected protocol %v", result[GroupProtocolConfig])
	}
	if result["application.id"] != "order-counter" {
		t.Fatal("settings must be copied through")
	}
}

func TestExplicitSettingsAlwaysWin(t *testing.T) {
	result := WithDefaults(map[string]any{GroupProtocolConfig: "classic"})

	if result[GroupProtocolConfig] != "classic" {
		t.Fatalf("explicit setting was overwritten: %v", result[GroupProtocolConfig])
	}
}

func TestInputMapIsNotModified(t *testing.T) {
	settings := map[string]any{"bootstrap.servers": "localhost:9092"}

	_ = WithDefaults(settings)

	if _, ok := settings[GroupProtocolConfig]; ok {
		t.Fatal("input map was modified")
	}
	if WithDefaults(nil)[GroupProtocolConfig] != StreamsGroupProtocol {
		t.Fatal("nil settings must be treated as empty")
	}
}
