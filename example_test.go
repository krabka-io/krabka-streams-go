package krabkastreams_test

import (
	"fmt"

	krabkastreams "github.com/krabka-io/krabka-streams-go"
)

func ExampleWithDefaults() {
	settings := krabkastreams.WithDefaults(map[string]any{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "order-counter",
	})

	fmt.Println(settings[krabkastreams.GroupProtocolConfig])
	// Output: streams
}

func ExampleWithDefaults_explicitSettingsWin() {
	settings := krabkastreams.WithDefaults(map[string]any{
		krabkastreams.GroupProtocolConfig: "classic",
	})

	fmt.Println(settings[krabkastreams.GroupProtocolConfig])
	// Output: classic
}
