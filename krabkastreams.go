// Package krabkastreams applies the client settings that a krabka streams
// group needs.
//
// krabka brokers coordinate stream processing applications through the streams
// group protocol (KIP-1071). The only krabka-specific configuration step is to
// enable that protocol, which [WithDefaults] does for you while leaving every
// setting you supply untouched.
//
// The helper operates on a plain string-keyed configuration map, the shape
// used by librdkafka-based clients such as confluent-kafka-go. Clients that
// take typed options instead can use the exported constants directly.
package krabkastreams

// GroupProtocolConfig is the Kafka client configuration key that selects the
// group protocol.
const GroupProtocolConfig = "group.protocol"

// StreamsGroupProtocol is the group.protocol value that enables the streams
// group protocol.
const StreamsGroupProtocol = "streams"

// WithDefaults copies the supplied settings and adds krabka defaults.
// Explicit settings keep their values.
//
// Currently the only default is [GroupProtocolConfig] set to
// [StreamsGroupProtocol]. The supplied map is not modified; a nil map is
// treated as empty.
func WithDefaults(settings map[string]any) map[string]any {
	result := make(map[string]any, len(settings)+1)
	for key, value := range settings {
		result[key] = value
	}
	if _, ok := result[GroupProtocolConfig]; !ok {
		result[GroupProtocolConfig] = StreamsGroupProtocol
	}
	return result
}
