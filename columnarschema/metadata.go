// Package columnarschema connects the schema registry serdes to columnar
// topologies.
//
// AvroBatchCodec and ProtobufBatchCodec implement columnar.BatchCodec, so
// they plug into AddSource and AddSink directly, and their columns follow the
// record schema instead of JSON text: nested records become Struct, arrays
// and repeated fields List, maps Map, and decimals and timestamps their
// native Arrow types.
//
// A few shapes have no finite or faithful Arrow form and fall back, tagged
// with field metadata so the write-back path can reverse them: a multi-branch
// Avro union becomes a struct with one nullable child per branch
// (krabka.avro.union), a recursive Avro record becomes its JSON text
// (krabka.json), and a recursive Protobuf message — or the dynamic
// google.protobuf.Struct, Value, and ListValue — becomes its canonical
// Protobuf JSON text (krabka.json plus krabka.proto.message naming the type).
package columnarschema

// Field metadata keys that tag fallback and provenance information on bridge
// columns.
const (
	// MetadataJSON marks a Utf8 column that holds serialized JSON text.
	MetadataJSON = "krabka.json"

	// MetadataAvroEnum names the Avro enum type of a Utf8 symbol column.
	MetadataAvroEnum = "krabka.avro.enum"

	// MetadataAvroEnumSymbols lists the Avro enum symbols, comma separated.
	MetadataAvroEnumSymbols = "krabka.avro.enum.symbols"

	// MetadataAvroUnion marks a struct column that spreads a multi-branch
	// Avro union across one nullable child per branch.
	MetadataAvroUnion = "krabka.avro.union"

	// MetadataAvroFixed names the Avro fixed type of a fixed-size binary
	// column.
	MetadataAvroFixed = "krabka.avro.fixed"

	// MetadataAvroLogical names an Avro logical type carried by the column.
	MetadataAvroLogical = "krabka.avro.logical"

	// MetadataProtoEnum names the Protobuf enum type of a Utf8 symbol column.
	MetadataProtoEnum = "krabka.proto.enum"

	// MetadataProtoOneof names the Protobuf oneof a column belongs to.
	MetadataProtoOneof = "krabka.proto.oneof"

	// MetadataProtoWrapper names the well-known wrapper type a column
	// unwraps.
	MetadataProtoWrapper = "krabka.proto.wrapper"

	// MetadataProtoMessage names the Protobuf message type of a JSON text
	// column.
	MetadataProtoMessage = "krabka.proto.message"
)
