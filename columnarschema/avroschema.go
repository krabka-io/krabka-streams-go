package columnarschema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hamba/avro/v2"
)

// AvroArrowSchema translates an Avro schema into the Arrow schema its bridge
// produces, without touching data. A record schema spreads its fields across
// columns; any other schema becomes a single column named "value".
func AvroArrowSchema(schema avro.Schema) (*arrow.Schema, error) {
	fields, err := avroTopLevelFields(schema)
	if err != nil {
		return nil, err
	}
	return arrow.NewSchema(fields, nil), nil
}

// AvroArrowField translates one named Avro schema into an Arrow field.
func AvroArrowField(name string, schema avro.Schema) (arrow.Field, error) {
	return avroField(name, schema, false, nil)
}

func avroTopLevelFields(schema avro.Schema) ([]arrow.Field, error) {
	if record, ok := schema.(*avro.RecordSchema); ok {
		fields := make([]arrow.Field, 0, len(record.Fields()))
		for _, field := range record.Fields() {
			converted, err := avroField(field.Name(), field.Type(), false, []string{record.FullName()})
			if err != nil {
				return nil, err
			}
			fields = append(fields, converted)
		}
		return fields, nil
	}
	field, err := avroField("value", schema, false, nil)
	if err != nil {
		return nil, err
	}
	return []arrow.Field{field}, nil
}

func avroField(name string, schema avro.Schema, nullable bool, visiting []string) (arrow.Field, error) {
	if ref, ok := schema.(*avro.RefSchema); ok {
		target := ref.Schema()
		if record, ok := target.(*avro.RecordSchema); ok && contains(visiting, record.FullName()) {
			return taggedField(name, arrow.BinaryTypes.String, nullable,
				map[string]string{MetadataJSON: "true"}), nil
		}
		return avroField(name, target, nullable, visiting)
	}
	if union, ok := schema.(*avro.UnionSchema); ok {
		var branches []avro.Schema
		hasNull := false
		for _, branch := range union.Types() {
			if branch.Type() == avro.Null {
				hasNull = true
			} else {
				branches = append(branches, branch)
			}
		}
		if len(branches) == 0 {
			return arrow.Field{}, fmt.Errorf("Avro field has only null branches: %s", name)
		}
		if len(branches) == 1 {
			return avroField(name, branches[0], nullable || hasNull, visiting)
		}
		children := make([]arrow.Field, 0, len(branches))
		names := map[string]bool{}
		for _, branch := range branches {
			child, err := avroField(avroBranchName(branch), branch, true, visiting)
			if err != nil {
				return arrow.Field{}, err
			}
			if names[child.Name] {
				return arrow.Field{}, fmt.Errorf("Avro union branch names collide in field %s", name)
			}
			names[child.Name] = true
			children = append(children, child)
		}
		return arrow.Field{
			Name:     name,
			Type:     arrow.StructOf(children...),
			Nullable: nullable || hasNull,
			Metadata: buildMetadata(map[string]string{MetadataAvroUnion: "true"}),
		}, nil
	}
	switch typed := schema.(type) {
	case *avro.PrimitiveSchema:
		return avroPrimitiveField(name, typed, nullable)
	case *avro.FixedSchema:
		if decimal, ok := typed.Logical().(*avro.DecimalLogicalSchema); ok {
			decimalType, err := avroDecimalType(name, decimal)
			if err != nil {
				return arrow.Field{}, err
			}
			return arrow.Field{Name: name, Type: decimalType, Nullable: nullable}, nil
		}
		return taggedField(name, &arrow.FixedSizeBinaryType{ByteWidth: typed.Size()}, nullable,
			map[string]string{MetadataAvroFixed: typed.FullName()}), nil
	case *avro.EnumSchema:
		return taggedField(name, arrow.BinaryTypes.String, nullable, map[string]string{
			MetadataAvroEnum:        typed.FullName(),
			MetadataAvroEnumSymbols: strings.Join(typed.Symbols(), ","),
		}), nil
	case *avro.RecordSchema:
		if contains(visiting, typed.FullName()) {
			return taggedField(name, arrow.BinaryTypes.String, nullable,
				map[string]string{MetadataJSON: "true"}), nil
		}
		visiting = append(visiting, typed.FullName())
		children := make([]arrow.Field, 0, len(typed.Fields()))
		for _, field := range typed.Fields() {
			child, err := avroField(field.Name(), field.Type(), false, visiting)
			if err != nil {
				return arrow.Field{}, err
			}
			children = append(children, child)
		}
		return arrow.Field{Name: name, Type: arrow.StructOf(children...), Nullable: nullable}, nil
	case *avro.ArraySchema:
		item, err := avroField("item", typed.Items(), false, visiting)
		if err != nil {
			return arrow.Field{}, err
		}
		return arrow.Field{Name: name, Type: arrow.ListOfField(item), Nullable: nullable}, nil
	case *avro.MapSchema:
		value, err := avroField("value", typed.Values(), false, visiting)
		if err != nil {
			return arrow.Field{}, err
		}
		return arrow.Field{
			Name:     name,
			Type:     arrow.MapOf(arrow.BinaryTypes.String, value.Type),
			Nullable: nullable,
		}, nil
	default:
		return arrow.Field{}, fmt.Errorf("unsupported Avro schema %s in field %s", schema.Type(), name)
	}
}

func avroPrimitiveField(name string, schema *avro.PrimitiveSchema, nullable bool) (arrow.Field, error) {
	logical := schema.Logical()
	logicalType := avro.LogicalType("")
	if logical != nil {
		logicalType = logical.Type()
	}
	switch schema.Type() {
	case avro.Null:
		return arrow.Field{}, fmt.Errorf("Avro field is the null type: %s", name)
	case avro.Boolean:
		return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: nullable}, nil
	case avro.Int:
		switch logicalType {
		case avro.Date:
			return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Date32, Nullable: nullable}, nil
		case avro.TimeMillis:
			return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Time32ms, Nullable: nullable}, nil
		}
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int32, Nullable: nullable}, nil
	case avro.Long:
		switch logicalType {
		case avro.TimeMicros:
			return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Time64us, Nullable: nullable}, nil
		case avro.TimestampMillis:
			return arrow.Field{Name: name,
				Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, Nullable: nullable}, nil
		case avro.TimestampMicros:
			return arrow.Field{Name: name,
				Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: nullable}, nil
		case avro.LocalTimestampMillis:
			return arrow.Field{Name: name,
				Type: &arrow.TimestampType{Unit: arrow.Millisecond}, Nullable: nullable}, nil
		case avro.LocalTimestampMicros:
			return arrow.Field{Name: name,
				Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: nullable}, nil
		}
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: nullable}, nil
	case avro.Float:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float32, Nullable: nullable}, nil
	case avro.Double:
		return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: nullable}, nil
	case avro.Bytes:
		if decimal, ok := logical.(*avro.DecimalLogicalSchema); ok {
			decimalType, err := avroDecimalType(name, decimal)
			if err != nil {
				return arrow.Field{}, err
			}
			return arrow.Field{Name: name, Type: decimalType, Nullable: nullable}, nil
		}
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.Binary, Nullable: nullable}, nil
	case avro.String:
		if logicalType == avro.UUID {
			return taggedField(name, arrow.BinaryTypes.String, nullable,
				map[string]string{MetadataAvroLogical: "uuid"}), nil
		}
		return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: nullable}, nil
	default:
		return arrow.Field{}, fmt.Errorf("unsupported Avro primitive %s in field %s", schema.Type(), name)
	}
}

func avroDecimalType(name string, decimal *avro.DecimalLogicalSchema) (arrow.DataType, error) {
	if decimal.Precision() > 76 {
		return nil, fmt.Errorf(
			"Avro decimal precision above 76 is unsupported in field %s: %d", name, decimal.Precision())
	}
	if decimal.Precision() > 38 {
		return &arrow.Decimal256Type{Precision: int32(decimal.Precision()), Scale: int32(decimal.Scale())}, nil
	}
	return &arrow.Decimal128Type{Precision: int32(decimal.Precision()), Scale: int32(decimal.Scale())}, nil
}

func avroBranchName(branch avro.Schema) string {
	if ref, ok := branch.(*avro.RefSchema); ok {
		branch = ref.Schema()
	}
	if named, ok := branch.(avro.NamedSchema); ok {
		return named.FullName()
	}
	return string(branch.Type())
}

// avroUnionBranches maps branch column names to branch schemas, null
// excluded, in declaration order.
func avroUnionBranches(union *avro.UnionSchema) ([]string, map[string]avro.Schema) {
	var names []string
	branches := map[string]avro.Schema{}
	for _, branch := range union.Types() {
		if branch.Type() == avro.Null {
			continue
		}
		name := avroBranchName(branch)
		names = append(names, name)
		branches[name] = branch
	}
	return names, branches
}

func taggedField(name string, dataType arrow.DataType, nullable bool, metadata map[string]string) arrow.Field {
	return arrow.Field{Name: name, Type: dataType, Nullable: nullable, Metadata: buildMetadata(metadata)}
}

func buildMetadata(pairs map[string]string) arrow.Metadata {
	keys := make([]string, 0, len(pairs))
	for key := range pairs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = pairs[key]
	}
	return arrow.NewMetadata(keys, values)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
