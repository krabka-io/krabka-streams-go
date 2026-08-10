package columnar

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// checkedAllocator fails the test if any Arrow memory is still allocated at
// cleanup, the Go analog of closing a Java RootAllocator.
func checkedAllocator(t *testing.T) *memory.CheckedAllocator {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	return mem
}

// transactions builds a payload batch with a Utf8 "user" and Int64 "amount"
// column. The caller releases it.
func transactions(mem memory.Allocator, users []string, amounts []int64) arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "user", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	for row := range users {
		builder.Field(0).(*array.StringBuilder).Append(users[row])
		builder.Field(1).(*array.Int64Builder).Append(amounts[row])
	}
	return builder.NewRecordBatch()
}

// annotate attaches metadata columns for tests, one entry per row.
func annotate(t *testing.T, payload arrow.Record, mem memory.Allocator, metadata ...rowMetadata) arrow.Record {
	t.Helper()
	batch, err := withMetadata(payload, metadata, mem)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func int64Column(t *testing.T, batch arrow.Record, name string) *array.Int64 {
	t.Helper()
	column, ok := columnByName(batch, name).(*array.Int64)
	if !ok {
		t.Fatalf("column %s is not Int64", name)
	}
	return column
}

func stringColumn(t *testing.T, batch arrow.Record, name string) *array.String {
	t.Helper()
	column, ok := columnByName(batch, name).(*array.String)
	if !ok {
		t.Fatalf("column %s is not String", name)
	}
	return column
}

func columnNames(batch arrow.Record) []string {
	names := make([]string, batch.Schema().NumFields())
	for i := range names {
		names[i] = batch.Schema().Field(i).Name
	}
	return names
}

func runOp(t *testing.T, operator *BuiltinOp, batch arrow.Record) arrow.Record {
	t.Helper()
	ctx := &Context{}
	if err := operator.Process(ctx, batch); err != nil {
		t.Fatal(err)
	}
	outputs := ctx.drain()
	if len(outputs) != 1 {
		t.Fatalf("expected one forwarded batch, got %d", len(outputs))
	}
	return outputs[0]
}
