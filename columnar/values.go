package columnar

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Value reads one value from an Arrow array as a native Go value.
//
// Null values are nil; strings, byte slices, sized integers, floats, and
// booleans map to their Go counterparts; decimals become *big.Rat; lists
// become []any; structs and maps become map[string]any; unions yield the
// active member's value. Dates, times, and timestamps are returned as their
// raw integer representation.
func Value(arr arrow.Array, row int) any {
	return arrowValue(arr, row)
}

// AppendValue appends one Go value to an Arrow builder, coercing it to the
// declared Arrow type; nil appends a null.
//
// String columns accept anything by formatting it; binary columns accept
// bytes and strings; integer columns accept integral values checked for
// overflow; unsigned columns additionally reject negatives; date and
// timestamp columns accept integers or time.Time; time-of-day columns accept
// integers or time.Duration; decimal columns accept *big.Rat, *big.Int,
// numbers, and numeric text; list, struct, and map columns accept slices and
// map[string]any; union columns pick the first member that accepts the
// value.
func AppendValue(builder array.Builder, value any) error {
	return appendGoValue(builder, value)
}
