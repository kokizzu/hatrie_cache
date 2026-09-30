package hatSql

import (
	"sort"
	"testing"
)

func TestRound14RadixInt64OrderStableSignedValues(t *testing.T) {
	values := []int64{0, -1, 7, -1, -9, 7, 1}
	ascending := []uint32{0, 1, 2, 3, 4, 5, 6}
	sortTypedTableColumnarInt64Order(ascending, values, false)
	if want, got := []uint32{4, 1, 3, 0, 6, 2, 5}, ascending; !equalUint32Slices(got, want) {
		t.Fatalf("ascending order = %v, want %v", got, want)
	}

	descending := []uint32{0, 1, 2, 3, 4, 5, 6}
	sortTypedTableColumnarInt64Order(descending, values, true)
	if want, got := []uint32{2, 5, 6, 0, 1, 3, 4}, descending; !equalUint32Slices(got, want) {
		t.Fatalf("descending order = %v, want %v", got, want)
	}
}

func TestRound14ColumnarInt64OrderMatchesStableGenericSemantics(t *testing.T) {
	values := []interface{}{int64(3), int64(-2), int64(3), int64(0)}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"score": values}, Rows: len(values)}
	order, _, ok := typedTableColumnarOrder(batch, "score")
	if !ok {
		t.Fatal("typedTableColumnarOrder returned unavailable")
	}
	if want, got := []uint32{1, 3, 0, 2}, order; !equalUint32Slices(got, want) {
		t.Fatalf("columnar order = %v, want %v", got, want)
	}
}

func TestRound14ColumnarInt64OrderUsesStableRadixForLargeInputs(t *testing.T) {
	const rows = 512
	values := make([]interface{}, rows)
	want := make([]uint32, rows)
	for row := range values {
		values[row] = int64((row*17)%23 - 11)
		want[row] = uint32(row)
	}
	sort.SliceStable(want, func(left, right int) bool {
		return values[want[left]].(int64) < values[want[right]].(int64)
	})
	batch := ColumnarBatch{Columns: map[string][]interface{}{"score": values}, Rows: rows}
	got, _, ok := typedTableColumnarOrder(batch, "score")
	if !ok {
		t.Fatal("typedTableColumnarOrder returned unavailable")
	}
	if !equalUint32Slices(got, want) {
		t.Fatalf("large columnar order = %v, want %v", got, want)
	}
}

func TestRound14ColumnarInt64OrderFallsBackForSmallAndMixedInputs(t *testing.T) {
	small := ColumnarBatch{Columns: map[string][]interface{}{"score": {int64(2), int64(1)}}, Rows: 2}
	if _, _, ok := typedTableColumnarInt64Order(small, "score"); ok {
		t.Fatal("small input unexpectedly selected radix order")
	}
	mixed := ColumnarBatch{Columns: map[string][]interface{}{"score": {int64(2), int(1)}}, Rows: 256}
	if _, _, ok := typedTableColumnarInt64Order(mixed, "score"); ok {
		t.Fatal("mixed input unexpectedly selected radix order")
	}
}

func equalUint32Slices(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var round14ColumnarOrderSink uint32

func BenchmarkRound14TypedTableColumnarInt64Order(b *testing.B) {
	const rows = 100_000
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = int64((row*7919)%rows) - rows/2
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"score": values}, Rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		order, _, ok := typedTableColumnarOrder(batch, "score")
		if !ok {
			b.Fatal("typedTableColumnarOrder returned unavailable")
		}
		round14ColumnarOrderSink = order[len(order)-1]
	}
}
