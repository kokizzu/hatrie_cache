package hatSql

import (
	"sort"
	"testing"
)

func TestRound15RadixCompositeOrderStableSignedValues(t *testing.T) {
	values := []int64{
		10, 10, 9,
		2, 1, 1,
	}
	order := []uint32{0, 1, 2}
	sortTypedTableColumnarInt64OrderFields(order, values, 3, []bool{true, false})
	if want, got := []uint32{1, 0, 2}, order; !equalUint32Slices(got, want) {
		t.Fatalf("composite order = %v, want %v", got, want)
	}
}

func TestRound15ColumnarCompositeInt64OrderMatchesStableGenericSemantics(t *testing.T) {
	const rows = 512
	score := make([]interface{}, rows)
	id := make([]interface{}, rows)
	want := make([]uint32, rows)
	for row := range score {
		score[row] = int64((row * 11) % 17)
		id[row] = int64((row * 7) % 19)
		want[row] = uint32(row)
	}
	sort.SliceStable(want, func(left, right int) bool {
		leftRow, rightRow := int(want[left]), int(want[right])
		leftScore, rightScore := score[leftRow].(int64), score[rightRow].(int64)
		if leftScore != rightScore {
			return leftScore > rightScore
		}
		return id[leftRow].(int64) < id[rightRow].(int64)
	})
	batch := ColumnarBatch{Columns: map[string][]interface{}{"score": score, "id": id}, Rows: rows}
	got, _, ok := typedTableColumnarOrderFields(batch, []string{"score", "id"}, []bool{true, false})
	if !ok {
		t.Fatal("typedTableColumnarOrderFields returned unavailable")
	}
	if !equalUint32Slices(got, want) {
		t.Fatalf("large composite order = %v, want %v", got, want)
	}
}

func TestRound15ColumnarCompositeInt64OrderFallsBackForMixedInputs(t *testing.T) {
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(2), int(1)},
			"id":    {int64(1), int64(2)},
		},
		Rows: 256,
	}
	if _, _, ok := typedTableColumnarInt64OrderFields(batch, []string{"score", "id"}, nil); ok {
		t.Fatal("mixed input unexpectedly selected radix composite order")
	}
}

var round15ColumnarOrderSink uint32

func BenchmarkRound15TypedTableColumnarCompositeInt64Order(b *testing.B) {
	const rows = 100_000
	score := make([]interface{}, rows)
	id := make([]interface{}, rows)
	for row := range score {
		score[row] = int64((row * 7919) % rows)
		id[row] = int64((row * 6151) % rows)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"score": score, "id": id}, Rows: rows}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		order, _, ok := typedTableColumnarOrderFields(batch, []string{"score", "id"}, []bool{true, false})
		if !ok {
			b.Fatal("typedTableColumnarOrderFields returned unavailable")
		}
		round15ColumnarOrderSink = order[len(order)-1]
	}
}
