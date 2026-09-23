package hatSql

import (
	"strconv"
	"testing"
)

var benchmarkT217Rows int

func newT217Table(b testing.TB) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "region", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
			{Name: "active", Kind: TypedTableBool},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return table
}

func newT217Batch(rows int) TypedTableColumnarBatch {
	keys := make([]string, rows)
	regions := make([]TypedTableValue, rows)
	scores := make([]TypedTableValue, rows)
	active := make([]TypedTableValue, rows)
	for row := 0; row < rows; row++ {
		keys[row] = "event-" + strconv.Itoa(row)
		regions[row] = TypedString("region-" + strconv.Itoa(row%16))
		scores[row] = TypedInt64(int64(row % 1000))
		active[row] = TypedBool(row%2 == 0)
	}
	return TypedTableColumnarBatch{
		Keys: keys,
		Columns: [][]TypedTableValue{
			regions,
			scores,
			active,
		},
	}
}

func BenchmarkT217TypedTableRowUpsertBaseline(b *testing.B) {
	const rows = 2048
	input := newT217Batch(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table := newT217Table(b)
		for row, key := range input.Keys {
			_, err := table.Upsert(key, []TypedTableValue{
				input.Columns[0][row],
				input.Columns[1][row],
				input.Columns[2][row],
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkT217Rows = table.Stats().RowCount
	}
}

func BenchmarkT217TypedTableAppendColumnarBatch(b *testing.B) {
	const rows = 2048
	input := newT217Batch(rows)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table := newT217Table(b)
		inserted, err := table.AppendColumnarBatch(input)
		if err != nil {
			b.Fatal(err)
		}
		if inserted != rows {
			b.Fatalf("inserted rows = %d, want %d", inserted, rows)
		}
		benchmarkT217Rows = table.Stats().RowCount
	}
}
