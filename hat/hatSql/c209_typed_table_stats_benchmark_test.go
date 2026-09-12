package hatSql

import (
	"strconv"
	"testing"
)

var c209TypedTableStatsSink TypedTableStats
var c209TypedTableBatchSink ColumnarBatch

func BenchmarkTypedTableStats(b *testing.B) {
	table := newC209TypedTableStatsBenchmarkTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		c209TypedTableStatsSink = table.Stats()
	}
}

func BenchmarkTypedTableColumnarSource(b *testing.B) {
	table := newC209TypedTableStatsBenchmarkTable(b)
	fields := []string{"team", "score", "ratio", "active"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch, found, err := table.ResolveSQLColumnarSource("CACHE", "events", fields)
		if err != nil || !found {
			b.Fatalf("ResolveSQLColumnarSource() found = %t, error = %v", found, err)
		}
		c209TypedTableBatchSink = batch
	}
}

func newC209TypedTableStatsBenchmarkTable(b *testing.B) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString, DictionaryEncoded: true},
			{Name: "score", Kind: TypedTableInt64},
			{Name: "ratio", Kind: TypedTableFloat64},
			{Name: "active", Kind: TypedTableBool},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 10000; index++ {
		team := "team-" + strconv.Itoa(index%32)
		if _, err := table.Upsert(strconv.Itoa(index), []TypedTableValue{
			TypedString(team),
			TypedInt64(int64(index)),
			TypedFloat64(float64(index) / 10),
			TypedBool(index%2 == 0),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
