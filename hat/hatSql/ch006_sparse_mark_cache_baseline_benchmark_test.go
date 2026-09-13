package hatSql

import (
	"context"
	"strconv"
	"testing"
)

var ch006BaselineQueryResult SQLQueryResult
var ch006BaselineSourceBatch ColumnarBatch

func BenchmarkCH006BaselineSparsePrimaryMarkQuery(b *testing.B) {
	table := newCH006BaselineSparseMarkTable(b)
	query := "FROM CACHE('events') SELECT id WHERE id = 32768"
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteQueryParameters(context.Background(), query, table, nil, QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch006BaselineQueryResult = result
	}
}

func BenchmarkCH006BaselineSparsePrimaryMarkSource(b *testing.B) {
	table := newCH006BaselineSparseMarkTable(b)
	fields := []string{"id"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields)
		if err != nil || !available {
			b.Fatalf("ResolveSQLColumnarSource() = available %t, error %v", available, err)
		}
		ch006BaselineSourceBatch = batch
	}
}

func newCH006BaselineSparseMarkTable(b *testing.B) *TypedTable {
	b.Helper()
	const rowCount = 65536
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			MaxBytes:           1,
			MinReads:           1,
			RowsPerSegment:     256,
			SparsePrimaryIndex: true,
			SparsePrimaryField: "id",
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < rowCount; index++ {
		if _, err := table.Upsert(benchmarkCH006Key(index), []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"id"}); err != nil {
		b.Fatal(err)
	}
	return table
}

func benchmarkCH006Key(index int) string {
	return "key-" + strconv.Itoa(index)
}
