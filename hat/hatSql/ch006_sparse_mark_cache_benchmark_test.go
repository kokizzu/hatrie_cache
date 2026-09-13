package hatSql

import (
	"context"
	"testing"
)

var ch006SparseMarkQueryResult SQLQueryResult
var ch006SparseMarkSourceBatch ColumnarBatch

func BenchmarkCH006SparsePrimaryMarkCache(b *testing.B) {
	query := "FROM CACHE('events') SELECT id WHERE id = 32768"
	for _, benchmark := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			table := newCH006SparseMarkTable(b, benchmark.enabled)
			ctx := context.Background()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				result, err := ExecuteQueryParameters(ctx, query, table, nil, QueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				ch006SparseMarkQueryResult = result
			}
		})
	}
}

func BenchmarkCH006SparsePrimaryMarkSource(b *testing.B) {
	fields := []string{"id"}
	for _, benchmark := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			table := newCH006SparseMarkTable(b, benchmark.enabled)
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if benchmark.enabled {
					batch, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
					if err != nil || !available {
						b.Fatalf("BorrowSQLColumnarSourceSegments() = available %t, error %v", available, err)
					}
					ch006SparseMarkSourceBatch = batch
					continue
				}
				batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields)
				if err != nil || !available {
					b.Fatalf("ResolveSQLColumnarSource() = available %t, error %v", available, err)
				}
				ch006SparseMarkSourceBatch = batch
			}
		})
	}
}

func newCH006SparseMarkTable(b *testing.B, sparseMarkCache bool) *TypedTable {
	b.Helper()
	const rowCount = 65536
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:                   true,
			MaxBytes:                  1,
			MinReads:                  1,
			RowsPerSegment:            256,
			SparsePrimaryIndex:        true,
			SparsePrimaryField:        "id",
			SparsePrimaryMarkCache:    sparseMarkCache,
			SparsePrimaryMarkMaxBytes: 1 << 20,
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
