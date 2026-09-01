package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkTypedTableAdaptiveSegments(b *testing.B) {
	const query = "FROM CACHE('events') SELECT id WHERE score >= 0 ORDER BY score ASC LIMIT 50"
	for _, benchmark := range []struct {
		name     string
		adaptive bool
	}{
		{name: "fixed_256"},
		{name: "adaptive_max_256", adaptive: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			table := newTypedTableAdaptiveSegmentsBenchmarkTable(b, benchmark.adaptive)
			for range 2 {
				result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
				if err != nil || len(result.Rows) != 50 {
					b.Fatalf("warm query = %#v, %v", result, err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
				if err != nil || len(result.Rows) != 50 {
					b.Fatalf("query = %#v, %v", result, err)
				}
			}
		})
	}
}

func newTypedTableAdaptiveSegmentsBenchmarkTable(b *testing.B, adaptive bool) *hatSql.TypedTable {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "id", Kind: hatSql.TypedTableInt64},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:          true,
			MaxBytes:         1 << 20,
			MinReads:         2,
			RowsPerSegment:   256,
			AdaptiveSegments: adaptive,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		if _, err := table.Upsert(fmt.Sprintf("event-%d", index), []hatSql.TypedTableValue{hatSql.TypedInt64(int64(index)), hatSql.TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
