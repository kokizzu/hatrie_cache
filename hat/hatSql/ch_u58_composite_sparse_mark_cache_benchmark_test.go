package hatSql_test

import (
	"context"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var chU58CompositeSparseMarkQuerySink hatSql.SQLQueryResult

func BenchmarkCHU58CompositeSparseMarkQueryBaseline(b *testing.B) {
	benchmarkCHU58CompositeSparseMarkQuery(b, false)
}

func BenchmarkCHU58CompositeSparseMarkQuery(b *testing.B) {
	benchmarkCHU58CompositeSparseMarkQuery(b, true)
}

func benchmarkCHU58CompositeSparseMarkQuery(b *testing.B, composite bool) {
	table := newCHU58CompositeSparseMarkBenchmarkTable(b, composite)
	query := "FROM CACHE('events') AS e WHERE e.tenant = 15 AND e.id >= 900 SELECT e.tenant, e.id"
	if _, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		chU58CompositeSparseMarkQuerySink = result
	}
}

func newCHU58CompositeSparseMarkBenchmarkTable(b testing.TB, composite bool) *hatSql.TypedTable {
	b.Helper()
	const (
		tenantCount   = 16
		rowsPerTenant = 1024
	)
	columnarCache := hatSql.TypedTableColumnarCacheOptions{
		Enabled:                   true,
		MaxBytes:                  1,
		MinReads:                  1,
		RowsPerSegment:            64,
		SparsePrimaryIndex:        true,
		SparsePrimaryMarkCache:    true,
		SparsePrimaryMarkMaxBytes: 1 << 20,
	}
	if composite {
		columnarCache.SparsePrimaryFields = []string{"tenant", "id"}
	} else {
		columnarCache.SparsePrimaryField = "tenant"
	}
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "tenant", Kind: hatSql.TypedTableInt64},
			{Name: "id", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: columnarCache,
	})
	if err != nil {
		b.Fatal(err)
	}
	for tenant := int64(0); tenant < tenantCount; tenant++ {
		for id := int64(0); id < rowsPerTenant; id++ {
			key := fmt.Sprintf("event-%d-%d", tenant, id)
			if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedInt64(tenant), hatSql.TypedInt64(id)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	return table
}
