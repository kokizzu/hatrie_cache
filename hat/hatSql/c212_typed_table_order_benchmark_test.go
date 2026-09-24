package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const c212TypedTableOrderRows = 20_000

var c212TypedTableOrderBenchmarkSink hatSql.SQLQueryResult

// BenchmarkC212TypedTableOrderColumnarTopNBaseline measures the current
// columnar top-N scan before an admitted ordinal order projection is used.
func BenchmarkC212TypedTableOrderColumnarTopNBaseline(b *testing.B) {
	table := newC212TypedTableOrderBenchmarkTable(b)
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC LIMIT 50"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
		}
		c212TypedTableOrderBenchmarkSink = result
	}
}

func BenchmarkC212TypedTableOrderCachedProjection(b *testing.B) {
	table := newC212TypedTableOrderBenchmarkTableWithOrderCache(b)
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC LIMIT 50"
	for warmup := 0; warmup < 8; warmup++ {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("warm-up ExecuteQueryParameters() = %#v, %v", result, err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
		}
		c212TypedTableOrderBenchmarkSink = result
	}
}

func newC212TypedTableOrderBenchmarkTable(b testing.TB) *hatSql.TypedTable {
	return newC212TypedTableOrderBenchmarkTableWithOrderCacheOption(b, false)
}

func newC212TypedTableOrderBenchmarkTableWithOrderCache(b testing.TB) *hatSql.TypedTable {
	return newC212TypedTableOrderBenchmarkTableWithOrderCacheOption(b, true)
}

func newC212TypedTableOrderBenchmarkTableWithOrderCacheOption(b testing.TB, sortedOrderCache bool) *hatSql.TypedTable {
	b.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "id", Kind: hatSql.TypedTableInt64},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:          true,
			SortedOrderCache: sortedOrderCache,
			MaxBytes:         16 << 20,
			MinReads:         1,
			RowsPerSegment:   256,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < c212TypedTableOrderRows; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []hatSql.TypedTableValue{
			hatSql.TypedInt64(int64(index)),
			hatSql.TypedInt64(int64((index * 7919) % c212TypedTableOrderRows)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	fields := []string{"id", "score"}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		b.Fatalf("ResolveSQLColumnarSource() available = %t, error = %v", available, err)
	}
	return table
}

func BenchmarkC212TypedTableCompositeOrderBaseline(b *testing.B) {
	table := newC212TypedTableCompositeOrderBenchmarkTable(b, false)
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC, item.id DESC LIMIT 50"
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
		}
		c212TypedTableOrderBenchmarkSink = result
	}
}

func BenchmarkC212TypedTableCompositeOrderCached(b *testing.B) {
	table := newC212TypedTableCompositeOrderBenchmarkTable(b, true)
	query := "FROM CACHE('events') AS item SELECT item.id, item.score ORDER BY item.score ASC, item.id DESC LIMIT 50"
	for warmup := 0; warmup < 8; warmup++ {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("warm-up ExecuteQueryParameters() = %#v, %v", result, err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), query, table, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 50 {
			b.Fatalf("ExecuteQueryParameters() = %#v, %v", result, err)
		}
		c212TypedTableOrderBenchmarkSink = result
	}
}

func newC212TypedTableCompositeOrderBenchmarkTable(b testing.TB, sortedOrderCache bool) *hatSql.TypedTable {
	b.Helper()
	const rows = 20_000
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "id", Kind: hatSql.TypedTableInt64},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:          true,
			SortedOrderCache: sortedOrderCache,
			MaxBytes:         16 << 20,
			MinReads:         1,
			RowsPerSegment:   256,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < rows; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []hatSql.TypedTableValue{
			hatSql.TypedInt64(int64(index)),
			hatSql.TypedInt64(int64((index * 7919) % rows)),
		}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
