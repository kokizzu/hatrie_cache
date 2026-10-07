package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHU40ResultCacheEpochHit(b *testing.B) {
	cache := NewResultCache(1)
	epoch := uint64(1)
	result := SQLQueryResultForCHU40Benchmark()
	if _, err := cache.Execute(context.Background(), "epoch", func() uint64 { return epoch }, func(context.Context) (QueryResult, error) {
		return result, nil
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cache.Execute(context.Background(), "epoch", func() uint64 { return epoch }, func(context.Context) (QueryResult, error) {
			return result, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU40ResultCacheDependencyHit(b *testing.B) {
	cache := NewResultCacheWithDependencies(1)
	dependencies := []ResultCacheDependency{{Kind: "table", Key: "orders"}}
	result := SQLQueryResultForCHU40Benchmark()
	if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", dependencies, func(context.Context) (QueryResult, error) {
		return result, nil
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cache.ExecuteWithDependencies(context.Background(), "orders-total", dependencies, func(context.Context) (QueryResult, error) {
			return result, nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func SQLQueryResultForCHU40Benchmark() QueryResult {
	return QueryResult{
		Columns: []string{"id", "value"},
		Rows:    []Row{{"id": int64(1), "value": "cached"}},
	}
}
