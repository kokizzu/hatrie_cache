package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHU40ResultCacheDependencyModes(b *testing.B) {
	dependency := []ResultCacheDependency{{Kind: "CACHE", Key: "events"}}
	version := func() (string, bool) { return "v1", true }
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{Rows: []Row{{"id": int64(1)}}}, nil
	}

	b.Run("default-versioned", func(b *testing.B) {
		cache := NewResultCache(1)
		if _, err := cache.ExecuteVersioned(context.Background(), "events-query", version, execute); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := cache.ExecuteVersioned(context.Background(), "events-query", version, execute); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("dependency-versioned", func(b *testing.B) {
		cache := NewResultCacheWithDependencies(1)
		if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", version, dependency, execute); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := cache.ExecuteVersionedWithDependencies(context.Background(), "events-query", version, dependency, execute); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("explicit-invalidation", func(b *testing.B) {
		cache := NewResultCacheWithDependencies(1)
		if _, err := cache.ExecuteWithDependencies(context.Background(), "events-query", dependency, execute); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := cache.ExecuteWithDependencies(context.Background(), "events-query", dependency, execute); err != nil {
				b.Fatal(err)
			}
		}
	})
}
