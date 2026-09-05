package hatSql

import (
	"container/list"
	"fmt"
	"testing"
)

func BenchmarkSQLPreparedQueryCacheHit(b *testing.B) {
	cache := NewSQLPreparedQueryCache(256)
	sources := make([]string, 256)
	for index := range sources {
		sources[index] = fmt.Sprintf("SELECT value FROM CACHE('metrics-%d') WHERE id >= $1", index)
		if _, err := cache.template(sources[index]); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := cache.template(sources[index%len(sources)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSQLPreparedQueryCacheExactLookupBaseline measures the old exact-key
// mutex/map/LRU path without token normalization. It is a local control for
// the lookup cost, not a second implementation used in production.
func BenchmarkSQLPreparedQueryCacheExactLookupBaseline(b *testing.B) {
	cache := NewSQLPreparedQueryCache(256)
	sources := make([]string, 256)
	for index := range sources {
		sources[index] = fmt.Sprintf("SELECT value FROM CACHE('metrics-%d') WHERE id >= $1", index)
		query, err := parseSQLQueryTemplate(sources[index])
		if err != nil {
			b.Fatal(err)
		}
		if cache.order == nil {
			cache.order = list.New()
		}
		cache.entries[sources[index]] = sqlPreparedQueryCacheEntry{query: query, order: cache.order.PushBack(sources[index])}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		cache.mu.Lock()
		entry, ok := cache.entries[sources[index%len(sources)]]
		if !ok {
			cache.mu.Unlock()
			b.Fatal("baseline cache entry missing")
		}
		cache.order.MoveToBack(entry.order)
		cache.mu.Unlock()
	}
}

func BenchmarkSQLPreparedQueryCacheNormalizedAliasHit(b *testing.B) {
	cache := NewSQLPreparedQueryCache(2)
	first := "SELECT value FROM CACHE('users') WHERE id = $1"
	alias := "\n select  value\n from cache('users') where id=$1\n"
	if _, err := cache.template(first); err != nil {
		b.Fatal(err)
	}
	if _, err := cache.template(alias); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := cache.template(alias); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLPreparedQueryCacheVersionedHit(b *testing.B) {
	cache := NewSQLPreparedQueryCache(2)
	source := "SELECT value FROM CACHE('users') WHERE id = $1"
	if _, err := cache.templateWithSchemaVersion(source, "schema-1"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := cache.templateWithSchemaVersion(source, "schema-1"); err != nil {
			b.Fatal(err)
		}
	}
}
