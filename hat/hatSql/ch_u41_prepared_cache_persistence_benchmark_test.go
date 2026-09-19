package hatSql

import (
	"fmt"
	"testing"
)

func chu41SeedPreparedCache(b *testing.B) (*SQLPreparedQueryCache, string) {
	b.Helper()
	cache := NewSQLPreparedQueryCache(256)
	for index := 0; index < 256; index++ {
		source := fmt.Sprintf("SELECT value FROM CACHE('metrics-%d') WHERE id >= $1", index)
		if _, err := PrepareSQLQueryWithSchemaVersion(source, []ParameterSpec{{Type: ParameterInteger}}, "schema-1", cache); err != nil {
			b.Fatal(err)
		}
	}
	return cache, b.TempDir() + "/prepared.cache"
}

func BenchmarkCHU41PreparedCacheHitNoPersistence(b *testing.B) {
	cache, _ := chu41SeedPreparedCache(b)
	source := "SELECT value FROM CACHE('metrics-127') WHERE id >= $1"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := cache.templateWithSchemaVersion(source, "schema-1"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU41PreparedCacheSave(b *testing.B) {
	cache, path := chu41SeedPreparedCache(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU41PreparedCacheLoad(b *testing.B) {
	cache, path := chu41SeedPreparedCache(b)
	if err := cache.Save(path, SQLPreparedQueryCachePersistenceOptions{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		restored := NewSQLPreparedQueryCache(256)
		if _, err := restored.Load(path, SQLPreparedQueryCachePersistenceOptions{SchemaVersion: "schema-1"}); err != nil {
			b.Fatal(err)
		}
	}
}
