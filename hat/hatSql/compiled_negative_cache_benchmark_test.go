package hatSql

import "testing"

func BenchmarkSQLCompiledQueryCacheRepeatedInvalidSource(b *testing.B) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT"
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 64, MaxBytes: 8 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Compile(source); err == nil {
			b.Fatal("invalid source unexpectedly compiled")
		}
	}
}

func BenchmarkSQLCompiledQueryCacheRepeatedValidSource(b *testing.B) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT id"
	cache, err := NewSQLCompiledQueryCache(SQLCompiledQueryCacheOptions{MaxEntries: 64, MaxBytes: 8 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.Compile(source); err != nil {
			b.Fatal(err)
		}
	}
}
