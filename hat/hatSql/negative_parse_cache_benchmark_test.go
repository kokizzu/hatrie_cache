package hatSql

import "testing"

func BenchmarkSQLPreparedQueryCacheRepeatedInvalidSource(b *testing.B) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT"
	cache := NewSQLPreparedQueryCache(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseSQLQueryWithCache(source, nil, cache); err == nil {
			b.Fatal("invalid source unexpectedly parsed")
		}
	}
}

func BenchmarkSQLPreparedQueryCacheRepeatedValidSource(b *testing.B) {
	const source = "FROM VALUES (1, 2) AS items(id) SELECT id"
	cache := NewSQLPreparedQueryCache(64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ParseSQLQueryWithCache(source, nil, cache); err != nil {
			b.Fatal(err)
		}
	}
}
