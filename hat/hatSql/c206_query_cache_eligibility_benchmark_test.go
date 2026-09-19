package hatSql

import "testing"

var benchmarkC206ResultCacheKey string

func BenchmarkC206ResultCacheAdmissionDeterministic(b *testing.B) {
	benchmarkC206ResultCacheAdmission(b, "FROM CACHE('events') SELECT id WHERE id = $1", true)
}

func BenchmarkC206ResultCacheAdmissionVolatile(b *testing.B) {
	benchmarkC206ResultCacheAdmission(b, "FROM CACHE('events') SELECT id, NOW() AS observed_at WHERE id = $1", false)
}

func benchmarkC206ResultCacheAdmission(b *testing.B, source string, wantKey bool) {
	b.ReportAllocs()
	parameters := []interface{}{int64(42)}
	for iteration := 0; iteration < b.N; iteration++ {
		key, ok := sqlResultCacheKey(source, parameters, SQLQueryOptions{})
		if ok != wantKey {
			b.Fatalf("sqlResultCacheKey(%q) accepted=%t, want %t", source, ok, wantKey)
		}
		benchmarkC206ResultCacheKey = key
	}
}
