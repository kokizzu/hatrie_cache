package hatSql

import (
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
