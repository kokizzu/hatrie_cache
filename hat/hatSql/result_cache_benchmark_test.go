package hatSql

import (
	"context"
	"testing"
)

func BenchmarkResultCacheHit(b *testing.B) {
	cache := NewResultCache(1)
	rows := make([]Row, 256)
	for index := range rows {
		rows[index] = Row{"id": index, "payload": map[string]interface{}{"items": []interface{}{map[string]interface{}{"name": "cached"}}}}
	}
	execute := func(context.Context) (QueryResult, error) {
		return QueryResult{Columns: []string{"id", "payload"}, Rows: rows}, nil
	}
	if _, err := cache.Execute(context.Background(), "cached", func() uint64 { return 1 }, execute); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := cache.Execute(context.Background(), "cached", func() uint64 { return 1 }, execute); err != nil {
			b.Fatal(err)
		}
	}
}
