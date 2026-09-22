package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC237ExplainProjectionSelection(b *testing.B) {
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{
				"id":      {float64(1), float64(2), float64(50), float64(51), float64(100), float64(101)},
				"payload": {"a", "b", "c", "d", "e", "f"},
			},
			Rows: 6,
		},
		rows: []Row{
			{"id": float64(1), "payload": "a"},
			{"id": float64(2), "payload": "b"},
			{"id": float64(50), "payload": "c"},
			{"id": float64(51), "payload": "d"},
			{"id": float64(100), "payload": "e"},
			{"id": float64(101), "payload": "f"},
		},
	}
	query := "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.payload"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, probe, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
