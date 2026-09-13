package hatSql

import (
	"context"
	"testing"
)

var ch024ExplainSegmentSkipSink SQLQueryResult

func BenchmarkCH024ExplainSegmentSkip(b *testing.B) {
	probe := &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": {1.0, 2.0, 50.0, 101.0}}, Rows: 4},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"id": {{Minimum: 1, Maximum: 2, Valid: true}, {Minimum: 50, Maximum: 101, Valid: true}},
			},
		},
	}
	query := "EXPLAIN ANALYZE FROM CACHE('events') AS event WHERE event.id >= 100 SELECT event.id"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, probe, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		ch024ExplainSegmentSkipSink = result
	}
}
