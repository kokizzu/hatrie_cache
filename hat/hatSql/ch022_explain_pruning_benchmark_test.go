package hatSql

import (
	"context"
	"encoding/json"
	"testing"
)

func BenchmarkCH022ExplainAnalyze(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		probe := ch022ExplainProbe()
		if _, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2", probe, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH022ExplainPlanWireBytes(b *testing.B) {
	result, err := ExecuteSQLQueryParameters(context.Background(), "EXPLAIN ANALYZE SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2", ch022ExplainProbe(), nil, SQLQueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	legacyPlan := append([]ExplainStep(nil), result.Plan...)
	for index := range legacyPlan {
		legacyPlan[index].Pruning = nil
	}
	legacyPayload, err := json.Marshal(legacyPlan)
	if err != nil {
		b.Fatal(err)
	}
	structuredPayload, err := json.Marshal(result.Plan)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(result.Plan); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(legacyPayload)), "legacy_plan_bytes")
	b.ReportMetric(float64(len(structuredPayload)), "structured_plan_bytes")
	b.ReportMetric(float64(len(structuredPayload)-len(legacyPayload)), "added_plan_bytes")
}

func ch022ExplainProbe() *sqlSegmentedColumnarSourceProbe {
	ids := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7)}
	scores := []interface{}{int64(1), int64(1), int64(1), int64(1), int64(100), int64(101), int64(200), int64(200)}
	return &sqlSegmentedColumnarSourceProbe{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Rows: len(ids)},
		segments: &ColumnarNumericSegments{
			RowsPerSegment: 2,
			Columns: map[string][]ColumnarNumericSegment{
				"score": {
					{Minimum: 1, Maximum: 1, Valid: true},
					{Minimum: 1, Maximum: 1, Valid: true},
					{Minimum: 100, Maximum: 101, Valid: true},
					{Minimum: 200, Maximum: 200, Valid: true},
				},
			},
		},
	}
}
