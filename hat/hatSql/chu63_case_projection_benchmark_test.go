package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHU63CaseProjection(b *testing.B) {
	values := make([]interface{}, 16384)
	for row := range values {
		values[row] = int64(row % 4096)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	resolver := &chu63CaseProjectionResolver{batch: batch, rows: chu63CaseRows(batch)}
	query := "SELECT CASE WHEN value >= 2048 THEN 'high' ELSE 'low' END AS band FROM CACHE('items')"

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != len(values) {
			b.Fatalf("rows = %d, want %d", len(result.Rows), len(values))
		}
	}
}
