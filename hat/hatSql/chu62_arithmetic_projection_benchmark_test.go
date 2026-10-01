package hatSql

import (
	"context"
	"testing"
)

func BenchmarkCHU62ArithmeticProjection(b *testing.B) {
	values := make([]interface{}, 16384)
	for row := range values {
		values[row] = int64(row)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	resolver := &chu62ArithmeticProjectionResolver{batch: batch, rows: chu62ArithmeticRows(batch)}
	query := "SELECT value + 1 AS incremented FROM CACHE('items')"

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
