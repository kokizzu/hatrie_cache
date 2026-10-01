package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLColumnarTypedNumericAggregates(b *testing.B) {
	const rows = 100000
	values := make([]float64, rows)
	valid := make([]bool, rows)
	for index := range values {
		values[index] = float64(index%1000) + 0.5
		valid[index] = index%5 != 0
	}
	resolver := chu65NumericAggregateResolver{batch: chu65FloatBatch(values, valid)}
	query := "SELECT SUM(value) AS sum, AVG(value) AS avg, MIN(value) AS min, MAX(value) AS max FROM CACHE('items')"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["min"] != float64(1.5) || result.Rows[0]["max"] != float64(999.5) {
			b.Fatalf("aggregate result = %#v", result.Rows)
		}
	}
}
