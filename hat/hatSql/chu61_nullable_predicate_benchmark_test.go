package hatSql

import "testing"

var chu61NullablePredicateBenchmarkSink int

func BenchmarkCHU61NullablePredicates(b *testing.B) {
	values := make([]interface{}, 16384)
	for row := range values {
		if row%8 == 0 {
			continue
		}
		values[row] = int64(row % 257)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	rows := chu61NullableRows(batch)

	for _, test := range []struct {
		name  string
		query string
	}{
		{name: "is-null", query: "SELECT value FROM CACHE('items') WHERE value IS NULL"},
		{name: "is-not-null", query: "SELECT value FROM CACHE('items') WHERE value IS NOT NULL"},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(batch.Rows), "rows/query")
			resolver := &chu61NullablePredicateResolver{batch: batch, rows: rows}
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQueryParameters(b.Context(), test.query, resolver, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				chu61NullablePredicateBenchmarkSink += len(result.Rows)
			}
		})
	}
}
