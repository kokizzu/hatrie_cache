package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLColumnarCountField(b *testing.B) {
	const rows = 100000
	values := make([]int64, rows)
	valid := make([]bool, rows)
	for index := range values {
		values[index] = int64(index)
		valid[index] = index%5 != 0
	}
	resolver := chu64CountFieldResolver{batch: chu64NumericBatch(values, valid)}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT COUNT(value) AS total FROM CACHE('items')", resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["total"] != int64(rows-rows/5) {
			b.Fatalf("count result = %#v", result.Rows)
		}
	}
}
