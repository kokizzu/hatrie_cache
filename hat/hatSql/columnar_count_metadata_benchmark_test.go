package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLColumnarCountStar(b *testing.B) {
	const rows = 100000
	resolver := sqlColumnarCountMetadataResolver{batch: ColumnarBatch{Rows: rows}}
	query := "SELECT COUNT(*) AS total FROM CACHE('items')"
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["total"] != int64(rows) {
			b.Fatalf("count result = %#v", result.Rows)
		}
	}
}
