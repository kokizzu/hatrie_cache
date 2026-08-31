package hatSql

import "testing"

func BenchmarkSQLColumnarStreamMaterializeLimit(b *testing.B) {
	const rows = 20_000
	ids := make([]interface{}, rows)
	for index := range ids {
		ids[index] = int64(index)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"id": ids}, Rows: rows}
	query := &sqlQuery{
		limit: 50,
		selects: []sqlSelectItem{{
			expr: sqlExpr{kind: "field", name: "id"},
		}},
	}
	for _, benchmark := range []struct {
		name    string
		scanAll bool
	}{
		{name: "limit_pushdown"},
		{name: "full_scan", scanAll: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, _ := sqlColumnarStreamMaterializeWithScan(query, batch, []string{"id"}, func(int) bool { return true }, benchmark.scanAll)
				if len(result.Rows) != query.limit {
					b.Fatalf("rows = %d, want %d", len(result.Rows), query.limit)
				}
			}
		})
	}
}
