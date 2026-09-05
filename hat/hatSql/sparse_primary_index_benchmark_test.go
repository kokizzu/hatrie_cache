package hatSql

import (
	"context"
	"testing"
)

func BenchmarkSQLColumnarSparsePrimaryRange(b *testing.B) {
	const (
		rows           = 1 << 20
		rowsPerSegment = 256
	)
	values := make([]interface{}, rows)
	segments := make([]ColumnarNumericSegment, rows/rowsPerSegment)
	for row := range values {
		values[row] = float64(row)
		if row%rowsPerSegment == 0 {
			segment := row / rowsPerSegment
			segments[segment] = ColumnarNumericSegment{Minimum: float64(row), Maximum: float64(row + rowsPerSegment - 1), Valid: true}
		}
	}
	for _, workload := range []struct {
		name      string
		query     string
		wantTotal int64
	}{
		{name: "point_lookup", query: "FROM CACHE('events') AS event WHERE event.id = 524288 SELECT COUNT(*) AS total", wantTotal: 1},
		{name: "full_range", query: "FROM CACHE('events') AS event WHERE event.id >= 0 SELECT COUNT(*) AS total", wantTotal: rows},
	} {
		for _, test := range []struct {
			name   string
			sparse bool
		}{
			{name: "legacy_linear_marks", sparse: false},
			{name: "sparse_primary_binary_search", sparse: true},
		} {
			test := test
			b.Run(workload.name+"/"+test.name, func(b *testing.B) {
				fieldSegments := &ColumnarNumericSegments{
					RowsPerSegment: rowsPerSegment,
					Columns:        map[string][]ColumnarNumericSegment{"id": segments},
				}
				if test.sparse {
					fieldSegments.SparsePrimaryField = "id"
				}
				probe := &sqlSegmentedColumnarSourceProbe{
					batch:    ColumnarBatch{Columns: map[string][]interface{}{"id": values}, Rows: rows},
					segments: fieldSegments,
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					result, err := ExecuteSQLQueryParameters(context.Background(), workload.query, probe, nil, SQLQueryOptions{MaxRows: rows})
					if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != workload.wantTotal {
						b.Fatalf("query result = %#v, error %v; want one matching row", result.Rows, err)
					}
				}
			})
		}
	}
}
