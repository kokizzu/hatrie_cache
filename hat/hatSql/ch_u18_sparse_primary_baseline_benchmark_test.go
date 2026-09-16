package hatSql

import "testing"

var benchmarkSQLSparsePrimaryRangeSink int

func benchmarkSQLSparsePrimaryRangeWorkload() (*ColumnarNumericSegments, []sqlColumnarNumericFilter, int) {
	const (
		rowsPerSegment = 256
		rowsPerTenant  = 8192
		tenantCount    = 100
	)
	segmentCount := tenantCount * rowsPerTenant / rowsPerSegment
	segments := &ColumnarNumericSegments{
		RowsPerSegment:     rowsPerSegment,
		SparsePrimaryField: "tenant",
		Columns: map[string][]ColumnarNumericSegment{
			"tenant": make([]ColumnarNumericSegment, segmentCount),
		},
	}
	for segment := 0; segment < segmentCount; segment++ {
		startRow := segment * rowsPerSegment
		endRow := startRow + rowsPerSegment - 1
		segments.Columns["tenant"][segment] = ColumnarNumericSegment{
			Minimum: float64(startRow / rowsPerTenant),
			Maximum: float64(endRow / rowsPerTenant),
			Valid:   true,
		}
	}
	predicates := []sqlColumnarNumericFilter{
		{field: "tenant", operator: "=", value: 42},
		{field: "id", operator: ">=", value: 7000},
	}
	return segments, predicates, segmentCount
}

func BenchmarkSQLColumnarSparsePrimarySingleFieldRange(b *testing.B) {
	segments, predicates, segmentCount := benchmarkSQLSparsePrimaryRangeWorkload()
	start, end, used := sqlColumnarSparsePrimarySegmentRange(segments, predicates, segmentCount)
	if !used {
		b.Fatal("single-field sparse primary range was not used")
	}
	b.ReportMetric(float64(end-start), "segments/query")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		start, end, _ = sqlColumnarSparsePrimarySegmentRange(segments, predicates, segmentCount)
		benchmarkSQLSparsePrimaryRangeSink = end - start
	}
}
