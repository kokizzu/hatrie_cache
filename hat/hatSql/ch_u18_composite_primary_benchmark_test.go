package hatSql

import "testing"

func benchmarkSQLCompositeSparsePrimaryRangeWorkload() (*ColumnarNumericSegments, []sqlColumnarNumericFilter, int) {
	segments, predicates, segmentCount := benchmarkSQLSparsePrimaryRangeWorkload()
	const rowsPerTenant = 8192
	const fields = 2
	segments.SparsePrimaryFields = []string{"tenant", "id"}
	segments.SparsePrimaryTupleMinimum = make([]float64, segmentCount*fields)
	segments.SparsePrimaryTupleMaximum = make([]float64, segmentCount*fields)
	for segment := 0; segment < segmentCount; segment++ {
		startRow := segment * segments.RowsPerSegment
		endRow := startRow + segments.RowsPerSegment - 1
		minimumOffset := segment * fields
		segments.SparsePrimaryTupleMinimum[minimumOffset] = float64(startRow / rowsPerTenant)
		segments.SparsePrimaryTupleMinimum[minimumOffset+1] = float64(startRow % rowsPerTenant)
		segments.SparsePrimaryTupleMaximum[minimumOffset] = float64(endRow / rowsPerTenant)
		segments.SparsePrimaryTupleMaximum[minimumOffset+1] = float64(endRow % rowsPerTenant)
	}
	return segments, predicates, segmentCount
}

func BenchmarkSQLColumnarSparsePrimaryCompositeRange(b *testing.B) {
	segments, predicates, segmentCount := benchmarkSQLCompositeSparsePrimaryRangeWorkload()
	start, end, used := sqlColumnarSparsePrimarySegmentRangeWithComposite(segments, predicates, segmentCount)
	if !used {
		b.Fatal("composite sparse primary range was not used")
	}
	b.ReportMetric(float64(end-start), "segments/query")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		start, end, _ = sqlColumnarSparsePrimarySegmentRangeWithComposite(segments, predicates, segmentCount)
		benchmarkSQLSparsePrimaryRangeSink = end - start
	}
}
