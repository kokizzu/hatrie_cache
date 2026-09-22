package hatSql

import "testing"

var c239PartMergeMetricsSink uint64

func BenchmarkC239PartMergeMetrics(b *testing.B) {
	table := newC239PartMergeMetricsBaselineTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		metrics := table.PartMergeMetrics()
		c239PartMergeMetricsSink += uint64(metrics.PendingDeletes) + metrics.RowsRead + metrics.RowsWritten + metrics.DeletedRows
	}
}
