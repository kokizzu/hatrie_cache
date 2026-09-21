package hatSql

import "testing"

func BenchmarkM212LogicalCompaction(b *testing.B) {
	table, frontier := newM212CompactionBenchmarkTable(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := table.AdvanceMVCCCompactionThrough(frontier); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	m212CompactionBenchmarkSink = table.mvcc.compactedThrough
	b.ReportMetric(float64(m212RetainedVersionNodes(table)), "retained-version-nodes")
}

func BenchmarkM212LogicalCompactionTransition(b *testing.B) {
	table, frontier := newM212CompactionBenchmarkTable(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		table.mvcc.compactedThrough = 0
		if err := table.AdvanceMVCCCompactionThrough(frontier); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	m212CompactionBenchmarkSink = table.mvcc.compactedThrough
	b.ReportMetric(float64(m212RetainedVersionNodes(table)), "retained-version-nodes")
}

func m212RetainedVersionNodes(table *TypedTable) int {
	if table == nil || table.mvcc == nil {
		return 0
	}
	retained := 0
	for _, head := range table.mvcc.heads {
		retained += m212VersionChainLength(head)
	}
	return retained
}
