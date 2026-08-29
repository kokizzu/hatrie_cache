package hatSql

import "testing"

var sqlExecutionArenaBenchmarkSink int

func BenchmarkSQLColumnarRowEnvelopeAllocation(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for range 2 {
			rows := make([]sqlExecRow, 4096)
			rows[0].singleAlias = "job"
			sqlExecutionArenaBenchmarkSink += len(rows)
		}
	}
}

func BenchmarkSQLColumnarRowEnvelopeArenaReuse(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		arena := sqlExecutionArena{}
		for range 2 {
			rows := arena.acquireColumnarRows(4096)
			rows[0].singleAlias = "job"
			sqlExecutionArenaBenchmarkSink += len(rows)
		}
	}
}
