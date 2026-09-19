package hatSql

import "testing"

var chu26QueryMemoryProfileBenchmarkSink bool

func BenchmarkCHU26QueryProfilerRecordMemory(b *testing.B) {
	profiler, err := NewSQLQueryProfiler(SQLQueryProfilerOptions{MaxMemoryOperatorsPerQuery: 1})
	if err != nil {
		b.Fatal(err)
	}
	sample := SQLQueryMemorySample{AllocatedBytes: 128, PeakBytes: 64, RetainedBytes: 32}
	if _, err := profiler.RecordMemory("q1", "scan", sample); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		captured, err := profiler.RecordMemory("q1", "scan", sample)
		if err != nil {
			b.Fatal(err)
		}
		chu26QueryMemoryProfileBenchmarkSink = captured
	}
}
