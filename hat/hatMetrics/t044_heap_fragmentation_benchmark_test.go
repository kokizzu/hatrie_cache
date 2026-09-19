package hatMetrics

import (
	"runtime"
	"testing"
)

var benchmarkT044Report HeapFragmentationReport

func BenchmarkT044ReadHeapFragmentationReport(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkT044Report = ReadHeapFragmentationReport()
	}
	runtime.KeepAlive(benchmarkT044Report)
}
