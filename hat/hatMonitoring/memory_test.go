package hatMonitoring

import (
	"encoding/json"
	"runtime"
	"sync"
	"testing"
)

func TestReadMemoryReport(t *testing.T) {
	report := ReadMemoryReport()
	if report.CollectedAt.IsZero() {
		t.Fatal("CollectedAt is zero")
	}
	if report.AllocBytes != report.HeapAllocBytes {
		t.Fatalf("AllocBytes = %d, HeapAllocBytes = %d, want equal values", report.AllocBytes, report.HeapAllocBytes)
	}
	if report.TotalAllocBytes < report.AllocBytes {
		t.Fatalf("TotalAllocBytes = %d, AllocBytes = %d, want cumulative allocation no smaller than live allocation", report.TotalAllocBytes, report.AllocBytes)
	}
	if report.Mallocs < report.Frees {
		t.Fatalf("Mallocs = %d, Frees = %d, want malloc count no smaller than free count", report.Mallocs, report.Frees)
	}
	if report.HeapSysBytes < report.HeapInuseBytes {
		t.Fatalf("HeapSysBytes = %d, HeapInuseBytes = %d, want reserved heap no smaller than in-use heap", report.HeapSysBytes, report.HeapInuseBytes)
	}
	if report.HeapIdleBytes < report.HeapReleasedBytes {
		t.Fatalf("HeapIdleBytes = %d, HeapReleasedBytes = %d, want idle heap no smaller than released heap", report.HeapIdleBytes, report.HeapReleasedBytes)
	}
	if report.LastGCUnixNano < 0 {
		t.Fatalf("LastGCUnixNano = %d, want non-negative value", report.LastGCUnixNano)
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal memory report: %v", err)
	}
	var decoded MemoryReport
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal memory report: %v", err)
	}
	if decoded.HeapObjects != report.HeapObjects || decoded.NumGC != report.NumGC || decoded.GOMemLimitBytes != report.GOMemLimitBytes {
		t.Fatalf("round-trip report = %#v, want heap objects %d, num gc %d, memory limit %d", decoded, report.HeapObjects, report.NumGC, report.GOMemLimitBytes)
	}
}

func BenchmarkReadMemoryReport(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		benchmarkMemoryReportSink = ReadMemoryReport()
	}
}

func BenchmarkRuntimeReadMemStats(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		benchmarkMemoryStatsSink = stats
	}
}

var benchmarkMemoryReportSink MemoryReport
var benchmarkMemoryStatsSink runtime.MemStats

func TestReadMemoryReportConcurrent(t *testing.T) {
	const readers = 8
	reports := make(chan MemoryReport, readers)
	var wait sync.WaitGroup
	wait.Add(readers)
	for index := 0; index < readers; index++ {
		go func() {
			defer wait.Done()
			reports <- ReadMemoryReport()
		}()
	}
	wait.Wait()
	close(reports)
	for report := range reports {
		if report.CollectedAt.IsZero() || report.HeapObjects == 0 {
			t.Fatalf("concurrent memory report = %#v, want populated report", report)
		}
	}
}
