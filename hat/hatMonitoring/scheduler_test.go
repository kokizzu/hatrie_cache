package hatMonitoring

import "testing"

var schedulerReportBenchmarkSink SchedulerReport

func TestReadSchedulerReport(t *testing.T) {
	report := ReadSchedulerReport()
	if report.CollectedAt.IsZero() {
		t.Fatal("scheduler report has no collection timestamp")
	}
	if report.Goroutines == 0 {
		t.Fatal("scheduler report has no goroutine count")
	}
	if report.GOMAXPROCS == 0 || report.NumCPU == 0 {
		t.Fatalf("scheduler report = %#v, want positive processor counts", report)
	}
}

func BenchmarkReadSchedulerReport(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		schedulerReportBenchmarkSink = ReadSchedulerReport()
	}
}
