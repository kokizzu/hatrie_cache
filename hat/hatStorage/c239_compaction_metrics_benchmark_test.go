package hatStorage

import (
	"context"
	"testing"
)

func BenchmarkC239CompactionDiagnosticsRecord(b *testing.B) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{HistoryPerArrangement: 8})
	if err != nil {
		b.Fatal(err)
	}
	if err := diagnostics.Register("part-0"); err != nil {
		b.Fatal(err)
	}
	observation := CompactionObservation{
		Arrangement:   "part-0",
		LogicalBytes:  4096,
		PhysicalBytes: 8192,
		InputBytes:    8192,
		OutputBytes:   4096,
		Outcome:       CompactionSucceeded,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := diagnostics.Record(observation); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC239CompactionSchedulerStats(b *testing.B) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		b.Fatal(err)
	}
	if accepted, err := scheduler.ScheduleWithIO("part-0", 4096, func(context.Context) error { return nil }); err != nil || !accepted {
		b.Fatalf("ScheduleWithIO() = %t, %v", accepted, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scheduler.Stats()
	}
}
