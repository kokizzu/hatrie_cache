package hatStorage

import (
	"context"
	"strconv"
	"testing"
	"time"
)

var compactionMetricsC239Sink CompactionMetrics

func BenchmarkC239ExistingHistoryComposition(b *testing.B) {
	scheduler, diagnostics := benchmarkC239CompactionFixture(b)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stats := scheduler.Stats()
		ages := scheduler.Ages()
		snapshots := diagnostics.Snapshot()
		var metrics CompactionMetrics
		metrics.MaxConcurrent = stats.MaxConcurrent
		metrics.Pending = stats.Pending
		metrics.Running = stats.Running
		metrics.Scheduled = stats.Scheduled
		metrics.Completed = stats.Completed
		metrics.Failed = stats.Failed
		metrics.OldestPendingAge = ages.OldestPendingAge(now)
		metrics.OldestRunningAge = ages.OldestRunningAge(now)
		for _, snapshot := range snapshots {
			metrics.Arrangements++
			metrics.TotalObservations += snapshot.TotalObservations
			metrics.SuccessfulCompactions += snapshot.SuccessfulCompactions
			metrics.FailedCompactions += snapshot.FailedCompactions
			metrics.InputBytes += snapshot.Last.InputBytes
			metrics.OutputBytes += snapshot.Last.OutputBytes
			metrics.CurrentCompactionDebtBytes += snapshot.Last.CompactionDebtBytes
		}
		compactionMetricsC239Sink = metrics
	}
}

func BenchmarkC239ExistingCompactionMetricComposition(b *testing.B) {
	scheduler, diagnostics := benchmarkC239CompactionFixture(b)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		stats := scheduler.Stats()
		ages := scheduler.Ages()
		summary := diagnostics.Summary()
		compactionMetricsC239Sink = CompactionMetrics{
			MaxConcurrent:              stats.MaxConcurrent,
			Pending:                    stats.Pending,
			Running:                    stats.Running,
			Scheduled:                  stats.Scheduled,
			Completed:                  stats.Completed,
			Failed:                     stats.Failed,
			IOBytesPerSecond:           stats.IOBytesPerSecond,
			IOThrottledTaskCount:       stats.IOThrottledTaskCount,
			IOThrottledBytes:           stats.IOThrottledBytes,
			IOWaitNanoseconds:          stats.IOWaitNanoseconds,
			OldestPendingAge:           ages.OldestPendingAge(now),
			OldestRunningAge:           ages.OldestRunningAge(now),
			Arrangements:               summary.Arrangements,
			TotalObservations:          summary.TotalObservations,
			SuccessfulCompactions:      summary.SuccessfulCompactions,
			FailedCompactions:          summary.FailedCompactions,
			InputBytes:                 summary.InputBytes,
			OutputBytes:                summary.OutputBytes,
			CurrentCompactionDebtBytes: summary.CurrentCompactionDebtBytes,
		}
	}
}

func BenchmarkC239SnapshotCompactionMetrics(b *testing.B) {
	scheduler, diagnostics := benchmarkC239CompactionFixture(b)
	now := time.Now()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		compactionMetricsC239Sink = SnapshotCompactionMetrics(scheduler, diagnostics, now)
	}
}

func benchmarkC239CompactionFixture(b *testing.B) (*CompactionScheduler, *CompactionDiagnostics) {
	b.Helper()
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 2})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if accepted, scheduleErr := scheduler.Schedule("part-"+strconv.Itoa(index), func(context.Context) error { return nil }); scheduleErr != nil || !accepted {
			b.Fatalf("Schedule(%d) = %t, %v", index, accepted, scheduleErr)
		}
	}
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 4, HistoryPerArrangement: 4})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		name := "part-" + strconv.Itoa(index)
		if err := diagnostics.Register(name); err != nil {
			b.Fatal(err)
		}
		if err := diagnostics.Record(CompactionObservation{
			Arrangement:         name,
			InputBytes:          1 << 20,
			OutputBytes:         1 << 18,
			CompactionDebtBytes: 1 << 19,
			Outcome:             CompactionSucceeded,
		}); err != nil {
			b.Fatal(err)
		}
	}
	return scheduler, diagnostics
}
