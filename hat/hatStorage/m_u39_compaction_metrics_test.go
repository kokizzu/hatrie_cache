package hatStorage

import (
	"context"
	"testing"
	"time"
)

func TestC239SnapshotCompactionMetricsCombinesBacklogAgeAndAmplification(t *testing.T) {
	base := time.Unix(500, 0)
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.now = func() time.Time { return base }
	if accepted, err := scheduler.Schedule("orders", func(context.Context) error { return nil }); err != nil || !accepted {
		t.Fatalf("Schedule() = %t, %v", accepted, err)
	}
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 1, HistoryPerArrangement: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := diagnostics.Record(CompactionObservation{
		Arrangement:         "orders",
		InputBytes:          900,
		OutputBytes:         300,
		CompactionDebtBytes: 120,
		Outcome:             CompactionSucceeded,
	}); err != nil {
		t.Fatal(err)
	}

	metrics := SnapshotCompactionMetrics(scheduler, diagnostics, base.Add(5*time.Second))
	if metrics.Pending != 1 || metrics.Running != 0 {
		t.Fatalf("backlog = %#v, want one pending task", metrics)
	}
	if metrics.OldestPendingAge != 5*time.Second || metrics.OldestRunningAge != 0 {
		t.Fatalf("ages = %#v, want 5s pending and zero running", metrics)
	}
	if metrics.InputBytes != 900 || metrics.OutputBytes != 300 || metrics.CurrentCompactionDebtBytes != 120 {
		t.Fatalf("byte metrics = %#v", metrics)
	}
	if ratio, ok := metrics.InputToOutputRatio(); !ok || ratio != 3 {
		t.Fatalf("InputToOutputRatio() = %v/%t, want 3/true", ratio, ok)
	}
}

func TestC239SnapshotCompactionMetricsNilInputsAreZero(t *testing.T) {
	metrics := SnapshotCompactionMetrics(nil, nil, time.Unix(500, 0))
	if metrics != (CompactionMetrics{}) {
		t.Fatalf("nil snapshot = %#v, want zero", metrics)
	}
}

func TestC239CompactionDiagnosticsSummaryPreservesCumulativeBytes(t *testing.T) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{MaxArrangements: 1, HistoryPerArrangement: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := diagnostics.Register("orders"); err != nil {
		t.Fatal(err)
	}
	for _, observation := range []CompactionObservation{
		{Arrangement: "orders", InputBytes: 100, OutputBytes: 25, CompactionDebtBytes: 7, Outcome: CompactionSucceeded},
		{Arrangement: "orders", InputBytes: 200, OutputBytes: 100, CompactionDebtBytes: 9, Outcome: CompactionFailed},
	} {
		if err := diagnostics.Record(observation); err != nil {
			t.Fatal(err)
		}
	}
	summary := diagnostics.Summary()
	if summary.TotalObservations != 2 || summary.SuccessfulCompactions != 1 || summary.FailedCompactions != 1 {
		t.Fatalf("summary counts = %#v", summary)
	}
	if summary.InputBytes != 300 || summary.OutputBytes != 125 || summary.CurrentCompactionDebtBytes != 9 {
		t.Fatalf("summary bytes = %#v", summary)
	}
}
