package hatStorage

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestC239CompactionSchedulerStatsReportsEstimatedBacklog(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var startedOnce sync.Once
	release := make(chan struct{})
	for _, task := range []struct {
		name  string
		bytes uint64
	}{
		{name: "part-a", bytes: 100},
		{name: "part-b", bytes: 300},
	} {
		if accepted, err := scheduler.ScheduleWithIO(task.name, task.bytes, func(context.Context) error {
			startedOnce.Do(func() { close(started) })
			<-release
			return nil
		}); err != nil || !accepted {
			t.Fatalf("ScheduleWithIO(%q) = %t, %v", task.name, accepted, err)
		}
	}
	queued := scheduler.Stats()
	if queued.Pending != 2 || queued.PendingEstimatedBytes != 400 || queued.RunningEstimatedBytes != 0 {
		t.Fatalf("queued stats = %#v, want two pending bytes=400", queued)
	}

	done := make(chan error, 1)
	go func() {
		_, runErr := scheduler.Run(context.Background())
		done <- runErr
	}()
	<-started
	running := scheduler.Stats()
	if running.Pending != 0 || running.Running != 2 || running.PendingEstimatedBytes != 0 || running.RunningEstimatedBytes != 400 {
		t.Fatalf("running stats = %#v, want running bytes=400", running)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	finished := scheduler.Stats()
	if finished.Running != 0 || finished.RunningEstimatedBytes != 0 {
		t.Fatalf("finished stats = %#v, want no running bytes", finished)
	}
}

func TestC239CompactionDiagnosticsReportsSuccessfulAmplification(t *testing.T) {
	diagnostics, err := NewCompactionDiagnostics(CompactionDiagnosticsOptions{HistoryPerArrangement: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := diagnostics.Register("part-0"); err != nil {
		t.Fatal(err)
	}
	observations := []CompactionObservation{
		{Arrangement: "part-0", InputBytes: 100, OutputBytes: 200, Outcome: CompactionSucceeded},
		{Arrangement: "part-0", InputBytes: 300, OutputBytes: 600, Outcome: CompactionSucceeded},
		{Arrangement: "part-0", InputBytes: 900, OutputBytes: 1_800, Outcome: CompactionFailed},
	}
	for _, observation := range observations {
		if err := diagnostics.Record(observation); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := diagnostics.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(snapshot))
	}
	got := snapshot[0]
	if got.SuccessfulInputBytes != 400 || got.SuccessfulOutputBytes != 800 {
		t.Fatalf("successful bytes = %d/%d, want 400/800", got.SuccessfulInputBytes, got.SuccessfulOutputBytes)
	}
	if got.WriteAmplification != 2 {
		t.Fatalf("WriteAmplification = %v, want 2", got.WriteAmplification)
	}
}

func TestC239CompactionSchedulerStatsRetainsRetryEstimate(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	if accepted, err := scheduler.ScheduleWithPriorityAndIO("retry", 1, 500, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("retry once")
		}
		return nil
	}); err != nil || !accepted {
		t.Fatalf("ScheduleWithPriorityAndIO() = %t, %v", accepted, err)
	}
	if _, err := scheduler.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want first attempt to fail")
	}
	retry := scheduler.Stats()
	if retry.Pending != 1 || retry.PendingEstimatedBytes != 500 || retry.Running != 0 || retry.RunningEstimatedBytes != 0 {
		t.Fatalf("retry stats = %#v, want one pending 500-byte task", retry)
	}
	if run, err := scheduler.Run(context.Background()); err != nil || run.Completed != 1 {
		t.Fatalf("retry Run() = %#v, %v, want one completion", run, err)
	}
	finished := scheduler.Stats()
	if finished.Pending != 0 || finished.Running != 0 || finished.PendingEstimatedBytes != 0 || finished.RunningEstimatedBytes != 0 {
		t.Fatalf("finished retry stats = %#v, want zero queue bytes", finished)
	}
}
