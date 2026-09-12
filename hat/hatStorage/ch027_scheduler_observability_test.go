package hatStorage

import (
	"context"
	"testing"
	"time"
)

func TestCompactionSchedulerStatsReportsQueueAndRunningAge(t *testing.T) {
	scheduler, err := NewCompactionScheduler(CompactionSchedulerOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(100, 0)
	now := base
	scheduler.now = func() time.Time { return now }
	if accepted, err := scheduler.Schedule("pending", func(context.Context) error { return nil }); err != nil || !accepted {
		t.Fatalf("Schedule(pending) = %t, %v", accepted, err)
	}
	now = base.Add(5 * time.Second)
	queued := scheduler.Ages()
	if age := queued.OldestPendingAge(base.Add(5 * time.Second)); age != 5*time.Second {
		t.Fatalf("queued age = %s, want %s", age, 5*time.Second)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	if accepted, err := scheduler.Schedule("running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil || !accepted {
		t.Fatalf("Schedule(running) = %t, %v", accepted, err)
	}
	now = base.Add(10 * time.Second)
	runDone := make(chan error, 1)
	go func() {
		_, runErr := scheduler.Run(context.Background())
		runDone <- runErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for running callback")
	}
	now = base.Add(13 * time.Second)
	running := scheduler.Ages()
	if age := running.OldestRunningAge(base.Add(13 * time.Second)); age != 3*time.Second {
		t.Fatalf("running age = %s, want %s", age, 3*time.Second)
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	finished := scheduler.Ages()
	if finished.OldestPendingAtUnixNanoseconds != 0 || finished.OldestRunningAtUnixNanoseconds != 0 {
		t.Fatalf("finished ages = %#v, want zero ages", finished)
	}
}
