package hatStorage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCompactionSchedulerStatsReportsQueueAndOutcomes(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{MaxConcurrent: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := scheduler.Stats(); got.MaxConcurrent != 2 || got.Pending != 0 || got.Running != 0 || got.Scheduled != 0 || got.Completed != 0 || got.Failed != 0 {
		t.Fatalf("initial scheduler stats = %#v", got)
	}
	callback := func(context.Context) error { return nil }
	if accepted, err := scheduler.Schedule("success", callback); err != nil || !accepted {
		t.Fatalf("Schedule(success) = %t, %v", accepted, err)
	}
	if accepted, err := scheduler.Schedule("success", callback); err != nil || accepted {
		t.Fatalf("duplicate Schedule(success) = %t, %v", accepted, err)
	}
	if got := scheduler.Stats(); got.Pending != 1 || got.Running != 0 || got.Scheduled != 0 {
		t.Fatalf("queued scheduler stats = %#v", got)
	}
	if result, err := scheduler.Run(context.Background()); err != nil || result.Completed != 1 {
		t.Fatalf("successful Run() = %#v, %v", result, err)
	}
	if got := scheduler.Stats(); got.Pending != 0 || got.Running != 0 || got.Scheduled != 1 || got.Completed != 1 || got.Failed != 0 {
		t.Fatalf("completed scheduler stats = %#v", got)
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
	runDone := make(chan error, 1)
	go func() {
		_, err := scheduler.Run(context.Background())
		runDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for running callback")
	}
	if got := scheduler.Stats(); got.Pending != 0 || got.Running != 1 || got.Scheduled != 2 || got.Completed != 1 || got.Failed != 0 {
		t.Fatalf("running scheduler stats = %#v", got)
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	accepted, err := scheduler.Schedule("retry", func(context.Context) error { return errors.New("retry") })
	if err != nil || !accepted {
		t.Fatalf("Schedule(retry) = %t, %v", accepted, err)
	}
	if _, err := scheduler.Run(context.Background()); err == nil {
		t.Fatal("failed Run() returned nil error")
	}
	got := scheduler.Stats()
	if got.Pending != 1 || got.Running != 0 || got.Scheduled != 3 || got.Completed != 2 || got.Failed != 1 {
		t.Fatalf("failed scheduler stats = %#v", got)
	}
}
