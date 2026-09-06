package hatPipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestSchedulerRunsTasksAndReturnsFirstTaskError(t *testing.T) {
	wantErr := errors.New("task failed")
	scheduler, err := NewScheduler(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	var ran atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	if err := scheduler.Submit(context.Background(), func(context.Context) error {
		close(started)
		<-release
		ran.Add(1)
		return wantErr
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := scheduler.Submit(context.Background(), func(context.Context) error {
		ran.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	scheduler.Close()
	close(release)
	if err := scheduler.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}
	if got := ran.Load(); got != 1 {
		t.Fatalf("tasks run = %d, want only the failing task", got)
	}
}

func TestSchedulerHonorsSubmitCancellationAndRejectsInvalidLifecycle(t *testing.T) {
	if _, err := NewScheduler(context.Background(), 0, 1); !errors.Is(err, ErrSchedulerInvalid) {
		t.Fatalf("zero workers error = %v, want %v", err, ErrSchedulerInvalid)
	}
	if _, err := NewScheduler(context.Background(), 1, -1); !errors.Is(err, ErrSchedulerInvalid) {
		t.Fatalf("negative capacity error = %v, want %v", err, ErrSchedulerInvalid)
	}
	scheduler, err := NewScheduler(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Submit(context.Background(), nil); !errors.Is(err, ErrSchedulerInvalid) {
		t.Fatalf("nil task error = %v, want %v", err, ErrSchedulerInvalid)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := scheduler.Submit(ctx, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submit error = %v, want context canceled", err)
	}
	scheduler.Close()
	if err := scheduler.Submit(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("closed submit error = %v, want %v", err, ErrSchedulerClosed)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("empty scheduler Wait() error = %v", err)
	}
}
