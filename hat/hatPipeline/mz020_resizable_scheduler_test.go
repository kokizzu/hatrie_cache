package hatPipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestResizableSchedulerResizeRunsEveryQueuedTask(t *testing.T) {
	scheduler, err := NewResizableScheduler(context.Background(), 4, 16)
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Int64
	for range 512 {
		if err := scheduler.Submit(context.Background(), func(context.Context) error {
			completed.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Resize(1); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if completed.Load() != 512 {
		t.Fatalf("completed tasks = %d, want 512", completed.Load())
	}
	if got := scheduler.WorkerCount(); got != 1 {
		t.Fatalf("worker count = %d, want 1", got)
	}
}

func TestResizableSchedulerScaleUpDoesNotWaitForOneWorker(t *testing.T) {
	scheduler, err := NewResizableScheduler(context.Background(), 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	task := func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}
	for range 3 {
		if err := scheduler.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.Resize(3); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			_ = scheduler.Wait()
			t.Fatal("scale-up did not run all workers")
		}
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestResizableSchedulerDownscaleWaitsForRunningTasks(t *testing.T) {
	scheduler, err := NewResizableScheduler(context.Background(), 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 3)
	release := make(chan struct{})
	task := func(context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}
	for range 3 {
		if err := scheduler.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(time.Second):
			close(release)
			_ = scheduler.Wait()
			t.Fatal("initial workers did not start")
		}
	}
	if err := scheduler.Resize(1); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestResizableSchedulerValidationAndFailurePropagation(t *testing.T) {
	if _, err := NewResizableScheduler(context.Background(), 0, 1); !errors.Is(err, ErrResizableSchedulerInvalid) {
		t.Fatalf("zero-worker error = %v", err)
	}
	if _, err := NewResizableScheduler(context.Background(), 1, -1); !errors.Is(err, ErrResizableSchedulerInvalid) {
		t.Fatalf("negative-queue error = %v", err)
	}
	scheduler, err := NewResizableScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Resize(0); !errors.Is(err, ErrResizableSchedulerWorkerCount) {
		t.Fatalf("zero resize error = %v", err)
	}
	want := errors.New("task failed")
	if err := scheduler.Submit(context.Background(), func(context.Context) error { return want }); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Wait(); !errors.Is(err, want) {
		t.Fatalf("wait error = %v, want %v", err, want)
	}
	if err := scheduler.Resize(2); !errors.Is(err, ErrResizableSchedulerClosed) {
		t.Fatalf("resize after wait error = %v", err)
	}
}

func TestResizableSchedulerCloseRejectsNewTasks(t *testing.T) {
	scheduler, err := NewResizableScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Close()
	if err := scheduler.Submit(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrResizableSchedulerClosed) {
		t.Fatalf("submit after close error = %v", err)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}
