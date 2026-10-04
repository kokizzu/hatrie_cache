package hatPipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
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

func TestSchedulerSubmitWithOptionsPropagatesDeadline(t *testing.T) {
	scheduler, err := NewScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	if err := scheduler.SubmitWithOptions(context.Background(), TaskOptions{Timeout: 20 * time.Millisecond}, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := scheduler.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want deadline exceeded", err)
	}
}

func TestSchedulerSubmitWithOptionsForwardsCallerCancellation(t *testing.T) {
	scheduler, err := NewScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	if err := scheduler.SubmitWithOptions(caller, TaskOptions{PropagateCaller: true}, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	if err := scheduler.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want canceled", err)
	}
}

func TestSchedulerSubmitWithOptionsDoesNotPropagateCallerWithoutOptIn(t *testing.T) {
	scheduler, err := NewScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	release := make(chan struct{})
	observed := make(chan error, 1)
	if err := scheduler.SubmitWithOptions(caller, TaskOptions{Deadline: time.Now().Add(time.Hour)}, func(ctx context.Context) error {
		close(started)
		<-release
		observed <- ctx.Err()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	close(release)
	if err := <-observed; err != nil {
		t.Fatalf("task context error = %v, want nil", err)
	}
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestSchedulerSubmitWithOptionsDeadlineIncludesQueueWait(t *testing.T) {
	scheduler, err := NewScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan struct{})
	release := make(chan struct{})
	if err := scheduler.Submit(context.Background(), func(context.Context) error {
		close(firstStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	secondStarted := make(chan struct{})
	if err := scheduler.SubmitWithOptions(context.Background(), TaskOptions{Timeout: 20 * time.Millisecond}, func(ctx context.Context) error {
		close(secondStarted)
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	close(release)
	<-secondStarted
	if err := scheduler.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait() error = %v, want queued deadline exceeded", err)
	}
}

func TestSchedulerSubmitWithOptionsRejectsNegativeTimeout(t *testing.T) {
	scheduler, err := NewScheduler(context.Background(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.SubmitWithOptions(context.Background(), TaskOptions{Timeout: time.Second}, nil); !errors.Is(err, ErrSchedulerInvalid) {
		t.Fatalf("nil task error = %v, want %v", err, ErrSchedulerInvalid)
	}
	if err := scheduler.SubmitWithOptions(context.Background(), TaskOptions{Timeout: -time.Millisecond}, func(context.Context) error { return nil }); !errors.Is(err, ErrSchedulerTaskOptionsInvalid) {
		t.Fatalf("negative timeout error = %v, want %v", err, ErrSchedulerTaskOptionsInvalid)
	}
	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}
