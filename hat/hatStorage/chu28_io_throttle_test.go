package hatStorage_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestCHU28CompactionSchedulerThrottlesEstimatedIO(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
		MaxConcurrent:       1,
		MaxIOBytesPerSecond: 1_000_000_000,
	})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	var order []string
	appendOrder := func(name string) func(context.Context) error {
		return func(context.Context) error {
			order = append(order, name)
			return nil
		}
	}
	const estimatedBytes = 64 << 20
	if queued, err := scheduler.ScheduleWithIO("first", estimatedBytes, appendOrder("first")); err != nil || !queued {
		t.Fatalf("ScheduleWithIO(first) = (%v, %v), want (true, nil)", queued, err)
	}
	if queued, err := scheduler.ScheduleWithIO("second", estimatedBytes, appendOrder("second")); err != nil || !queued {
		t.Fatalf("ScheduleWithIO(second) = (%v, %v), want (true, nil)", queued, err)
	}
	run, err := scheduler.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if run.Scheduled != 2 || run.Completed != 2 || run.Failed != 0 {
		t.Fatalf("Run() = %#v, want two completed tasks", run)
	}
	if want := []string{"first", "second"}; len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Fatalf("execution order = %v, want %v", order, want)
	}
	stats := scheduler.Stats()
	if stats.IOBytesPerSecond != 1_000_000_000 {
		t.Fatalf("IOBytesPerSecond = %d, want 1000000000", stats.IOBytesPerSecond)
	}
	if stats.IOThrottledTaskCount == 0 || stats.IOThrottledBytes < estimatedBytes || stats.IOWaitNanoseconds == 0 {
		t.Fatalf("throttle stats = %#v, want a delayed second task", stats)
	}
}

func TestCHU28CompactionSchedulerThrottleCancellationRequeues(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{
		MaxConcurrent:       1,
		MaxIOBytesPerSecond: 1_000_000_000,
	})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	var secondCalls atomic.Int32
	const estimatedBytes = 64 << 20
	if _, err := scheduler.ScheduleWithIO("first", estimatedBytes, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("ScheduleWithIO(first) error = %v", err)
	}
	if _, err := scheduler.ScheduleWithIO("second", estimatedBytes, func(context.Context) error {
		secondCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("ScheduleWithIO(second) error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	run, err := scheduler.Run(ctx)
	if err == nil || run.Failed != 1 || run.Completed != 1 {
		t.Fatalf("Run() = %#v, error %v, want one canceled failure and one completion", run, err)
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("canceled throttled task calls = %d, want 0", secondCalls.Load())
	}
	if pending := scheduler.Pending(); pending != 1 {
		t.Fatalf("Pending() = %d, want one requeued task", pending)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want context deadline", err)
	}
}

func TestCHU28CompactionSchedulerDefaultIOPathIsUnthrottled(t *testing.T) {
	scheduler, err := hatStorage.NewCompactionScheduler(hatStorage.CompactionSchedulerOptions{})
	if err != nil {
		t.Fatalf("NewCompactionScheduler() error = %v", err)
	}
	if queued, err := scheduler.ScheduleWithIO("default", 1<<40, func(context.Context) error { return nil }); err != nil || !queued {
		t.Fatalf("ScheduleWithIO() = (%v, %v), want (true, nil)", queued, err)
	}
	if _, err := scheduler.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	stats := scheduler.Stats()
	if stats.IOBytesPerSecond != 0 || stats.IOThrottledTaskCount != 0 || stats.IOWaitNanoseconds != 0 {
		t.Fatalf("default IO stats = %#v, want zero throttle state", stats)
	}
}
