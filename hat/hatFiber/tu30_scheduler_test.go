package hatFiber

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU30SchedulerRoundRobinAndReap(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 4})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var trace []int
	newStep := func(label int) StepFunc {
		runs := 0
		return func(context.Context) (Step, error) {
			runs++
			trace = append(trace, label)
			if runs < 3 {
				return StepYield, nil
			}
			return StepDone, nil
		}
	}

	first, err := scheduler.Spawn(newStep(1))
	if err != nil {
		t.Fatalf("Spawn(first) error = %v", err)
	}
	second, err := scheduler.Spawn(newStep(2))
	if err != nil {
		t.Fatalf("Spawn(second) error = %v", err)
	}
	cancelled, err := scheduler.Spawn(func(context.Context) (Step, error) {
		t.Fatal("cancelled fiber ran")
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Spawn(cancelled) error = %v", err)
	}
	if err := scheduler.Cancel(cancelled); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	stats, err := scheduler.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.Steps != 6 || stats.Completed != 2 || stats.Cancelled != 1 {
		t.Fatalf("Run() stats = %#v", stats)
	}
	if want := []int{1, 2, 1, 2, 1, 2}; !reflect.DeepEqual(trace, want) {
		t.Fatalf("trace = %v, want %v", trace, want)
	}

	for name, id := range map[string]FiberID{
		"first":     first,
		"second":    second,
		"cancelled": cancelled,
	} {
		status, err := scheduler.Status(id)
		if err != nil {
			t.Fatalf("Status(%s) error = %v", name, err)
		}
		want := StatusDone
		if name == "cancelled" {
			want = StatusCancelled
		}
		if status != want {
			t.Fatalf("Status(%s) = %v, want %v", name, status, want)
		}
		if err := scheduler.Reap(id); err != nil {
			t.Fatalf("Reap(%s) error = %v", name, err)
		}
	}
}

func TestTU30SchedulerCancellationPreservesReadyFiber(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	runs := 0
	id, err := scheduler.Spawn(func(ctx context.Context) (Step, error) {
		runs++
		if runs == 1 {
			return StepYield, nil
		}
		return StepDone, ctx.Err()
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stats, err := scheduler.Run(ctx, 1)
	if err != nil || stats.Steps != 1 {
		t.Fatalf("first Run() = %#v, error = %v", stats, err)
	}
	cancel()
	if _, err := scheduler.Run(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Run() error = %v, want context.Canceled", err)
	}
	if status, err := scheduler.Status(id); err != nil || status != StatusReady {
		t.Fatalf("Status() = %v, error = %v, want ready", status, err)
	}
	if err := scheduler.Reap(id); !errors.Is(err, ErrFiberNotFinished) {
		t.Fatalf("Reap(ready) error = %v, want ErrFiberNotFinished", err)
	}
}

func TestTU30SchedulerBoundsFibers(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, nil })
	if err != nil {
		t.Fatalf("Spawn(first) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, nil }); !errors.Is(err, ErrFiberCapacity) {
		t.Fatalf("Spawn(second) error = %v, want ErrFiberCapacity", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := scheduler.Reap(first); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}

func TestTU30SchedulerReusesSlotsWithoutStaleIDs(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, nil })
	if err != nil {
		t.Fatalf("Spawn(first) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run(first) error = %v", err)
	}
	if err := scheduler.Reap(first); err != nil {
		t.Fatalf("Reap(first) error = %v", err)
	}
	second, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, nil })
	if err != nil {
		t.Fatalf("Spawn(second) error = %v", err)
	}
	if first == second {
		t.Fatalf("reused slot ID = %d, generation did not advance", second)
	}
	if _, err := scheduler.Status(first); !errors.Is(err, ErrFiberNotFound) {
		t.Fatalf("Status(stale) error = %v, want ErrFiberNotFound", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run(second) error = %v", err)
	}
	if err := scheduler.Reap(second); err != nil {
		t.Fatalf("Reap(second) error = %v", err)
	}
}

func TestTU30SchedulerCloseCancelsReady(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	wantErr := errors.New("step failed")
	failed, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, wantErr })
	if err != nil {
		t.Fatalf("Spawn(failed) error = %v", err)
	}
	cancelled, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepYield, nil })
	if err != nil {
		t.Fatalf("Spawn(cancelled) error = %v", err)
	}
	scheduler.Close()
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, nil }); !errors.Is(err, ErrSchedulerClosed) {
		t.Fatalf("Spawn(after close) error = %v, want ErrSchedulerClosed", err)
	}
	stats, err := scheduler.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if stats.Cancelled != 2 || stats.Failed != 0 {
		t.Fatalf("Run() stats = %#v, want two cancellations", stats)
	}
	if status, err := scheduler.Status(failed); err != nil || status != StatusCancelled {
		t.Fatalf("Status(failed) = %v, error = %v, want cancelled", status, err)
	}
	if failure := scheduler.Failure(failed); !errors.Is(failure, context.Canceled) {
		t.Fatalf("Failure(cancelled) = %v, want context.Canceled", failure)
	}
	if status, err := scheduler.Status(cancelled); err != nil || status != StatusCancelled {
		t.Fatalf("Status(cancelled) = %v, error = %v, want cancelled", status, err)
	}
	if err := scheduler.Reap(failed); err != nil {
		t.Fatalf("Reap(failed) error = %v", err)
	}
	if err := scheduler.Reap(cancelled); err != nil {
		t.Fatalf("Reap(cancelled) error = %v", err)
	}
}

func TestTU30SchedulerReportsStepFailure(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	wantErr := errors.New("step failed")
	id, err := scheduler.Spawn(func(context.Context) (Step, error) { return StepDone, wantErr })
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	stats, err := scheduler.Run(context.Background(), 0)
	if err != nil || stats.Failed != 1 {
		t.Fatalf("Run() = %#v, error = %v", stats, err)
	}
	if failure := scheduler.Failure(id); !errors.Is(failure, wantErr) {
		t.Fatalf("Failure() = %v, want %v", failure, wantErr)
	}
	if err := scheduler.Reap(id); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}
