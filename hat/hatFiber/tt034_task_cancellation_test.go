package hatFiber

import (
	"context"
	"errors"
	"testing"
)

func TestTT034SchedulerCancellationTokenAndDrain(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatal(err)
	}
	if scheduler.Context() == nil {
		t.Fatal("Context() returned nil")
	}
	if scheduler.DrainState() != DrainStateOpen {
		t.Fatalf("initial drain state = %v, want open", scheduler.DrainState())
	}

	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		t.Fatal("canceled fiber callback ran during drain")
		return StepDone, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Close()
	if !errors.Is(scheduler.Context().Err(), context.Canceled) {
		t.Fatalf("scheduler context error = %v, want context.Canceled", scheduler.Context().Err())
	}
	if scheduler.DrainState() != DrainStateRequested {
		t.Fatalf("post-close drain state = %v, want requested", scheduler.DrainState())
	}

	stats, err := scheduler.Drain(context.Background())
	if err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if stats.Cancelled != 1 || stats.Remaining != 0 {
		t.Fatalf("Drain() stats = %#v, want one canceled and empty queue", stats)
	}
	if scheduler.DrainState() != DrainStateComplete {
		t.Fatalf("completed drain state = %v, want complete", scheduler.DrainState())
	}
	if status, err := scheduler.Status(identifier); err != nil || status != StatusCancelled {
		t.Fatalf("canceled status = %v, %v", status, err)
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}

func TestTT034DrainCancelsWaitingFibersWithoutResumingCallbacks(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatal(err)
	}
	condition, err := NewCondition(scheduler)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		calls++
		return condition.Wait()
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if calls != 1 || condition.Pending() != 1 {
		t.Fatalf("waiting setup calls=%d pending=%d, want 1/1", calls, condition.Pending())
	}

	scheduler.Close()
	if _, err := scheduler.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("drain resumed waiting callback %d times", calls-1)
	}
	if status, err := scheduler.Status(identifier); err != nil || status != StatusCancelled {
		t.Fatalf("waiting fiber status = %v, %v", status, err)
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
	if condition.Pending() != 1 {
		t.Fatalf("condition pending = %d, want stale waiter retained until signal", condition.Pending())
	}
}

func TestTT034DrainHonorsCallerCancellationAndCanResume(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatal(err)
	}
	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		t.Fatal("canceled fiber callback ran during canceled drain")
		return StepDone, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scheduler.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Drain() error = %v, want context.Canceled", err)
	}
	if scheduler.DrainState() != DrainStateRequested || scheduler.Pending() != 1 {
		t.Fatalf("canceled drain state=%v pending=%d, want requested/1", scheduler.DrainState(), scheduler.Pending())
	}
	if _, err := scheduler.Drain(context.Background()); err != nil {
		t.Fatalf("resumed Drain() error = %v", err)
	}
	if scheduler.DrainState() != DrainStateComplete || scheduler.Pending() != 0 {
		t.Fatalf("resumed drain state=%v pending=%d, want complete/0", scheduler.DrainState(), scheduler.Pending())
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}

func TestTT034CloseFromCallbackCancelsCurrentFiber(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	identifier, err := scheduler.Spawn(func(context.Context) (Step, error) {
		calls++
		scheduler.Close()
		return StepYield, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := scheduler.Run(context.Background(), 0)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if calls != 1 || stats.Cancelled != 1 || scheduler.Pending() != 0 {
		t.Fatalf("callback close calls=%d stats=%#v pending=%d, want 1/one canceled/0", calls, stats, scheduler.Pending())
	}
	if _, err := scheduler.Drain(context.Background()); err != nil {
		t.Fatalf("Drain() error = %v", err)
	}
	if err := scheduler.Reap(identifier); err != nil {
		t.Fatalf("Reap() error = %v", err)
	}
}
