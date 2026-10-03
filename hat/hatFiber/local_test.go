package hatFiber_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatFiber"
)

func TestT032LocalIsolationAndReuse(t *testing.T) {
	local := new(hatFiber.Local[int])
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     1,
		QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	var firstContext hatFiber.Context
	var firstValue atomic.Int32
	first := func(ctx hatFiber.Context) (hatFiber.Step, error) {
		firstContext = ctx
		if _, ok := local.Get(ctx); ok {
			return nil, errors.New("new fiber inherited local state")
		}
		if err := local.Set(ctx, 41); err != nil {
			return nil, err
		}
		value, ok := local.Get(ctx)
		if !ok || value != 41 {
			return nil, errors.New("local value was not readable by its fiber")
		}
		return func(ctx hatFiber.Context) (hatFiber.Step, error) {
			value, ok := local.Get(ctx)
			if !ok || value != 41 {
				return nil, errors.New("local value did not survive a continuation")
			}
			firstValue.Store(int32(value))
			return nil, nil
		}, nil
	}
	if _, err := scheduler.Spawn(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := firstValue.Load(); got != 41 {
		t.Fatalf("first value = %d, want 41", got)
	}
	if _, ok := local.Get(firstContext); ok {
		t.Fatal("completed fiber retained local state through its old context")
	}

	var secondInitial atomic.Int32
	second := func(ctx hatFiber.Context) (hatFiber.Step, error) {
		if _, ok := local.Get(ctx); ok {
			secondInitial.Store(1)
		}
		if err := local.Set(ctx, 99); err != nil {
			return nil, err
		}
		if !local.Delete(ctx) {
			return nil, errors.New("Delete reported no value")
		}
		if _, ok := local.Get(ctx); ok {
			return nil, errors.New("Delete did not remove local state")
		}
		return nil, nil
	}
	if _, err := scheduler.Spawn(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := secondInitial.Load(); got != 0 {
		t.Fatalf("second fiber inherited state marker %d", got)
	}

	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestT032LocalCancellationClearsState(t *testing.T) {
	local := new(hatFiber.Local[string])
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     1,
		QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	condition := new(hatFiber.Condition)
	var parkedContext hatFiber.Context
	var parked hatFiber.Step
	parked = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		parkedContext = ctx
		if err := local.Set(ctx, "cancelled"); err != nil {
			return nil, err
		}
		return condition.Wait(ctx, parked)
	}
	if _, err := scheduler.Spawn(context.Background(), parked); err != nil {
		t.Fatal(err)
	}
	waitForT032Waiters(t, condition.WaiterCount, 1)
	scheduler.Cancel()
	if err := scheduler.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v, want %v", err, context.Canceled)
	}
	if _, ok := local.Get(parkedContext); ok {
		t.Fatal("canceled fiber retained local state through its old context")
	}
}

func TestT032LocalConcurrentFiberIsolation(t *testing.T) {
	local := new(hatFiber.Local[int])
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       2,
		MaxFibers:     2,
		QueueCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var first, second atomic.Int32
	spawn := func(want int, got *atomic.Int32) hatFiber.Step {
		return func(ctx hatFiber.Context) (hatFiber.Step, error) {
			if err := local.Set(ctx, want); err != nil {
				return nil, err
			}
			return func(ctx hatFiber.Context) (hatFiber.Step, error) {
				value, ok := local.Get(ctx)
				if !ok || value != want {
					return nil, errors.New("fiber-local values crossed fibers")
				}
				got.Store(int32(value))
				return nil, nil
			}, nil
		}
	}
	if _, err := scheduler.Spawn(context.Background(), spawn(11, &first)); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Spawn(context.Background(), spawn(22, &second)); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := first.Load(); got != 11 {
		t.Fatalf("first fiber value = %d, want 11", got)
	}
	if got := second.Load(); got != 22 {
		t.Fatalf("second fiber value = %d, want 22", got)
	}
	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestT032LocalValidation(t *testing.T) {
	var local hatFiber.Local[int]
	if err := local.Set(hatFiber.Context{}, 1); !errors.Is(err, hatFiber.ErrLocalInvalid) {
		t.Fatalf("invalid Set() error = %v, want %v", err, hatFiber.ErrLocalInvalid)
	}
	if _, ok := local.Get(hatFiber.Context{}); ok {
		t.Fatal("invalid context unexpectedly returned a value")
	}
	if local.Delete(hatFiber.Context{}) {
		t.Fatal("invalid Delete() reported a value")
	}
	var nilLocal *hatFiber.Local[int]
	if err := nilLocal.Set(hatFiber.Context{}, 1); !errors.Is(err, hatFiber.ErrLocalInvalid) {
		t.Fatalf("nil local error = %v, want %v", err, hatFiber.ErrLocalInvalid)
	}
}

func waitForT032Waiters(t *testing.T, count func() int, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if got := count(); got == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("waiter count did not reach %d; got %d", want, count())
		case <-ticker.C:
		}
	}
}
