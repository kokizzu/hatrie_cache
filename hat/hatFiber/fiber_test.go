package hatFiber_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatFiber"
)

func TestSchedulerResumesYieldedFibersFairly(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     8,
		QueueCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	order := make([]byte, 0, 6)
	var a hatFiber.Step
	aRuns := 0
	a = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		order = append(order, 'a')
		aRuns++
		if aRuns == 1 {
			close(started)
			<-release
		}
		if aRuns == 3 {
			return nil, nil
		}
		return a, nil
	}
	var b hatFiber.Step
	bRuns := 0
	b = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		order = append(order, 'b')
		bRuns++
		if bRuns == 3 {
			return nil, nil
		}
		return b, nil
	}

	if _, err := scheduler.Spawn(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first fiber did not start")
	}
	if _, err := scheduler.Spawn(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	close(release)

	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, want := order, []byte{'a', 'b', 'a', 'b', 'a', 'b'}; !reflect.DeepEqual(got, want) {
		t.Fatalf("execution order = %q, want %q", got, want)
	}
}

func TestSchedulerBoundsFibersAndDrainsOnClose(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     1,
		QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	step := func(ctx hatFiber.Context) (hatFiber.Step, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		runs.Add(1)
		close(started)
		<-release
		return nil, nil
	}
	if _, err := scheduler.Spawn(context.Background(), step); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("bounded fiber did not start")
	}
	if _, err := scheduler.Spawn(context.Background(), step); !errors.Is(err, hatFiber.ErrSchedulerFull) {
		t.Fatalf("second Spawn() error = %v, want %v", err, hatFiber.ErrSchedulerFull)
	}
	scheduler.Close()
	close(release)
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("runs = %d, want 1", got)
	}
	if _, err := scheduler.Spawn(context.Background(), step); !errors.Is(err, hatFiber.ErrSchedulerClosed) {
		t.Fatalf("Spawn() after close error = %v, want %v", err, hatFiber.ErrSchedulerClosed)
	}
}

func TestSchedulerCancellationAndErrorPropagation(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     4,
		QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := scheduler.Spawn(canceled, func(hatFiber.Context) (hatFiber.Step, error) {
		return nil, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Spawn(canceled) error = %v, want %v", err, context.Canceled)
	}

	wantErr := errors.New("fiber failed")
	if _, err := scheduler.Spawn(context.Background(), func(ctx hatFiber.Context) (hatFiber.Step, error) {
		return nil, wantErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}
}

func TestSchedulerWaitIdleReusesWorkersAndSlots(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     2,
		QueueCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Wait()

	var runs atomic.Int32
	step := func(hatFiber.Context) (hatFiber.Step, error) {
		runs.Add(1)
		return nil, nil
	}
	for range 2 {
		if _, err := scheduler.Spawn(context.Background(), step); err != nil {
			t.Fatal(err)
		}
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if stats := scheduler.Stats(); stats.Active != 0 || stats.Completed != 2 {
		t.Fatalf("first batch stats = %#v", stats)
	}
	if _, err := scheduler.Spawn(context.Background(), step); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 3 {
		t.Fatalf("runs = %d, want 3", got)
	}
}
