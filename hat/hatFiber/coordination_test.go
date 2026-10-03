package hatFiber_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatFiber"
)

func TestT031ChannelBackpressureAndClose(t *testing.T) {
	channel, err := hatFiber.NewChannel[int](1)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.Send(1); err != nil {
		t.Fatal(err)
	}
	if err := channel.Send(2); !errors.Is(err, hatFiber.ErrChannelWouldBlock) {
		t.Fatalf("full Send() error = %v, want %v", err, hatFiber.ErrChannelWouldBlock)
	}
	value, ok, err := channel.Receive()
	if err != nil || !ok || value != 1 {
		t.Fatalf("Receive() = (%d, %t, %v), want (1, true, nil)", value, ok, err)
	}
	channel.Close()
	channel.Close()
	if err := channel.Send(3); !errors.Is(err, hatFiber.ErrChannelClosed) {
		t.Fatalf("closed Send() error = %v, want %v", err, hatFiber.ErrChannelClosed)
	}
	value, ok, err = channel.Receive()
	if err != nil || ok || value != 0 {
		t.Fatalf("drained Receive() = (%d, %t, %v), want (0, false, nil)", value, ok, err)
	}
}

func TestT031ChannelWaitDoesNotOccupyWorker(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     4,
		QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	channel, err := hatFiber.NewChannel[int](1)
	if err != nil {
		t.Fatal(err)
	}
	var received atomic.Int32
	var probe atomic.Int32
	var consumer hatFiber.Step
	consumer = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		result, err := channel.ReceiveStep(ctx, consumer)
		if err != nil {
			return nil, err
		}
		if result.Blocked {
			return nil, nil
		}
		if !result.OK {
			return nil, errors.New("channel closed before the value arrived")
		}
		received.Store(int32(result.Value))
		return nil, nil
	}
	if _, err := scheduler.Spawn(context.Background(), consumer); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Spawn(context.Background(), func(hatrieCtx hatFiber.Context) (hatFiber.Step, error) {
		probe.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := channel.Send(42); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := received.Load(); got != 42 {
		t.Fatalf("received = %d, want 42", got)
	}
	if got := probe.Load(); got != 1 {
		t.Fatalf("probe runs = %d, want 1", got)
	}
	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestT031ConditionNotifyAllAndCancellation(t *testing.T) {
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     4,
		QueueCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	condition := &hatFiber.Condition{}
	var resumed atomic.Int32
	for range 2 {
		resumedOnce := false
		var waiter hatFiber.Step
		waiter = func(ctx hatFiber.Context) (hatFiber.Step, error) {
			if resumedOnce {
				resumed.Add(1)
				return nil, nil
			}
			resumedOnce = true
			return condition.Wait(ctx, waiter)
		}
		if _, err := scheduler.Spawn(context.Background(), waiter); err != nil {
			t.Fatal(err)
		}
	}
	waitForT031Waiters(t, condition.WaiterCount, 2)
	condition.NotifyAll()
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := resumed.Load(); got != 2 {
		t.Fatalf("resumed = %d, want 2", got)
	}

	cancelScheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     1,
		QueueCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelCondition := &hatFiber.Condition{}
	var parked hatFiber.Step
	parked = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		return cancelCondition.Wait(ctx, parked)
	}
	if _, err := cancelScheduler.Spawn(context.Background(), parked); err != nil {
		t.Fatal(err)
	}
	waitForT031Waiters(t, cancelCondition.WaiterCount, 1)
	cancelScheduler.Cancel()
	if err := cancelScheduler.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v, want %v", err, context.Canceled)
	}
}

func TestT031SemaphoreAndWaitGroup(t *testing.T) {
	semaphore, err := hatFiber.NewSemaphore(1)
	if err != nil {
		t.Fatal(err)
	}
	if !semaphore.TryAcquire() {
		t.Fatal("initial semaphore acquire failed")
	}
	scheduler, err := hatFiber.NewScheduler(context.Background(), hatFiber.Options{
		Workers:       1,
		MaxFibers:     2,
		QueueCapacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	var acquired atomic.Int32
	var acquire hatFiber.Step
	acquire = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		_, ready, err := semaphore.AcquireStep(ctx, acquire)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, nil
		}
		acquired.Add(1)
		return nil, nil
	}
	if _, err := scheduler.Spawn(context.Background(), acquire); err != nil {
		t.Fatal(err)
	}
	waitForT031Waiters(t, semaphore.WaiterCount, 1)
	if err := semaphore.Release(); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := acquired.Load(); got != 1 {
		t.Fatalf("acquired = %d, want 1", got)
	}

	group := &hatFiber.WaitGroup{}
	if err := group.Add(1); err != nil {
		t.Fatal(err)
	}
	var waited atomic.Int32
	var waitStep hatFiber.Step
	waitStep = func(ctx hatFiber.Context) (hatFiber.Step, error) {
		_, ready, err := group.WaitStep(ctx, waitStep)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, nil
		}
		waited.Add(1)
		return nil, nil
	}
	if _, err := scheduler.Spawn(context.Background(), waitStep); err != nil {
		t.Fatal(err)
	}
	waitForT031Waiters(t, group.WaiterCount, 1)
	if err := group.Done(); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.WaitIdle(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := waited.Load(); got != 1 {
		t.Fatalf("waited = %d, want 1", got)
	}
	scheduler.Close()
	if err := scheduler.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestT031CoordinationValidation(t *testing.T) {
	if _, err := hatFiber.NewChannel[int](-1); !errors.Is(err, hatFiber.ErrCoordinationInvalid) {
		t.Fatalf("negative channel error = %v", err)
	}
	if _, err := hatFiber.NewChannel[int](0); !errors.Is(err, hatFiber.ErrCoordinationInvalid) {
		t.Fatalf("zero channel error = %v", err)
	}
	if _, err := hatFiber.NewSemaphore(-1); !errors.Is(err, hatFiber.ErrCoordinationInvalid) {
		t.Fatalf("negative semaphore error = %v", err)
	}
	group := &hatFiber.WaitGroup{}
	if err := group.Add(-1); !errors.Is(err, hatFiber.ErrWaitGroupNegative) {
		t.Fatalf("negative wait group error = %v", err)
	}
}

func waitForT031Waiters(t *testing.T, count func() int, want int) {
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
