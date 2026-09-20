package hatFiber

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU31UnbufferedChannelHandoff(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 4})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	channel, err := NewChannel[int](scheduler, 0)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	var received []int
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		value, ok, err := channel.TryReceive()
		if errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitReceive()
		}
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, errors.New("channel closed before value")
		}
		received = append(received, value)
		return StepDone, nil
	}); err != nil {
		t.Fatalf("Spawn(receiver) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := channel.TrySend(42); errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitSend(42)
		} else if err != nil {
			return 0, err
		}
		return StepDone, nil
	}); err != nil {
		t.Fatalf("Spawn(sender) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(received, []int{42}) {
		t.Fatalf("received = %v, want [42]", received)
	}
}

func TestTU31ConditionSemaphoreAndWaitGroup(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 8})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	condition, err := NewCondition(scheduler)
	if err != nil {
		t.Fatalf("NewCondition() error = %v", err)
	}
	semaphore, err := NewSemaphore(scheduler, 0)
	if err != nil {
		t.Fatalf("NewSemaphore() error = %v", err)
	}
	waitGroup, err := NewWaitGroup(scheduler, 1)
	if err != nil {
		t.Fatalf("NewWaitGroup() error = %v", err)
	}
	conditionReady := false
	semaphoreReady := false
	waitGroupReady := false
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if !conditionReady {
			return condition.Wait()
		}
		return StepDone, nil
	}); err != nil {
		t.Fatalf("Spawn(condition waiter) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if semaphore.TryAcquire() {
			semaphoreReady = true
			return StepDone, nil
		}
		return semaphore.WaitAcquire()
	}); err != nil {
		t.Fatalf("Spawn(semaphore waiter) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if waitGroupReady {
			return StepDone, nil
		}
		return waitGroup.Wait()
	}); err != nil {
		t.Fatalf("Spawn(wait-group waiter) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		conditionReady = true
		if !condition.Signal() {
			return 0, errors.New("condition had no waiter")
		}
		if err := semaphore.Release(1); err != nil {
			return 0, err
		}
		waitGroupReady = true
		if err := waitGroup.Done(); err != nil {
			return 0, err
		}
		return StepDone, nil
	}); err != nil {
		t.Fatalf("Spawn(signaler) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !semaphoreReady || !conditionReady || !waitGroupReady {
		t.Fatalf("conditionReady=%v semaphoreReady=%v waitGroupReady=%v", conditionReady, semaphoreReady, waitGroupReady)
	}
}

func TestTU31ChannelCloseWakesWaitersAndBounds(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	channel, err := NewChannel[int](scheduler, 1)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	if err := channel.TrySend(1); err != nil {
		t.Fatalf("TrySend() error = %v", err)
	}
	sender, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if err := channel.TrySend(2); errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitSend(2)
		} else if errors.Is(err, ErrChannelClosed) {
			return StepDone, nil
		} else if err != nil {
			return 0, err
		}
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Spawn(sender) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 1); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if status, err := scheduler.Status(sender); err != nil || status != StatusWaiting {
		t.Fatalf("sender status = %v, error = %v, want waiting", status, err)
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	value, ok, err := channel.TryReceive()
	if err != nil || !ok || value != 1 {
		t.Fatalf("TryReceive() = %d, %v, %v, want buffered value", value, ok, err)
	}
	_, ok, err = channel.TryReceive()
	if err != nil || ok {
		t.Fatalf("TryReceive(after drain) = ok %v, error %v, want closed empty", ok, err)
	}
	if err := scheduler.Reap(sender); err != nil {
		t.Fatalf("Reap(sender) error = %v", err)
	}
}

func TestTU31BufferedChannelRetryOrdering(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 3})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	channel, err := NewChannel[int](scheduler, 1)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	var received []int
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		if len(received) == 2 {
			return StepDone, nil
		}
		value, ok, err := channel.TryReceive()
		if errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitReceive()
		}
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, errors.New("channel closed before both values")
		}
		received = append(received, value)
		return StepYield, nil
	}); err != nil {
		t.Fatalf("Spawn(receiver) error = %v", err)
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		switch len(received) {
		case 0:
			if err := channel.TrySend(1); err != nil {
				return 0, err
			}
			return StepYield, nil
		case 1:
			if err := channel.TrySend(2); errors.Is(err, ErrChannelWouldBlock) {
				return channel.WaitSend(2)
			} else if err != nil {
				return 0, err
			}
			return StepDone, nil
		default:
			return StepDone, nil
		}
	}); err != nil {
		t.Fatalf("Spawn(sender) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(received, []int{1, 2}) {
		t.Fatalf("received = %v, want [1 2]", received)
	}
}

func TestTU31ConditionBroadcastAndParkContract(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 4})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	condition, err := NewCondition(scheduler)
	if err != nil {
		t.Fatalf("NewCondition() error = %v", err)
	}
	if _, err := condition.Wait(); !errors.Is(err, ErrFiberNotRunning) {
		t.Fatalf("Wait() outside callback error = %v, want ErrFiberNotRunning", err)
	}
	ready := false
	woken := 0
	for index := 0; index < 2; index++ {
		if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
			if ready {
				woken++
				return StepDone, nil
			}
			return condition.Wait()
		}); err != nil {
			t.Fatalf("Spawn(waiter %d) error = %v", index, err)
		}
	}
	if _, err := scheduler.Spawn(func(context.Context) (Step, error) {
		ready = true
		if got := condition.Broadcast(); got != 2 {
			return 0, errors.New("condition broadcast did not wake both waiters")
		}
		return StepDone, nil
	}); err != nil {
		t.Fatalf("Spawn(signaler) error = %v", err)
	}
	invalid, err := scheduler.Spawn(func(context.Context) (Step, error) {
		return StepWait, nil
	})
	if err != nil {
		t.Fatalf("Spawn(invalid waiter) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 4); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if failure := scheduler.Failure(invalid); !errors.Is(failure, ErrFiberNotParked) {
		t.Fatalf("invalid StepWait failure = %v, want ErrFiberNotParked", failure)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("drain Run() error = %v", err)
	}
	if woken != 2 {
		t.Fatalf("woken = %d, want 2", woken)
	}
}

func TestTU31CloseWakesReceiverAndCancelSkipsStaleWaiter(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	channel, err := NewChannel[int](scheduler, 0)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	receivedClosed := false
	receiver, err := scheduler.Spawn(func(context.Context) (Step, error) {
		value, ok, err := channel.TryReceive()
		if errors.Is(err, ErrChannelWouldBlock) {
			return channel.WaitReceive()
		}
		if err != nil {
			return 0, err
		}
		if ok {
			return 0, errors.New("received a value from an empty channel")
		}
		_ = value
		receivedClosed = true
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Spawn(receiver) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 1); err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if err := scheduler.Cancel(receiver); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := channel.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if scheduler.Pending() != 0 {
		t.Fatalf("Pending() = %d, want 0 after canceling a waiter", scheduler.Pending())
	}
	if receivedClosed {
		t.Fatal("canceled receiver ran after close")
	}
	if err := scheduler.Reap(receiver); err != nil {
		t.Fatalf("Reap(receiver) error = %v", err)
	}

	resumed, err := scheduler.Spawn(func(context.Context) (Step, error) {
		value, ok, err := channel.TryReceive()
		if err != nil {
			return 0, err
		}
		if ok || value != 0 {
			return 0, errors.New("closed channel returned a value")
		}
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Spawn(closed receiver) error = %v", err)
	}
	if _, err := scheduler.Run(context.Background(), 0); err != nil {
		t.Fatalf("second Run() error = %v", err)
	}
	if err := scheduler.Reap(resumed); err != nil {
		t.Fatalf("Reap(closed receiver) error = %v", err)
	}
}

func TestTU31WaitGroupRejectsIntegerOverflow(t *testing.T) {
	scheduler, err := New(Options{MaxFibers: 1})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	group, err := NewWaitGroup(scheduler, 1)
	if err != nil {
		t.Fatalf("NewWaitGroup() error = %v", err)
	}
	minInt := -int(^uint(0)>>1) - 1
	if err := group.Add(minInt); !errors.Is(err, ErrWaitGroupUnderflow) {
		t.Fatalf("Add(minInt) error = %v, want ErrWaitGroupUnderflow", err)
	}
	if group.Count() != 1 {
		t.Fatalf("Count() = %d after rejected update, want 1", group.Count())
	}
}
