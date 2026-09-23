package hatFiber

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestT235WorkerPoolReportsStepWaitAndPanics(t *testing.T) {
	pool, err := NewWorkerPool(WorkerPoolOptions{
		Workers:            1,
		MaxFibersPerWorker: 2,
		QueueCapacity:      2,
		StepsPerTurn:       2,
	})
	if err != nil {
		t.Fatalf("NewWorkerPool() error = %v", err)
	}
	ctx := context.Background()
	waitFuture, err := pool.Submit(ctx, func(context.Context) (Step, error) {
		return StepWait, nil
	})
	if err != nil {
		t.Fatalf("Submit(wait) error = %v", err)
	}
	panicFuture, err := pool.Submit(ctx, func(context.Context) (Step, error) {
		panic("boom")
	})
	if err != nil {
		t.Fatalf("Submit(panic) error = %v", err)
	}
	if err := waitFuture.Wait(ctx); !errors.Is(err, ErrFiberNotParked) {
		t.Fatalf("StepWait future error = %v, want ErrFiberNotParked", err)
	}
	if err := panicFuture.Wait(ctx); err == nil || !strings.Contains(err.Error(), "worker task panic: boom") {
		t.Fatalf("panic future error = %v", err)
	}
	if err := pool.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestT235WorkerPoolCancelsAdmittedAndQueuedTasks(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	pool, err := NewWorkerPool(WorkerPoolOptions{
		Context:            parent,
		Workers:            1,
		MaxFibersPerWorker: 1,
		QueueCapacity:      2,
		StepsPerTurn:       1,
	})
	if err != nil {
		t.Fatalf("NewWorkerPool() error = %v", err)
	}
	started := make(chan struct{})
	first, err := pool.Submit(context.Background(), func(context.Context) (Step, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		return StepYield, nil
	})
	if err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	second, err := pool.Submit(context.Background(), func(context.Context) (Step, error) {
		return StepDone, nil
	})
	if err != nil {
		t.Fatalf("Submit(second) error = %v", err)
	}
	<-started
	cancel()
	if err := first.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("first future error = %v, want context.Canceled", err)
	}
	if err := second.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("second future error = %v, want context.Canceled", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
