package hatFiber

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestT235WorkerPoolRunsYieldingTasks(t *testing.T) {
	pool, err := NewWorkerPool(WorkerPoolOptions{
		Workers:            2,
		MaxFibersPerWorker: 4,
		QueueCapacity:      4,
		StepsPerTurn:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Int32
	futures := make([]*Future, 0, 8)
	for task := 0; task < 8; task++ {
		runs := 0
		future, err := pool.Submit(context.Background(), func(context.Context) (Step, error) {
			runs++
			if runs < 3 {
				return StepYield, nil
			}
			completed.Add(1)
			return StepDone, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		futures = append(futures, future)
	}
	for _, future := range futures {
		if err := future.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := completed.Load(), int32(len(futures)); got != want {
		t.Fatalf("completed = %d, want %d", got, want)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestT235WorkerPoolPropagatesErrorsAndCloses(t *testing.T) {
	pool, err := NewWorkerPool(WorkerPoolOptions{Workers: 1, MaxFibersPerWorker: 2})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("task failed")
	future, err := pool.Submit(context.Background(), func(context.Context) (Step, error) {
		return StepDone, wantErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := future.Wait(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("future error = %v, want %v", err, wantErr)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.Submit(cancelled, func(context.Context) (Step, error) { return StepDone, nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled submit = %v, want %v", err, context.Canceled)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Submit(context.Background(), func(context.Context) (Step, error) { return StepDone, nil }); !errors.Is(err, ErrWorkerPoolClosed) {
		t.Fatalf("submit after close = %v, want %v", err, ErrWorkerPoolClosed)
	}
}

func TestT235WorkerPoolRejectsInvalidOptions(t *testing.T) {
	for _, options := range []WorkerPoolOptions{
		{Workers: -1},
		{MaxFibersPerWorker: -1},
		{QueueCapacity: -1},
		{StepsPerTurn: -1},
	} {
		if _, err := NewWorkerPool(options); !errors.Is(err, ErrWorkerPoolInvalid) {
			t.Fatalf("NewWorkerPool(%+v) = %v, want %v", options, err, ErrWorkerPoolInvalid)
		}
	}
}
