package hatPipeline_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestWorkStealingPoolStealsQueuedWorkFromBusyWorker(t *testing.T) {
	pool, err := hatPipeline.NewWorkStealingPool(context.Background(), 2, 32)
	if err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.SubmitTo(context.Background(), 0, func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker 0 did not start the blocking task")
	}

	const queued = 16
	completed := make(chan struct{}, queued)
	for index := 0; index < queued; index++ {
		if err := pool.SubmitTo(context.Background(), 0, func(context.Context) error {
			completed <- struct{}{}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < queued; index++ {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatalf("queued task %d was not stolen", index+1)
		}
	}
	if stats := pool.Stats(); stats.Stolen == 0 {
		t.Fatalf("pool stats = %#v, want at least one stolen task", stats)
	}

	close(release)
	pool.Close()
	if err := pool.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if stats := pool.Stats(); stats.Submitted != queued+1 || stats.Completed != queued+1 {
		t.Fatalf("final pool stats = %#v, want submitted and completed %d", stats, queued+1)
	}
}

func TestWorkStealingPoolDrainsAndRejectsInvalidLifecycle(t *testing.T) {
	if _, err := hatPipeline.NewWorkStealingPool(context.Background(), 0, 1); !errors.Is(err, hatPipeline.ErrWorkStealingPoolInvalid) {
		t.Fatalf("zero workers error = %v", err)
	}
	if _, err := hatPipeline.NewWorkStealingPool(context.Background(), 1, 0); !errors.Is(err, hatPipeline.ErrWorkStealingPoolInvalid) {
		t.Fatalf("zero queue capacity error = %v", err)
	}

	pool, err := hatPipeline.NewWorkStealingPool(nil, 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(context.Background(), nil); !errors.Is(err, hatPipeline.ErrWorkStealingPoolInvalid) {
		t.Fatalf("nil task error = %v", err)
	}
	if err := pool.SubmitTo(context.Background(), 2, func(context.Context) error { return nil }); !errors.Is(err, hatPipeline.ErrWorkStealingPoolInvalid) {
		t.Fatalf("invalid worker error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.Submit(canceled, func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submit error = %v", err)
	}

	var ran atomic.Int32
	for index := 0; index < 8; index++ {
		if err := pool.Submit(context.Background(), func(context.Context) error {
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	pool.Close()
	if err := pool.Submit(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, hatPipeline.ErrWorkStealingPoolClosed) {
		t.Fatalf("submit after close error = %v", err)
	}
	if err := pool.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if got := ran.Load(); got != 8 {
		t.Fatalf("tasks run = %d, want 8", got)
	}
	if err := pool.Wait(); err != nil {
		t.Fatalf("second Wait() error = %v", err)
	}
}

func TestWorkStealingPoolStopsAfterFirstTaskError(t *testing.T) {
	wantErr := errors.New("task failed")
	pool, err := hatPipeline.NewWorkStealingPool(context.Background(), 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.Submit(context.Background(), func(context.Context) error {
		close(started)
		<-release
		return wantErr
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("failing task did not start")
	}
	var ran atomic.Int32
	if err := pool.Submit(context.Background(), func(context.Context) error {
		ran.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	pool.Close()
	if err := pool.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("tasks after failure run = %d, want 0", got)
	}
}
