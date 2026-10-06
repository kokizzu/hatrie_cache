package hatPipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncBatcherRegistryRegistersSnapshotsSortedAndFlushes(t *testing.T) {
	var firstItems atomic.Int64
	var secondItems atomic.Int64
	first, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      8,
		MaxBatchSize:  8,
		FlushInterval: time.Hour,
		Handler: func(context.Context, []int) error {
			firstItems.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      8,
		MaxBatchSize:  8,
		FlushInterval: time.Hour,
		Handler: func(context.Context, []int) error {
			secondItems.Add(1)
			return nil
		},
	})
	if err != nil {
		first.Close(context.Background())
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	defer second.Close(context.Background())

	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{MaxQueues: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("zeta", second); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", first); err != nil {
		t.Fatal(err)
	}
	if err := first.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := second.Submit(context.Background(), 2); err != nil {
		t.Fatal(err)
	}

	snapshot := registry.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(snapshot))
	}
	if snapshot[0].Name != "alpha" || snapshot[1].Name != "zeta" {
		t.Fatalf("snapshot names = %q, %q, want alpha, zeta", snapshot[0].Name, snapshot[1].Name)
	}
	if snapshot[0].Stats.Pending != 1 || snapshot[1].Stats.Pending != 1 {
		t.Fatalf("snapshot pending = %d, %d, want 1, 1", snapshot[0].Stats.Pending, snapshot[1].Stats.Pending)
	}

	if err := registry.Flush(context.Background(), "alpha"); err != nil {
		t.Fatal(err)
	}
	if got := firstItems.Load(); got != 1 {
		t.Fatalf("first handler calls = %d, want 1", got)
	}
	if err := registry.FlushAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := secondItems.Load(); got != 1 {
		t.Fatalf("second handler calls = %d, want 1", got)
	}
	for _, queue := range registry.Snapshot() {
		if queue.Stats.Pending != 0 {
			t.Fatalf("queue %q pending = %d after flush, want 0", queue.Name, queue.Stats.Pending)
		}
	}
}

func TestAsyncBatcherRegistryRejectsInvalidRegistrationAndBounds(t *testing.T) {
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{MaxQueues: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("", nil); !errors.Is(err, ErrAsyncBatcherRegistryNameRequired) {
		t.Fatalf("empty name error = %v, want name required", err)
	}
	if err := registry.Register("alpha", nil); !errors.Is(err, ErrAsyncBatcherRegistryQueueRequired) {
		t.Fatalf("nil queue error = %v, want queue required", err)
	}
	if err := registry.Register("alpha", &registryTestControl{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", &registryTestControl{}); !errors.Is(err, ErrAsyncBatcherRegistryQueueExists) {
		t.Fatalf("duplicate error = %v, want queue exists", err)
	}
	if err := registry.Register("beta", &registryTestControl{}); !errors.Is(err, ErrAsyncBatcherRegistryMaxQueuesReached) {
		t.Fatalf("capacity error = %v, want max queues reached", err)
	}

	if _, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{MaxQueues: -1}); !errors.Is(err, ErrAsyncBatcherRegistryMaxQueuesInvalid) {
		t.Fatalf("negative max queues error = %v, want invalid max queues", err)
	}
}

func TestAsyncBatcherRegistryUnregisterLeavesQueueOwnedByCaller(t *testing.T) {
	queue := &registryTestControl{}
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", queue); err != nil {
		t.Fatal(err)
	}
	removed, err := registry.Unregister("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if removed != queue {
		t.Fatal("unregister returned a different queue")
	}
	if _, err := registry.Unregister("alpha"); !errors.Is(err, ErrAsyncBatcherRegistryQueueMissing) {
		t.Fatalf("missing unregister error = %v, want queue missing", err)
	}
	if len(registry.Snapshot()) != 0 {
		t.Fatal("unregistered queue remains in snapshot")
	}
	if queue.closeCalls.Load() != 0 {
		t.Fatal("unregister closed a queue owned by the caller")
	}
}

func TestAsyncBatcherRegistryCloseDrainsAndRejectsFurtherOperations(t *testing.T) {
	var handled atomic.Int64
	queue, err := NewAsyncBatcher(AsyncBatcherOptions[int]{
		Capacity:      8,
		MaxBatchSize:  8,
		FlushInterval: time.Hour,
		Handler: func(_ context.Context, values []int) error {
			handled.Add(int64(len(values)))
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{})
	if err != nil {
		queue.Close(context.Background())
		t.Fatal(err)
	}
	if err := registry.Register("alpha", queue); err != nil {
		queue.Close(context.Background())
		t.Fatal(err)
	}
	if err := queue.Submit(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("handled values = %d, want 1", got)
	}
	if err := registry.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("beta", &registryTestControl{}); !errors.Is(err, ErrAsyncBatcherRegistryClosed) {
		t.Fatalf("register after close error = %v, want closed", err)
	}
	if err := registry.Flush(context.Background(), "alpha"); !errors.Is(err, ErrAsyncBatcherRegistryClosed) {
		t.Fatalf("flush after close error = %v, want closed", err)
	}
}

func TestAsyncBatcherRegistryFlushAllAggregatesErrorsAndContinues(t *testing.T) {
	firstErr := errors.New("first flush failed")
	secondErr := errors.New("second flush failed")
	first := &registryTestControl{flushErr: firstErr}
	second := &registryTestControl{flushErr: secondErr}
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", first); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("beta", second); err != nil {
		t.Fatal(err)
	}
	err = registry.FlushAll(context.Background())
	if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
		t.Fatalf("flush all error = %v, want both handler errors", err)
	}
	if first.flushCalls.Load() != 1 || second.flushCalls.Load() != 1 {
		t.Fatalf("flush calls = %d, %d, want 1, 1", first.flushCalls.Load(), second.flushCalls.Load())
	}
	if !strings.Contains(err.Error(), "alpha") || !strings.Contains(err.Error(), "beta") {
		t.Fatalf("flush all error = %v, want queue names", err)
	}
}

func TestAsyncBatcherRegistryFlushPropagatesContextCancellation(t *testing.T) {
	queue := &registryTestControl{flushWait: make(chan struct{})}
	registry, err := NewAsyncBatcherRegistry(AsyncBatcherRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("alpha", queue); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := registry.Flush(ctx, "alpha"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush cancellation error = %v, want deadline exceeded", err)
	}
	close(queue.flushWait)
}

type registryTestControl struct {
	mu         sync.Mutex
	stats      AsyncBatcherStats
	flushErr   error
	closeErr   error
	flushWait  chan struct{}
	flushCalls atomic.Int64
	closeCalls atomic.Int64
}

func (control *registryTestControl) Flush(ctx context.Context) error {
	control.flushCalls.Add(1)
	if control.flushWait != nil {
		select {
		case <-control.flushWait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return control.flushErr
}

func (control *registryTestControl) Close(context.Context) error {
	control.closeCalls.Add(1)
	return control.closeErr
}

func (control *registryTestControl) Stats() AsyncBatcherStats {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.stats
}
