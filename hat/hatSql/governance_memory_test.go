package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestNamespaceQueryMemoryBudgetQueuesAndReleases(t *testing.T) {
	budget := newNamespaceQueryMemoryBudget(100, 1)
	if err := budget.acquire(context.Background(), 60); err != nil {
		t.Fatalf("initial acquire() error = %v", err)
	}

	queued := make(chan error, 1)
	go func() { queued <- budget.acquire(context.Background(), 60) }()
	waitForNamespaceMemoryWaiters(t, budget, 1)

	if err := budget.acquire(context.Background(), 1); !errors.Is(err, ErrNamespaceQueryQueueFull) {
		t.Fatalf("third acquire() error = %v, want %v", err, ErrNamespaceQueryQueueFull)
	}

	budget.release(60)
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("queued acquire() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued memory acquire was not released")
	}
	budget.release(60)
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("memory budget retained %d bytes, want 0", got)
	}
}

func TestNamespaceQueryMemoryBudgetCancellationDoesNotLeakReservation(t *testing.T) {
	budget := newNamespaceQueryMemoryBudget(100, 0)
	if err := budget.acquire(context.Background(), 80); err != nil {
		t.Fatalf("initial acquire() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := budget.acquire(ctx, 40); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled acquire() error = %v, want deadline exceeded", err)
	}
	budget.release(80)
	if got := budget.usedBytes(); got != 0 {
		t.Fatalf("canceled memory acquire retained %d bytes, want 0", got)
	}
}

func TestNamespaceQueryGovernorMemoryBudgetBlocksBeforeExecution(t *testing.T) {
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{
		MaxConcurrentQueries: 2,
		MaxQueuedQueries:     1,
		MaxMemoryBytes:       100,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		calls.Add(1)
		close(started)
		<-release
		return []SQLRow{{"id": int64(1)}}, nil
	})
	firstResult := make(chan error, 1)
	go func() {
		_, err := governor.Execute(context.Background(), "tenant", "SELECT * FROM CACHE('items')", resolver, nil, SQLQueryOptions{MemoryReservationBytes: 80})
		firstResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first memory-budgeted query did not reach its resolver")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = governor.Execute(ctx, "tenant", "SELECT * FROM CACHE('items')", resolver, nil, SQLQueryOptions{MemoryReservationBytes: 80})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting memory-budgeted query error = %v, want deadline exceeded", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver calls while memory was full = %d, want 1", got)
	}

	close(release)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first memory-budgeted query error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first memory-budgeted query did not finish")
	}
}

func TestNamespaceQueryGovernorRejectsOversizedMemoryReservation(t *testing.T) {
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxMemoryBytes: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		called = true
		return nil, nil
	})
	_, err = governor.Execute(context.Background(), "tenant", "SELECT * FROM CACHE('items')", resolver, nil, SQLQueryOptions{MemoryReservationBytes: 101})
	if !errors.Is(err, ErrNamespaceQueryMemoryBudgetExceeded) {
		t.Fatalf("oversized reservation error = %v, want %v", err, ErrNamespaceQueryMemoryBudgetExceeded)
	}
	if called {
		t.Fatal("resolver ran for an oversized memory reservation")
	}
}

func TestNamespaceResourceMemoryReservationEstimate(t *testing.T) {
	if got := namespaceQueryMemoryReservation(SQLQueryOptions{
		MaxJoinBytes:   100,
		MaxResultBytes: 200,
		MaxSortBytes:   150,
	}, 1_000); got != 200 {
		t.Fatalf("derived memory reservation = %d, want 200", got)
	}
	if got := namespaceQueryMemoryReservation(SQLQueryOptions{MemoryReservationBytes: 300, MaxResultBytes: 200}, 1_000); got != 300 {
		t.Fatalf("explicit memory reservation = %d, want 300", got)
	}
	if got := namespaceQueryMemoryReservation(SQLQueryOptions{}, 1_000); got != 1_000 {
		t.Fatalf("unbounded memory reservation = %d, want 1000", got)
	}
	limits := NamespaceResourceLimits{MaxMemoryBytes: 1_000, MaxGroupBytes: 200}
	options := limits.Apply(SQLQueryOptions{MaxGroupBytes: 500})
	if options.MaxGroupBytes != 200 || options.MemoryReservationBytes != 200 {
		t.Fatalf("clamped memory reservation = %#v, want group=200 reservation=200", options)
	}
}

func TestNamespaceQueryGovernorRejectsNegativeMemoryLimits(t *testing.T) {
	if _, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxMemoryBytes: -1}, nil); err == nil {
		t.Fatal("negative MaxMemoryBytes was accepted")
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1) AS item(value) SELECT value", nil, SQLQueryOptions{MemoryReservationBytes: -1}); err == nil {
		t.Fatal("negative MemoryReservationBytes was accepted")
	}
}

func waitForNamespaceMemoryWaiters(t *testing.T, budget *namespaceQueryMemoryBudget, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		budget.mu.Lock()
		count := len(budget.waiters)
		budget.mu.Unlock()
		if count >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiting memory budget waiters = fewer than %d", want)
}
