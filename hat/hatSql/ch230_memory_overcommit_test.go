package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCH230MemoryOvercommitQueueWaitsAndReleases(t *testing.T) {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 64, MaxWaiters: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Acquire(context.Background(), 64); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- queue.Acquire(ctx, 32)
	}()
	waitForCH230Waiters(t, queue, 1)
	select {
	case err := <-done:
		t.Fatalf("overcommit waiter returned before release: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	queue.Release(64)
	if err := <-done; err != nil {
		t.Fatalf("overcommit waiter failed after release: %v", err)
	}
	stats := queue.Snapshot()
	if stats.UsedBytes != 32 || stats.Waiters != 0 || stats.Grants != 2 {
		t.Fatalf("queue stats after grant = %#v, want used=32 waiters=0 grants=2", stats)
	}
	queue.Release(32)
	if stats = queue.Snapshot(); stats.UsedBytes != 0 {
		t.Fatalf("queue used bytes after final release = %d, want zero", stats.UsedBytes)
	}
}

func TestCH230MemoryOvercommitQueueCancellationRemovesWaiter(t *testing.T) {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 64, MaxWaiters: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Acquire(context.Background(), 64); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- queue.Acquire(ctx, 32)
	}()
	waitForCH230Waiters(t, queue, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v, want context canceled", err)
	}
	stats := queue.Snapshot()
	if stats.UsedBytes != 64 || stats.Waiters != 0 || stats.Cancellations != 1 {
		t.Fatalf("queue stats after cancellation = %#v, want used=64 waiters=0 cancellations=1", stats)
	}
	queue.Release(64)
}

func TestCH230MemoryOvercommitQueueBoundsWaiters(t *testing.T) {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 64, MaxWaiters: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Acquire(context.Background(), 64); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- queue.Acquire(ctx, 32)
	}()
	waitForCH230Waiters(t, queue, 1)
	if err := queue.Acquire(context.Background(), 32); !errors.Is(err, ErrSQLMemoryOvercommitQueueFull) {
		t.Fatalf("second waiter error = %v, want queue full", err)
	}
	queue.Release(64)
	if err := <-done; err != nil {
		t.Fatalf("first waiter failed after release: %v", err)
	}
	queue.Release(32)
}

func TestCH230ExecutionControlUsesOvercommitQueue(t *testing.T) {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MemoryOvercommit: queue})
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if err := control.observeOperatorMemory("GROUP BY", 48); err != nil {
		t.Fatalf("initial operator reservation: %v", err)
	}
	if got := queue.Snapshot().UsedBytes; got != 48 {
		t.Fatalf("queue bytes after initial reservation = %d, want 48", got)
	}
	if err := control.observeOperatorMemory("GROUP BY", 16); err != nil {
		t.Fatalf("operator shrink: %v", err)
	}
	if got := queue.Snapshot().UsedBytes; got != 16 {
		t.Fatalf("queue bytes after operator shrink = %d, want 16", got)
	}
	control.releaseOperatorMemory("GROUP BY")
	if got := queue.Snapshot().UsedBytes; got != 0 {
		t.Fatalf("queue bytes after control release = %d, want zero", got)
	}
}

func TestCH230SQLQueryUsesOvercommitQueue(t *testing.T) {
	queue, err := NewSQLMemoryOvercommitQueue(SQLMemoryOvercommitOptions{LimitBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	query := `FROM VALUES ('b', 2), ('a', 1) AS src(group_id, value)
SELECT src.group_id, SUM(src.value) AS total
GROUP BY src.group_id
ORDER BY src.group_id`
	_, err = ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{MemoryOvercommit: queue})
	if !errors.Is(err, ErrSQLMemoryOvercommitRequestTooLarge) {
		t.Fatalf("query error = %v, want overcommit request-too-large", err)
	}
	if stats := queue.Snapshot(); stats.UsedBytes != 0 || stats.Waiters != 0 {
		t.Fatalf("queue stats after rejected query = %#v, want no retained reservation", stats)
	}
}

func waitForCH230Waiters(t *testing.T, queue *SQLMemoryOvercommitQueue, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if queue.Snapshot().Waiters == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("queue waiters did not reach %d; stats = %#v", want, queue.Snapshot())
		case <-ticker.C:
		}
	}
}
