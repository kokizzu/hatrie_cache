package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTT049RowLocksSerializeSameKey(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{ShardCount: 4, MaxKeys: 8})
	first, err := manager.Acquire(context.Background(), "row:1")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	acquired := make(chan *SQLRowLockLease, 1)
	go func() {
		lease, acquireErr := manager.Acquire(context.Background(), "row:1")
		if acquireErr == nil {
			acquired <- lease
		}
	}()
	select {
	case lease := <-acquired:
		lease.Release()
		t.Fatal("second lease acquired while first lease was held")
	case <-time.After(20 * time.Millisecond):
	}

	if !first.Release() {
		t.Fatal("first lease did not release")
	}
	select {
	case second := <-acquired:
		if !second.Release() {
			t.Fatal("second lease did not release")
		}
	case <-time.After(time.Second):
		t.Fatal("second lease did not acquire after release")
	}
}

func TestTT049RowLocksTryAcquireAndCapacity(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{MaxKeys: 1})
	first, err := manager.TryAcquire("row:1")
	if err != nil || first == nil {
		t.Fatalf("first TryAcquire() = %v, %v", first, err)
	}
	defer first.Release()
	if second, err := manager.TryAcquire("row:1"); err != nil || second != nil {
		t.Fatalf("contended TryAcquire() = %v, %v, want no lease", second, err)
	}
	if _, err := manager.TryAcquire("row:2"); !errors.Is(err, ErrSQLRowLockCapacity) {
		t.Fatalf("capacity error = %v, want %v", err, ErrSQLRowLockCapacity)
	}
}

func TestTT049RowLocksCancellationReclaimsWaiter(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{MaxKeys: 2})
	first, err := manager.Acquire(context.Background(), "row:1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := manager.Acquire(ctx, "row:1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled Acquire() = %v, want deadline", err)
	}
	if !first.Release() {
		t.Fatal("first lease did not release")
	}
	if stats := manager.Stats(); stats.Entries != 0 {
		t.Fatalf("stats after cancellation and release = %#v, want no entries", stats)
	}
}

func TestTT049RowLocksRejectInvalidInputs(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{MaxKeyBytes: 4})
	if _, err := manager.Acquire(context.Background(), ""); !errors.Is(err, ErrSQLRowLockKeyRequired) {
		t.Fatalf("empty key error = %v, want %v", err, ErrSQLRowLockKeyRequired)
	}
	if _, err := manager.TryAcquire("12345"); !errors.Is(err, ErrSQLRowLockKeyTooLarge) {
		t.Fatalf("long key error = %v, want %v", err, ErrSQLRowLockKeyTooLarge)
	}
	if _, err := (*SQLRowLockManager)(nil).TryAcquire("row:1"); !errors.Is(err, ErrSQLRowLockManagerNil) {
		t.Fatalf("nil manager error = %v, want %v", err, ErrSQLRowLockManagerNil)
	}
}
