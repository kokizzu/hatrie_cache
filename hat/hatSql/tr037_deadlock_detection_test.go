package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTR037DeadlockDetectionRejectsTwoKeyCycle(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true})
	firstA, err := manager.AcquireOwned(context.Background(), "tx-a", "row-a")
	if err != nil {
		t.Fatalf("AcquireOwned(tx-a, row-a) error = %v", err)
	}
	defer firstA.Release()
	firstB, err := manager.AcquireOwned(context.Background(), "tx-b", "row-b")
	if err != nil {
		t.Fatalf("AcquireOwned(tx-b, row-b) error = %v", err)
	}

	blocked := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.AcquireOwned(context.Background(), "tx-b", "row-a")
		if lease != nil {
			lease.Release()
		}
		blocked <- acquireErr
	}()
	waitForTR037WaitEdge(t, manager)

	if _, err := manager.AcquireOwned(context.Background(), "tx-a", "row-b"); !errors.Is(err, ErrSQLRowLockDeadlock) {
		t.Fatalf("cycle error = %v, want %v", err, ErrSQLRowLockDeadlock)
	}
	if firstA.Release() != true {
		t.Fatal("firstA.Release() = false, want true")
	}
	if err := <-blocked; err != nil {
		t.Fatalf("blocked owner after cycle resolution = %v", err)
	}
	if firstB.Release() != true {
		t.Fatal("firstB.Release() = false, want true")
	}
	if got := manager.Stats().WaitEdges; got != 0 {
		t.Fatalf("WaitEdges = %d after cleanup, want 0", got)
	}
}

func TestTR037DeadlockDetectionRemovesCanceledWait(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true})
	lease, err := manager.AcquireOwned(context.Background(), "holder", "row")
	if err != nil {
		t.Fatalf("holder AcquireOwned() error = %v", err)
	}
	defer lease.Release()

	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan error, 1)
	go func() {
		waitingLease, acquireErr := manager.AcquireOwned(ctx, "waiter", "row")
		if waitingLease != nil {
			waitingLease.Release()
		}
		blocked <- acquireErr
	}()
	waitForTR037WaitEdge(t, manager)
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquisition error = %v, want context.Canceled", err)
	}
	if got := manager.Stats().WaitEdges; got != 0 {
		t.Fatalf("WaitEdges = %d after cancellation, want 0", got)
	}
}

func TestTR037DeadlockDetectionValidatesOwnerAndRemainsOptIn(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{})
	if manager.Stats().DeadlockDetection {
		t.Fatal("default DeadlockDetection = true, want false")
	}
	if _, err := manager.AcquireOwned(context.Background(), "", "row"); !errors.Is(err, ErrSQLRowLockOwnerRequired) {
		t.Fatalf("empty owner error = %v, want %v", err, ErrSQLRowLockOwnerRequired)
	}
	ordinary, err := manager.AcquireOwned(context.Background(), "too-long", "row")
	if err != nil {
		t.Fatalf("ordinary owner error = %v", err)
	}
	ordinary.Release()
	limited := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true, MaxOwnerBytes: 4})
	if _, err := limited.AcquireOwned(context.Background(), "12345", "row"); !errors.Is(err, ErrSQLRowLockOwnerTooLarge) {
		t.Fatalf("large owner error = %v, want %v", err, ErrSQLRowLockOwnerTooLarge)
	}
}

func TestTR037DeadlockDetectionBoundsWaitGraph(t *testing.T) {
	manager := NewSQLRowLockManager(SQLRowLockManagerOptions{EnableDeadlockDetection: true, MaxWaitEdges: 1})
	holder, err := manager.AcquireOwned(context.Background(), "holder", "row")
	if err != nil {
		t.Fatalf("holder AcquireOwned() error = %v", err)
	}
	defer holder.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan error, 1)
	go func() {
		lease, acquireErr := manager.AcquireOwned(ctx, "waiter-1", "row")
		if lease != nil {
			lease.Release()
		}
		blocked <- acquireErr
	}()
	waitForTR037WaitEdge(t, manager)
	if got := manager.Stats().MaxWaitEdges; got != 1 {
		t.Fatalf("MaxWaitEdges = %d, want 1", got)
	}
	if _, err := manager.AcquireOwned(context.Background(), "waiter-2", "row"); !errors.Is(err, ErrSQLRowLockWaitGraphCapacity) {
		t.Fatalf("wait graph capacity error = %v, want %v", err, ErrSQLRowLockWaitGraphCapacity)
	}
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("bounded waiter cleanup error = %v, want context.Canceled", err)
	}
}

func waitForTR037WaitEdge(t *testing.T, manager *SQLRowLockManager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if manager.Stats().WaitEdges > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("wait edge was not registered; stats = %#v", manager.Stats())
}
