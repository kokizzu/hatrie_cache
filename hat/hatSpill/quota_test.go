package hatSpill

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBudgetRejectsReservationLargerThanLimit(t *testing.T) {
	budget := NewBudget(10)
	_, err := budget.Reserve(context.Background(), 11)
	if !errors.Is(err, ErrRequestTooLarge) {
		t.Fatalf("Reserve error = %v, want %v", err, ErrRequestTooLarge)
	}
	if got := budget.Snapshot(); got.Used != 0 || got.Queued != 0 {
		t.Fatalf("rejected request changed snapshot: %+v", got)
	}
}

func TestBudgetMaintainsFIFOAndEnforcesBytes(t *testing.T) {
	budget := NewBudget(10)
	hel, err := budget.Reserve(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}

	first := make(chan Reservation, 1)
	second := make(chan Reservation, 1)
	go reserveForTest(t, budget, 7, first)
	waitForQueued(t, budget, 1)
	go reserveForTest(t, budget, 4, second)
	waitForQueued(t, budget, 2)

	hel.Release()
	firstLease := receiveReservation(t, first)
	select {
	case lease := <-second:
		lease.Release()
		t.Fatal("second waiter bypassed the first waiter")
	case <-time.After(20 * time.Millisecond):
	}

	firstLease.Release()
	secondLease := receiveReservation(t, second)
	secondLease.Release()
	if got := budget.Snapshot().Used; got != 0 {
		t.Fatalf("Used after releases = %d, want 0", got)
	}
}

func TestBudgetCancellationRemovesWaiter(t *testing.T) {
	budget := NewBudget(4)
	hel, err := budget.Reserve(context.Background(), 4)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, reserveErr := budget.Reserve(ctx, 1)
		result <- reserveErr
	}()
	waitForQueued(t, budget, 1)
	cancel()
	if err := receiveError(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reserve error = %v, want context.Canceled", err)
	}
	if got := budget.Snapshot().Queued; got != 0 {
		t.Fatalf("Queued after cancellation = %d, want 0", got)
	}
	hel.Release()
}

func TestBudgetDoesNotReserveForCanceledContext(t *testing.T) {
	budget := NewBudget(4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.Reserve(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reserve error = %v, want context.Canceled", err)
	}
	if got := budget.Snapshot(); got.Used != 0 || got.Queued != 0 {
		t.Fatalf("canceled reservation changed snapshot: %+v", got)
	}
}

func TestBudgetReleaseIsIdempotentAcrossCopies(t *testing.T) {
	budget := NewBudget(8)
	lease, err := budget.Reserve(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	copyLease := lease
	if !lease.Release() {
		t.Fatal("first Release returned false")
	}
	if copyLease.Release() {
		t.Fatal("copied Release returned true twice")
	}
	if lease.Release() {
		t.Fatal("original Release returned true twice")
	}
	if got := budget.Snapshot().Used; got != 0 {
		t.Fatalf("Used after idempotent releases = %d, want 0", got)
	}
}

func TestBudgetCloseUnblocksWaiters(t *testing.T) {
	budget := NewBudget(2)
	hel, err := budget.Reserve(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, reserveErr := budget.Reserve(context.Background(), 1)
		result <- reserveErr
	}()
	waitForQueued(t, budget, 1)
	budget.Close()
	if err := receiveError(t, result); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reserve error = %v, want %v", err, ErrClosed)
	}
	hel.Release()
	if _, err := budget.Reserve(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reserve after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestUnlimitedBudgetDoesNotQueue(t *testing.T) {
	budget := NewBudget(0)
	lease, err := budget.Reserve(context.Background(), 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Snapshot(); got.Used != 1<<62 || got.Queued != 0 || got.Limit != 0 {
		t.Fatalf("unlimited snapshot = %+v", got)
	}
	lease.Release()
}

func TestUnlimitedBudgetRejectsCounterOverflow(t *testing.T) {
	budget := NewBudget(0)
	lease, err := budget.Reserve(context.Background(), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budget.Reserve(context.Background(), 1); !errors.Is(err, ErrQuotaOverflow) {
		t.Fatalf("Reserve overflow error = %v, want %v", err, ErrQuotaOverflow)
	}
	lease.Release()
}

func reserveForTest(t *testing.T, budget *Budget, bytes uint64, result chan<- Reservation) {
	t.Helper()
	lease, err := budget.Reserve(context.Background(), bytes)
	if err != nil {
		t.Errorf("Reserve(%d) error = %v", bytes, err)
		return
	}
	result <- lease
}

func receiveReservation(t *testing.T, result <-chan Reservation) Reservation {
	t.Helper()
	select {
	case lease := <-result:
		return lease
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reservation")
		return Reservation{}
	}
}

func receiveError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Reserve error")
		return nil
	}
}

func waitForQueued(t *testing.T, budget *Budget, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if budget.Snapshot().Queued >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Queued = %d, want at least %d", budget.Snapshot().Queued, want)
}
