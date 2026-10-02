package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestM35SQLSourceFrontierTrackerWaitUntilBlocksUntilCommonFrontierIsReady(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{
		{Source: "orders", Partition: "0"},
		{Source: "orders", Partition: "1"},
	})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}

	result := make(chan error, 1)
	go func() {
		result <- tracker.WaitUntil(context.Background(), 10)
	}()

	assertM35WaitPending(t, result)
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil {
		t.Fatalf("Observe(0) error = %v", err)
	}
	assertM35WaitPending(t, result)
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "1", Frontier: 9}); err != nil {
		t.Fatalf("Observe(1,9) error = %v", err)
	}
	assertM35WaitPending(t, result)
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "1", Frontier: 10}); err != nil {
		t.Fatalf("Observe(1,10) error = %v", err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("WaitUntil() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitUntil() did not unblock at the common frontier")
	}
}

func TestM35SQLSourceFrontierTrackerWaitUntilHonorsCancellationAndZeroRequiresObservation(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- tracker.WaitUntil(ctx, 0)
	}()
	assertM35WaitPending(t, result)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitUntil() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitUntil() did not honor cancellation")
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 0}); err != nil {
		t.Fatalf("Observe(zero) error = %v", err)
	}
	if err := tracker.WaitUntil(context.Background(), 0); err != nil {
		t.Fatalf("WaitUntil(observed zero) error = %v", err)
	}
}

func TestM35SQLSourceFrontierTrackerWaitUntilWakesAllWaiters(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	started := make(chan struct{}, 2)
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			started <- struct{}{}
			results <- tracker.WaitUntil(context.Background(), 10)
		}()
	}
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("WaitUntil() waiter did not start")
		}
	}
	assertM35WaitPending(t, results)
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	for index := 0; index < 2; index++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("WaitUntil() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("WaitUntil() waiter did not wake")
		}
	}
}

func TestM35SQLSourceFrontierTrackerWaitUntilNilReceiverAndNilContext(t *testing.T) {
	var tracker *SQLSourceFrontierTracker
	if err := tracker.WaitUntil(nil, 0); !errors.Is(err, ErrSQLSourceFrontierTrackerNil) {
		t.Fatalf("nil WaitUntil() error = %v, want nil tracker error", err)
	}
}

func TestM35SQLSourceFrontierTrackerWaitUntilReadyHasNoAllocations(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "0"}})
	if err != nil {
		t.Fatalf("NewSQLSourceFrontierTracker() error = %v", err)
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "0", Frontier: 10}); err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if err := tracker.WaitUntil(context.Background(), 10); err != nil {
			t.Fatalf("WaitUntil() error = %v", err)
		}
	}); allocations != 0 {
		t.Fatalf("ready WaitUntil() allocations = %v, want 0", allocations)
	}
}

func assertM35WaitPending(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("WaitUntil() returned early with %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}
