package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLSourceFrontierTrackerWaitReadyBlocksUntilEveryPartitionReachesFrontier(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{
		{Source: "orders", Partition: "eu"},
		{Source: "orders", Partition: "us"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- tracker.WaitReady(ctx, 10) }()

	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "eu", Frontier: 10}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ready:
		t.Fatalf("WaitReady returned after one partition: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "us", Frontier: 9}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ready:
		t.Fatalf("WaitReady returned below target frontier: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := tracker.Observe(SQLSourceFrontier{Source: "orders", Partition: "us", Frontier: 10}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("WaitReady() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not unblock at the common frontier")
	}
	if frontier, ready := tracker.CommonFrontier(); !ready || frontier != 10 {
		t.Fatalf("CommonFrontier() = %d, %t; want 10, true", frontier, ready)
	}
}

func TestSQLSourceFrontierTrackerWaitReadyHonorsCancellation(t *testing.T) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{Source: "orders", Partition: "eu"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tracker.WaitReady(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady() error = %v, want context.Canceled", err)
	}
}
