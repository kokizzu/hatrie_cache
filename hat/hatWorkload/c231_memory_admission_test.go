package hatWorkload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestC231MemoryAdmissionBlocksUntilBytesRelease(t *testing.T) {
	controller, err := NewMemoryAdmissionController(AdmissionOptions{
		MaxInFlight: 2,
		MaxQueued:   4,
	})
	if err != nil {
		t.Fatalf("NewMemoryAdmissionController() error = %v", err)
	}
	if err := controller.RegisterClass(MemoryClassOptions{
		ClassOptions:   ClassOptions{Name: "analytics", MaxInFlight: 2},
		MaxMemoryBytes: 100,
	}); err != nil {
		t.Fatalf("RegisterClass() error = %v", err)
	}

	first, err := controller.Acquire(context.Background(), "analytics", 80)
	if err != nil {
		t.Fatalf("first Acquire() error = %v", err)
	}
	defer first.Release()

	acquired := make(chan MemoryAdmissionLease, 1)
	acquireErr := make(chan error, 1)
	go func() {
		lease, acquireErrValue := controller.Acquire(context.Background(), "analytics", 30)
		if acquireErrValue != nil {
			acquireErr <- acquireErrValue
			return
		}
		acquired <- lease
	}()

	select {
	case lease := <-acquired:
		lease.Release()
		t.Fatal("second memory reservation was admitted before release")
	case err := <-acquireErr:
		t.Fatalf("second Acquire() error = %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	first.Release()
	select {
	case lease := <-acquired:
		if lease.MemoryBytes() != 30 || lease.Class() != "analytics" {
			t.Fatalf("lease = %q/%d, want analytics/30", lease.Class(), lease.MemoryBytes())
		}
		lease.Release()
	case err := <-acquireErr:
		t.Fatalf("second Acquire() after release error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("second memory reservation did not proceed after release")
	}
}

func TestC231MemoryAdmissionKeepsClassBudgetsIndependent(t *testing.T) {
	controller, err := NewMemoryAdmissionController(AdmissionOptions{MaxInFlight: 4})
	if err != nil {
		t.Fatalf("NewMemoryAdmissionController() error = %v", err)
	}
	for _, class := range []string{"one", "two"} {
		if err := controller.RegisterClass(MemoryClassOptions{
			ClassOptions:   ClassOptions{Name: class},
			MaxMemoryBytes: 50,
		}); err != nil {
			t.Fatalf("RegisterClass(%q) error = %v", class, err)
		}
	}

	first, err := controller.Acquire(context.Background(), "one", 50)
	if err != nil {
		t.Fatalf("class one Acquire() error = %v", err)
	}
	second, err := controller.Acquire(context.Background(), "two", 50)
	if err != nil {
		first.Release()
		t.Fatalf("class two Acquire() error = %v", err)
	}
	first.Release()
	second.Release()
}

func TestC231MemoryAdmissionRejectsOversizedRequestsAndCancellation(t *testing.T) {
	controller, err := NewMemoryAdmissionController(AdmissionOptions{MaxInFlight: 1, MaxQueued: 1})
	if err != nil {
		t.Fatalf("NewMemoryAdmissionController() error = %v", err)
	}
	if err := controller.RegisterClass(MemoryClassOptions{
		ClassOptions:   ClassOptions{Name: "batch"},
		MaxMemoryBytes: 10,
	}); err != nil {
		t.Fatalf("RegisterClass() error = %v", err)
	}
	if _, err := controller.Acquire(context.Background(), "batch", 11); !errors.Is(err, ErrMemoryAdmissionRequestTooLarge) {
		t.Fatalf("oversized Acquire() error = %v, want ErrMemoryAdmissionRequestTooLarge", err)
	}

	active, err := controller.Acquire(context.Background(), "batch", 10)
	if err != nil {
		t.Fatalf("active Acquire() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	queued := make(chan error, 1)
	go func() {
		_, acquireErr := controller.Acquire(ctx, "batch", 1)
		queued <- acquireErr
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case acquireErr := <-queued:
		if !errors.Is(acquireErr, context.Canceled) {
			t.Fatalf("canceled Acquire() error = %v, want context.Canceled", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled memory waiter did not return")
	}
	active.Release()
	active.Release()

	lease, err := controller.Acquire(context.Background(), "batch", 1)
	if err != nil {
		t.Fatalf("post-cancellation Acquire() error = %v", err)
	}
	lease.Release()
}

func TestC231MemoryAdmissionSnapshotAndZeroBudget(t *testing.T) {
	controller, err := NewMemoryAdmissionController(AdmissionOptions{MaxInFlight: 2})
	if err != nil {
		t.Fatalf("NewMemoryAdmissionController() error = %v", err)
	}
	if err := controller.RegisterClass(MemoryClassOptions{ClassOptions: ClassOptions{Name: "unlimited"}}); err != nil {
		t.Fatalf("RegisterClass() error = %v", err)
	}
	lease, err := controller.Acquire(context.Background(), "unlimited", 1<<40)
	if err != nil {
		t.Fatalf("zero-budget Acquire() error = %v", err)
	}
	snapshot := controller.Snapshot()
	if snapshot.ReservedMemoryBytes != 1<<40 || len(snapshot.Classes) != 1 || snapshot.Classes[0].ActiveMemoryBytes != 1<<40 {
		t.Fatalf("snapshot = %#v, want one active 1 TiB reservation", snapshot)
	}
	lease.Release()
	lease.Release()
	if snapshot = controller.Snapshot(); snapshot.ReservedMemoryBytes != 0 || snapshot.Admission.Active != 0 {
		t.Fatalf("released snapshot = %#v, want zero active state", snapshot)
	}
}
