package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestC231WorkloadGroupEnforcesClassConcurrencyAndMemory(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		MaxConcurrent: 2,
		Classes: []SQLWorkloadClass{{
			Name:           "analytics",
			MaxConcurrent:  1,
			MaxMemoryBytes: 100,
		}},
	})
	if err != nil {
		t.Fatalf("NewSQLWorkloadAdmission() error = %v", err)
	}
	defer admission.Close()

	first, err := admission.AcquireWithMemory(context.Background(), "analytics", 60)
	if err != nil {
		t.Fatalf("first AcquireWithMemory() error = %v", err)
	}

	if _, err := admission.AcquireWithMemory(context.Background(), "analytics", 101); !errors.Is(err, ErrSQLWorkloadAdmissionMemoryLimit) {
		t.Fatalf("oversized AcquireWithMemory() error = %v, want memory limit", err)
	}

	secondDone := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.AcquireWithMemory(context.Background(), "analytics", 40)
		if acquireErr != nil {
			secondDone <- acquireErr
			return
		}
		lease()
		secondDone <- nil
	}()

	deadline := time.Now().Add(time.Second)
	for admission.Stats().Pending == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if admission.Stats().Pending != 1 {
		t.Fatalf("pending = %d, want class-concurrency waiter", admission.Stats().Pending)
	}

	first()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second AcquireWithMemory() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second AcquireWithMemory() did not wake after release")
	}

	classStats := admission.ClassStats()
	if len(classStats) != 2 {
		t.Fatalf("ClassStats() length = %d, want analytics and default", len(classStats))
	}
	for _, stats := range classStats {
		if stats.Name == "analytics" && (stats.MaxConcurrent != 1 || stats.MaxMemoryBytes != 100 || stats.Active != 0 || stats.MemoryBytes != 0) {
			t.Fatalf("analytics class stats = %#v after release, want zero active and memory", stats)
		}
	}
}

func TestC231WorkloadGroupValidatesBudgets(t *testing.T) {
	if _, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		Classes: []SQLWorkloadClass{{Name: "bad", MaxConcurrent: -1}},
	}); !errors.Is(err, ErrSQLWorkloadAdmissionOptionsInvalid) {
		t.Fatalf("negative class concurrency error = %v, want options invalid", err)
	}
	if _, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		Classes: []SQLWorkloadClass{{Name: "bad", MaxMemoryBytes: -1}},
	}); !errors.Is(err, ErrSQLWorkloadAdmissionOptionsInvalid) {
		t.Fatalf("negative class memory error = %v, want options invalid", err)
	}
}

func TestC231WorkloadGroupWaitsForMemoryBudget(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		MaxConcurrent: 2,
		Classes: []SQLWorkloadClass{{
			Name:           "analytics",
			MaxMemoryBytes: 100,
		}},
	})
	if err != nil {
		t.Fatalf("NewSQLWorkloadAdmission() error = %v", err)
	}
	defer admission.Close()

	first, err := admission.AcquireWithMemory(context.Background(), "analytics", 70)
	if err != nil {
		t.Fatalf("first AcquireWithMemory() error = %v", err)
	}
	secondDone := make(chan error, 1)
	go func() {
		lease, acquireErr := admission.AcquireWithMemory(context.Background(), "analytics", 40)
		if acquireErr == nil {
			lease()
		}
		secondDone <- acquireErr
	}()

	deadline := time.Now().Add(time.Second)
	for admission.Stats().Pending == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if admission.Stats().Pending != 1 {
		t.Fatalf("pending = %d, want memory-budget waiter", admission.Stats().Pending)
	}
	first()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatalf("second AcquireWithMemory() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("memory-budget waiter did not wake after release")
	}
}
