package hatSql

import (
	"context"
	"testing"
	"time"
)

func TestC238MutationSnapshotReportsRemainingElapsedAndEstimate(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{Workers: 1, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	handle, err := controller.Submit(context.Background(), MutationSpec{
		ID: "c238-active",
		Run: func(ctx context.Context, report MutationProgressReporter) error {
			if err := report(MutationProgress{Completed: 2, Total: 10}); err != nil {
				return err
			}
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	controller.mu.Lock()
	handle.job.startedAt = time.Now().UTC().Add(-200 * time.Millisecond)
	controller.mu.Unlock()

	active, err := handle.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if active.Remaining != 8 {
		t.Fatalf("active Remaining = %d, want 8", active.Remaining)
	}
	if active.Elapsed < 190*time.Millisecond {
		t.Fatalf("active Elapsed = %s, want at least 190ms", active.Elapsed)
	}
	if active.EstimatedRemaining < 700*time.Millisecond || active.EstimatedRemaining > 2*time.Second {
		t.Fatalf("active EstimatedRemaining = %s, want roughly 800ms", active.EstimatedRemaining)
	}

	close(release)
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, err := handle.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Remaining != 0 {
		t.Fatalf("terminal Remaining = %d, want 0", terminal.Remaining)
	}
	if terminal.EstimatedRemaining != 0 {
		t.Fatalf("terminal EstimatedRemaining = %s, want 0", terminal.EstimatedRemaining)
	}
	if terminal.Elapsed < active.Elapsed {
		t.Fatalf("terminal Elapsed = %s, active Elapsed = %s", terminal.Elapsed, active.Elapsed)
	}
}

func TestC238MutationSnapshotUnknownTotalHasNoFalseEstimate(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{Workers: 1, QueueCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	handle, err := controller.Submit(context.Background(), MutationSpec{
		ID: "c238-unknown-total",
		Run: func(ctx context.Context, report MutationProgressReporter) error {
			if err := report(MutationProgress{Completed: 2}); err != nil {
				return err
			}
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	active, err := handle.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if active.Remaining != 0 || active.EstimatedRemaining != 0 {
		t.Fatalf("unknown-total snapshot = %#v, want no derived remaining work", active)
	}
	close(release)
	if err := handle.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
