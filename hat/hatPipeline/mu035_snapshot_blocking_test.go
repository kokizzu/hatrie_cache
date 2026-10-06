package hatPipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestMU035SnapshotCutoverWaitBlocksUntilCommit(t *testing.T) {
	coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(hatPipeline.SnapshotCutoverOptions{MaxCutovers: 2, MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{
		ID:        "snapshot-1",
		Timestamp: 10,
		Sources: []hatPipeline.SnapshotCutoverSource{
			{ID: "source-a", Generation: 1},
			{ID: "source-b", Generation: 1},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result := make(chan snapshotWaitResult, 1)
	go func() {
		status, waitErr := coordinator.Wait(context.Background(), "snapshot-1")
		result <- snapshotWaitResult{status: status, err: waitErr}
	}()
	assertMU035Waits(t, result)

	if _, err := coordinator.Acknowledge("snapshot-1", hatPipeline.SnapshotCutoverAcknowledgement{SourceID: "source-a", Generation: 1, Lower: 10, Upper: 10}); err != nil {
		t.Fatal(err)
	}
	assertMU035Waits(t, result)
	if _, err := coordinator.Acknowledge("snapshot-1", hatPipeline.SnapshotCutoverAcknowledgement{SourceID: "source-b", Generation: 1, Lower: 10, Upper: 10}); err != nil {
		t.Fatal(err)
	}
	assertMU035Waits(t, result)

	if _, err := coordinator.Commit("snapshot-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-result:
		if completed.err != nil || completed.status.State != hatPipeline.SnapshotCutoverCommitted || completed.status.Remaining != 0 {
			t.Fatalf("Wait() = %#v, want committed status", completed)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not unblock after commit")
	}
}

func TestMU035SnapshotCutoverWaitReturnsAbortAndHonorsCancellation(t *testing.T) {
	coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(hatPipeline.SnapshotCutoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{ID: "abort-me", Timestamp: 1, Sources: []hatPipeline.SnapshotCutoverSource{{ID: "source", Generation: 1}}}); err != nil {
		t.Fatal(err)
	}
	aborted := make(chan snapshotWaitResult, 1)
	go func() {
		status, waitErr := coordinator.Wait(context.Background(), "abort-me")
		aborted <- snapshotWaitResult{status: status, err: waitErr}
	}()
	assertMU035Waits(t, aborted)
	if _, err := coordinator.Abort("abort-me", "source failed"); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-aborted:
		if !errors.Is(result.err, hatPipeline.ErrSnapshotCutoverAborted) || result.status.State != hatPipeline.SnapshotCutoverAborted || result.status.Reason != "source failed" {
			t.Fatalf("aborted Wait() = %#v, want bounded abort error/status", result)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not unblock after abort")
	}

	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{ID: "cancel-me", Timestamp: 1, Sources: []hatPipeline.SnapshotCutoverSource{{ID: "source", Generation: 1}}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, waitErr := coordinator.Wait(ctx, "cancel-me")
		canceled <- waitErr
	}()
	cancel()
	select {
	case waitErr := <-canceled:
		if !errors.Is(waitErr, context.Canceled) {
			t.Fatalf("canceled Wait() error = %v, want context.Canceled", waitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait() did not honor context cancellation")
	}
}

func TestMU035SnapshotCutoverWaitWakesAllWaiters(t *testing.T) {
	coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(hatPipeline.SnapshotCutoverOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{ID: "many", Timestamp: 1, Sources: []hatPipeline.SnapshotCutoverSource{{ID: "source", Generation: 1}}}); err != nil {
		t.Fatal(err)
	}
	results := make(chan snapshotWaitResult, 2)
	for range 2 {
		go func() {
			status, waitErr := coordinator.Wait(context.Background(), "many")
			results <- snapshotWaitResult{status: status, err: waitErr}
		}()
	}
	assertMU035Waits(t, results)
	assertMU035Waits(t, results)
	if _, err := coordinator.Acknowledge("many", hatPipeline.SnapshotCutoverAcknowledgement{SourceID: "source", Generation: 1, Lower: 1, Upper: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Commit("many"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case result := <-results:
			if result.err != nil || result.status.State != hatPipeline.SnapshotCutoverCommitted {
				t.Fatalf("waiter result = %#v, want committed", result)
			}
		case <-time.After(time.Second):
			t.Fatal("one of the waiters did not wake")
		}
	}
}

func TestMU035SnapshotCutoverWaitRejectsInvalidContextAndForgottenSnapshot(t *testing.T) {
	coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(hatPipeline.SnapshotCutoverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Wait(nil, "missing"); !errors.Is(err, hatPipeline.ErrSnapshotCutoverContextNil) {
		t.Fatalf("nil context error = %v, want ErrSnapshotCutoverContextNil", err)
	}
	if _, err := coordinator.Wait(context.Background(), "missing"); !errors.Is(err, hatPipeline.ErrSnapshotCutoverNotFound) {
		t.Fatalf("missing snapshot error = %v, want not found", err)
	}
	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{ID: "forget-me", Timestamp: 1, Sources: []hatPipeline.SnapshotCutoverSource{{ID: "source", Generation: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Abort("forget-me", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Forget("forget-me"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Wait(context.Background(), "forget-me"); !errors.Is(err, hatPipeline.ErrSnapshotCutoverNotFound) {
		t.Fatalf("forgotten snapshot error = %v, want not found", err)
	}
}

type snapshotWaitResult struct {
	status hatPipeline.SnapshotCutoverStatus
	err    error
}

func assertMU035Waits(t *testing.T, result <-chan snapshotWaitResult) {
	t.Helper()
	select {
	case completed := <-result:
		t.Fatalf("Wait() returned early: %#v", completed)
	case <-time.After(10 * time.Millisecond):
	}
}
