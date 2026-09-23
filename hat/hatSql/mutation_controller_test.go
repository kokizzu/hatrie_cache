package hatSql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMutationControllerRunsQueuedWorkByPriority(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{QueueCapacity: 8, Workers: 1, HistoryCapacity: 8})
	if err != nil {
		t.Fatalf("NewMutationController() error = %v", err)
	}
	defer controller.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	var orderMu sync.Mutex
	order := make([]string, 0, 3)
	blocker, err := controller.Submit(context.Background(), MutationSpec{
		ID:       "blocker",
		Priority: 0,
		Run: func(ctx context.Context, _ MutationProgressReporter) error {
			orderMu.Lock()
			order = append(order, "blocker")
			orderMu.Unlock()
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
		t.Fatalf("submit blocker: %v", err)
	}
	waitForMutationState(t, blocker, MutationRunning)

	newJob := func(id string, priority int) MutationHandle {
		handle, submitErr := controller.Submit(context.Background(), MutationSpec{
			ID:       id,
			Priority: priority,
			Run: func(context.Context, MutationProgressReporter) error {
				orderMu.Lock()
				order = append(order, id)
				orderMu.Unlock()
				return nil
			},
		})
		if submitErr != nil {
			t.Fatalf("submit %s: %v", id, submitErr)
		}
		return handle
	}
	high := newJob("high", 100)
	low := newJob("low", -100)
	close(release)
	for name, handle := range map[string]MutationHandle{"blocker": blocker, "high": high, "low": low} {
		if err := handle.Wait(context.Background()); err != nil {
			t.Fatalf("wait %s: %v", name, err)
		}
	}
	orderMu.Lock()
	gotOrder := append([]string(nil), order...)
	orderMu.Unlock()
	wantOrder := []string{"blocker", "high", "low"}
	if len(gotOrder) != len(wantOrder) {
		t.Fatalf("execution order = %#v, want %#v", gotOrder, wantOrder)
	}
	for index := range wantOrder {
		if gotOrder[index] != wantOrder[index] {
			t.Fatalf("execution order = %#v, want %#v", gotOrder, wantOrder)
		}
	}
}

func TestMutationControllerCancelsQueuedAndRunningMutations(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{QueueCapacity: 4, Workers: 1, HistoryCapacity: 8})
	if err != nil {
		t.Fatalf("NewMutationController() error = %v", err)
	}
	defer controller.Close()

	running, err := controller.Submit(context.Background(), MutationSpec{
		ID: "running",
		Run: func(ctx context.Context, _ MutationProgressReporter) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("submit running: %v", err)
	}
	waitForMutationState(t, running, MutationRunning)
	queued, err := controller.Submit(context.Background(), MutationSpec{ID: "queued", Run: func(context.Context, MutationProgressReporter) error {
		t.Fatal("canceled queued mutation started")
		return nil
	}})
	if err != nil {
		t.Fatalf("submit queued: %v", err)
	}
	if err := queued.Cancel(); err != nil {
		t.Fatalf("cancel queued: %v", err)
	}
	if err := queued.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Wait() error = %v, want context.Canceled", err)
	}
	queuedSnapshot, err := queued.Snapshot()
	if err != nil {
		t.Fatalf("queued Snapshot() error = %v", err)
	}
	if queuedSnapshot.State != MutationCanceled {
		t.Fatalf("queued state = %q, want %q", queuedSnapshot.State, MutationCanceled)
	}
	if err := running.Cancel(); err != nil {
		t.Fatalf("cancel running: %v", err)
	}
	if err := running.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("running Wait() error = %v, want context.Canceled", err)
	}
	runningSnapshot, err := running.Snapshot()
	if err != nil {
		t.Fatalf("running Snapshot() error = %v", err)
	}
	if runningSnapshot.State != MutationCanceled {
		t.Fatalf("running state = %q, want %q", runningSnapshot.State, MutationCanceled)
	}
}

func TestMutationControllerReportsProgressAndBoundsHistory(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{HistoryCapacity: 1})
	if err != nil {
		t.Fatalf("NewMutationController() error = %v", err)
	}
	defer controller.Close()

	first, err := controller.Submit(context.Background(), MutationSpec{ID: "first", Run: func(_ context.Context, report MutationProgressReporter) error {
		if err := report(MutationProgress{Completed: 1, Total: 2}); err != nil {
			return err
		}
		return report(MutationProgress{Completed: 2, Total: 2})
	}})
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	if err := first.Wait(context.Background()); err != nil {
		t.Fatalf("wait first: %v", err)
	}
	second, err := controller.Submit(context.Background(), MutationSpec{ID: "second", Run: func(_ context.Context, report MutationProgressReporter) error {
		return report(MutationProgress{Completed: 3, Total: 3})
	}})
	if err != nil {
		t.Fatalf("submit second: %v", err)
	}
	if err := second.Wait(context.Background()); err != nil {
		t.Fatalf("wait second: %v", err)
	}
	snapshot, err := second.Snapshot()
	if err != nil {
		t.Fatalf("second Snapshot() error = %v", err)
	}
	if snapshot.State != MutationSucceeded || snapshot.Completed != 3 || snapshot.Total != 3 {
		t.Fatalf("second snapshot = %#v, want succeeded 3/3", snapshot)
	}
	if _, err := first.Snapshot(); !errors.Is(err, ErrMutationNotFound) {
		t.Fatalf("evicted first Snapshot() error = %v, want %v", err, ErrMutationNotFound)
	}
}

func TestMutationControllerRejectsFullQueueAndClosesCleanly(t *testing.T) {
	controller, err := NewMutationController(MutationControllerOptions{QueueCapacity: 1, Workers: 1})
	if err != nil {
		t.Fatalf("NewMutationController() error = %v", err)
	}
	blocker, err := controller.Submit(context.Background(), MutationSpec{ID: "blocker", Run: func(ctx context.Context, _ MutationProgressReporter) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	if err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	waitForMutationState(t, blocker, MutationRunning)
	queued, err := controller.Submit(context.Background(), MutationSpec{ID: "queued", Run: func(context.Context, MutationProgressReporter) error { return nil }})
	if err != nil {
		t.Fatalf("submit queued: %v", err)
	}
	if _, err := controller.Submit(context.Background(), MutationSpec{ID: "rejected", Run: func(context.Context, MutationProgressReporter) error { return nil }}); !errors.Is(err, ErrMutationQueueFull) {
		t.Fatalf("full queue error = %v, want %v", err, ErrMutationQueueFull)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := blocker.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocker Wait() error = %v, want context.Canceled", err)
	}
	if err := queued.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Wait() error = %v, want context.Canceled", err)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func waitForMutationState(t *testing.T, handle MutationHandle, want MutationState) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := handle.Snapshot()
		if err == nil && snapshot.State == want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("mutation %q did not reach state %q; snapshot=%#v err=%v", handle.ID(), want, snapshot, err)
		case <-ticker.C:
		}
	}
}
