package hatPipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatPipeline"
)

func TestM223HydrationStateMachineLifecycleAndProgress(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	initial := machine.Snapshot()
	if initial.State != hatPipeline.HydrationStateCold || initial.Generation != 0 || initial.Completed != 0 || initial.Total != 0 || initial.Remaining != 0 {
		t.Fatalf("initial Snapshot() = %#v", initial)
	}

	if err := machine.Begin(3); err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	started := machine.Snapshot()
	if started.State != hatPipeline.HydrationStateHydrating || started.Generation != 1 || started.Completed != 0 || started.Total != 3 || started.Remaining != 3 {
		t.Fatalf("started Snapshot() = %#v", started)
	}
	if err := machine.Begin(3); !errors.Is(err, hatPipeline.ErrHydrationAlreadyRunning) {
		t.Fatalf("second Begin() error = %v, want %v", err, hatPipeline.ErrHydrationAlreadyRunning)
	}
	if err := machine.Advance(1); err != nil {
		t.Fatalf("Advance(1) error = %v", err)
	}
	progress := machine.Snapshot()
	if progress.State != hatPipeline.HydrationStateHydrating || progress.Completed != 1 || progress.Total != 3 || progress.Remaining != 2 {
		t.Fatalf("progress Snapshot() = %#v", progress)
	}
	if err := machine.Complete(); !errors.Is(err, hatPipeline.ErrHydrationIncomplete) {
		t.Fatalf("early Complete() error = %v, want %v", err, hatPipeline.ErrHydrationIncomplete)
	}
	if err := machine.Advance(2); err != nil {
		t.Fatalf("Advance(2) error = %v", err)
	}
	if err := machine.Complete(); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	ready := machine.Snapshot()
	if ready.State != hatPipeline.HydrationStateReady || ready.Generation != 1 || ready.Completed != 3 || ready.Total != 3 || ready.Remaining != 0 {
		t.Fatalf("ready Snapshot() = %#v", ready)
	}

	if err := machine.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	reset := machine.Snapshot()
	if reset.State != hatPipeline.HydrationStateCold || reset.Generation != 1 || reset.Completed != 0 || reset.Total != 0 || reset.Remaining != 0 {
		t.Fatalf("reset Snapshot() = %#v", reset)
	}
}

func TestM223HydrationStateMachineWaitFailureAndCancellation(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(1); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan struct {
		snapshot hatPipeline.HydrationSnapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := machine.Wait(context.Background())
		waitResult <- struct {
			snapshot hatPipeline.HydrationSnapshot
			err      error
		}{snapshot: snapshot, err: err}
	}()
	select {
	case result := <-waitResult:
		t.Fatalf("Wait() returned before completion: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}
	if err := machine.Advance(1); err != nil {
		t.Fatal(err)
	}
	if err := machine.Complete(); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-waitResult:
		if result.err != nil || result.snapshot.State != hatPipeline.HydrationStateReady || result.snapshot.Remaining != 0 {
			t.Fatalf("Wait() result = %#v, want ready", result)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ready state")
	}

	failure := errors.New("snapshot corrupt")
	if err := machine.Begin(2); err != nil {
		t.Fatal(err)
	}
	if err := machine.Fail(failure); err != nil {
		t.Fatal(err)
	}
	failed := machine.Snapshot()
	if failed.State != hatPipeline.HydrationStateFailed || !errors.Is(failed.Failure, failure) || failed.Generation != 2 {
		t.Fatalf("failed Snapshot() = %#v", failed)
	}
	if _, err := machine.Wait(context.Background()); !errors.Is(err, hatPipeline.ErrHydrationFailed) || !errors.Is(err, failure) {
		t.Fatalf("failed Wait() error = %v, want %v", err, hatPipeline.ErrHydrationFailed)
	}
	if err := machine.Begin(1); err != nil {
		t.Fatalf("retry Begin() error = %v", err)
	}
	if err := machine.Advance(1); err != nil {
		t.Fatal(err)
	}
	if err := machine.Complete(); err != nil {
		t.Fatal(err)
	}

	cancelContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := machine.Reset(); err != nil {
		t.Fatal(err)
	}
	cancelResult := make(chan error, 1)
	go func() {
		_, err := machine.Wait(cancelContext)
		cancelResult <- err
	}()
	cancel()
	select {
	case err := <-cancelResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Wait() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
}

func TestM223HydrationStateMachineZeroWorkAndNilInputs(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Begin(0); err != nil {
		t.Fatalf("zero-work Begin() error = %v", err)
	}
	snapshot := machine.Snapshot()
	if snapshot.State != hatPipeline.HydrationStateReady || snapshot.Generation != 1 || snapshot.Total != 0 || snapshot.Remaining != 0 {
		t.Fatalf("zero-work Snapshot() = %#v", snapshot)
	}
	if waited, err := machine.Wait(context.Background()); err != nil || waited.State != hatPipeline.HydrationStateReady {
		t.Fatalf("zero-work Wait() = %#v, %v", waited, err)
	}
	if _, err := machine.Wait(nil); !errors.Is(err, hatPipeline.ErrHydrationContextNil) {
		t.Fatalf("nil-context Wait() error = %v, want %v", err, hatPipeline.ErrHydrationContextNil)
	}

	var nilMachine *hatPipeline.HydrationStateMachine
	if snapshot := nilMachine.Snapshot(); snapshot != (hatPipeline.HydrationSnapshot{}) {
		t.Fatalf("nil Snapshot() = %#v", snapshot)
	}
	if err := nilMachine.Begin(1); !errors.Is(err, hatPipeline.ErrHydrationInvalid) {
		t.Fatalf("nil Begin() error = %v, want %v", err, hatPipeline.ErrHydrationInvalid)
	}
}

func TestM223HydrationStateMachineRejectsInvalidProgress(t *testing.T) {
	machine := hatPipeline.NewHydrationStateMachine()
	if err := machine.Advance(1); !errors.Is(err, hatPipeline.ErrHydrationNotRunning) {
		t.Fatalf("Advance before Begin() error = %v, want %v", err, hatPipeline.ErrHydrationNotRunning)
	}
	if err := machine.Complete(); !errors.Is(err, hatPipeline.ErrHydrationNotRunning) {
		t.Fatalf("Complete before Begin() error = %v, want %v", err, hatPipeline.ErrHydrationNotRunning)
	}
	if err := machine.Fail(errors.New("no run")); !errors.Is(err, hatPipeline.ErrHydrationNotRunning) {
		t.Fatalf("Fail before Begin() error = %v, want %v", err, hatPipeline.ErrHydrationNotRunning)
	}
	if err := machine.Begin(2); err != nil {
		t.Fatal(err)
	}
	if err := machine.Advance(3); !errors.Is(err, hatPipeline.ErrHydrationProgressExceeded) {
		t.Fatalf("overflow Advance() error = %v, want %v", err, hatPipeline.ErrHydrationProgressExceeded)
	}
	if err := machine.Fail(nil); !errors.Is(err, hatPipeline.ErrHydrationFailureNil) {
		t.Fatalf("nil Fail() error = %v, want %v", err, hatPipeline.ErrHydrationFailureNil)
	}
	if err := machine.Reset(); !errors.Is(err, hatPipeline.ErrHydrationAlreadyRunning) {
		t.Fatalf("Reset while hydrating error = %v, want %v", err, hatPipeline.ErrHydrationAlreadyRunning)
	}
}
