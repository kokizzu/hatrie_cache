package hatPipeline

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMU35SnapshotBarrierBlocksUntilAllPrerequisitesReady(t *testing.T) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"index", "source"}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- barrier.Wait(context.Background()) }()

	if err := barrier.MarkReady("source"); err != nil {
		t.Fatal(err)
	}
	status := barrier.Status()
	if status.State != SnapshotBarrierPending || !reflect.DeepEqual(status.Pending, []string{"index"}) {
		t.Fatalf("pending status = %#v, want only index pending", status)
	}

	select {
	case err := <-result:
		t.Fatalf("barrier released early with %v", err)
	default:
	}
	if err := barrier.MarkReady("index"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("barrier did not release after all prerequisites became ready")
	}
	status = barrier.Status()
	if status.State != SnapshotBarrierReady || !reflect.DeepEqual(status.Ready, []string{"index", "source"}) || len(status.Pending) != 0 {
		t.Fatalf("ready status = %#v", status)
	}
}

func TestMU35SnapshotBarrierFailureCancellationAndReset(t *testing.T) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"source"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := barrier.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v", err)
	}
	failure := errors.New("source snapshot failed")
	if err := barrier.MarkFailed("source", failure); err != nil {
		t.Fatal(err)
	}
	if err := barrier.Wait(context.Background()); !errors.Is(err, ErrSnapshotBarrierFailed) || !strings.Contains(err.Error(), failure.Error()) {
		t.Fatalf("failed Wait() error = %v", err)
	}
	if err := barrier.MarkReady("source"); !errors.Is(err, ErrSnapshotBarrierFailed) {
		t.Fatalf("MarkReady after failure = %v", err)
	}
	if err := barrier.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := barrier.MarkReady("source"); err != nil {
		t.Fatal(err)
	}
	if err := barrier.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after reset = %v", err)
	}
	if barrier.Status().Epoch != 2 {
		t.Fatalf("epoch = %d, want 2", barrier.Status().Epoch)
	}
}

func TestMU35SnapshotBarrierCloseWakesWaitersAndIsIdempotent(t *testing.T) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"source"}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- barrier.Wait(context.Background()) }()
	if err := barrier.Close(); err != nil {
		t.Fatal(err)
	}
	if err := barrier.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrSnapshotBarrierClosed) {
			t.Fatalf("closed Wait() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not wake waiter")
	}
	if err := barrier.MarkReady("source"); !errors.Is(err, ErrSnapshotBarrierClosed) {
		t.Fatalf("MarkReady after close = %v", err)
	}
}

func TestMU35SnapshotBarrierValidatesBoundsAndUnknownNames(t *testing.T) {
	for _, options := range []SnapshotBarrierOptions{
		{Prerequisites: []string{"source", "source"}},
		{Prerequisites: []string{""}},
		{Prerequisites: []string{strings.Repeat("x", MaxSnapshotBarrierNameBytes+1)}},
		{MaxPrerequisites: MaxSnapshotBarrierPrerequisites + 1},
	} {
		if _, err := NewSnapshotBarrier(options); !errors.Is(err, ErrSnapshotBarrierOptions) {
			t.Fatalf("options %#v error = %v, want options error", options, err)
		}
	}
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{Prerequisites: []string{"source"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.MarkReady("unknown"); !errors.Is(err, ErrSnapshotBarrierUnknownPrerequisite) {
		t.Fatalf("unknown MarkReady error = %v", err)
	}
	if err := barrier.MarkFailed("source", nil); !errors.Is(err, ErrSnapshotBarrierFailureRequired) {
		t.Fatalf("nil MarkFailed error = %v", err)
	}
	var nilBarrier *SnapshotBarrier
	if err := nilBarrier.Wait(context.Background()); !errors.Is(err, ErrSnapshotBarrierNil) {
		t.Fatalf("nil Wait() error = %v", err)
	}
}

func TestMU35SnapshotBarrierEmptySetIsReady(t *testing.T) {
	barrier, err := NewSnapshotBarrier(SnapshotBarrierOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.Wait(context.Background()); err != nil {
		t.Fatalf("empty Wait() error = %v", err)
	}
	if barrier.Status().State != SnapshotBarrierReady {
		t.Fatalf("empty state = %v, want ready", barrier.Status().State)
	}
}
