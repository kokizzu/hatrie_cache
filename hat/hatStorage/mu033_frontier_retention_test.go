package hatStorage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type mu033RetentionGate struct {
	started chan struct{}
	release chan struct{}
}

func (gate *mu033RetentionGate) WaitUntilSafe(ctx context.Context, frontierID string, boundary uint64) error {
	if frontierID != "orders" || boundary != 42 {
		panic("unexpected retention request")
	}
	select {
	case gate.started <- struct{}{}:
	default:
	}
	select {
	case <-gate.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestMU033CompactionControllerRejectsIncompleteRetentionMetadata(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, queued, err := controller.Submit(CompactionRequest{
		Target:            "orders-part-1",
		RetentionFrontier: "orders",
		Run:               func(context.Context) error { return nil },
	})
	if queued || !errors.Is(err, ErrCompactionRetentionInvalid) {
		t.Fatalf("Submit() = queued=%v err=%v, want retention validation error", queued, err)
	}
}

func TestMU033CompactionControllerWaitsForRetentionGate(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &mu033RetentionGate{started: make(chan struct{}), release: make(chan struct{})}
	var ran atomic.Bool
	job, queued, err := controller.Submit(CompactionRequest{
		Target:            "orders-part-1",
		RetentionGate:     gate,
		RetentionFrontier: "orders",
		RetentionBoundary: 42,
		Run: func(context.Context) error {
			ran.Store(true)
			return nil
		},
	})
	if err != nil || !queued {
		t.Fatalf("Submit() = %#v/%v/%v, want queued job", job, queued, err)
	}

	runDone := make(chan error, 1)
	go func() {
		_, runErr := controller.Run(context.Background())
		runDone <- runErr
	}()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("retention gate was not called")
	}
	if ran.Load() {
		t.Fatal("compaction ran before retention gate released")
	}
	close(gate.release)
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not finish after retention gate release")
	}
	if !ran.Load() {
		t.Fatal("compaction callback did not run")
	}
	status, ok := controller.Status(job.ID)
	if !ok || status.State != CompactionJobSucceeded {
		t.Fatalf("job status = %#v/%v, want succeeded", status, ok)
	}
}

func TestMU033CompactionControllerRetriesCanceledRetentionWait(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gate := &mu033RetentionGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	job, queued, err := controller.Submit(CompactionRequest{
		Target:            "orders-part-2",
		RetentionGate:     gate,
		RetentionFrontier: "orders",
		RetentionBoundary: 42,
		Run:               func(context.Context) error { return nil },
	})
	if err != nil || !queued {
		t.Fatalf("Submit() = %#v/%v/%v, want queued job", job, queued, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) error = %v, want context.Canceled", err)
	}
	status, ok := controller.Status(job.ID)
	if !ok || status.State != CompactionJobRetryPending {
		t.Fatalf("canceled job status = %#v/%v, want retry_pending", status, ok)
	}
	close(gate.release)
	if _, err := controller.Run(context.Background()); err != nil {
		t.Fatalf("Run(retry) error = %v", err)
	}
	status, ok = controller.Status(job.ID)
	if !ok || status.State != CompactionJobSucceeded {
		t.Fatalf("retried job status = %#v/%v, want succeeded", status, ok)
	}
}
