package hatStorage

import (
	"context"
	"errors"
	"testing"
)

func TestCHU35CompactionControllerBoundsAndReportsLifecycle(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{
		MaxPending:      1,
		HistoryCapacity: 2,
	})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	runs := 0
	first, accepted, err := controller.Submit(CompactionRequest{
		Target:         "events/part-1",
		Priority:       7,
		EstimatedBytes: 4096,
		Run: func(context.Context) error {
			runs++
			return nil
		},
	})
	if err != nil || !accepted || first.ID == 0 || first.State != CompactionJobPending {
		t.Fatalf("Submit(first) = %#v, %v, %v", first, accepted, err)
	}
	duplicate, accepted, err := controller.Submit(CompactionRequest{
		Target: " events/part-1 ",
		Run:    func(context.Context) error { return errors.New("must not replace duplicate") },
	})
	if err != nil || accepted || duplicate.ID != first.ID {
		t.Fatalf("Submit(duplicate) = %#v, %v, %v; want existing job", duplicate, accepted, err)
	}
	if _, _, err := controller.Submit(CompactionRequest{
		Target: "events/part-2",
		Run:    func(context.Context) error { return nil },
	}); !errors.Is(err, ErrCompactionControllerQueueFull) {
		t.Fatalf("Submit(over capacity) error = %v, want ErrCompactionControllerQueueFull", err)
	}
	if status, ok := controller.Status(first.ID); !ok || status.State != CompactionJobPending || status.Target != "events/part-1" {
		t.Fatalf("Status(before Run) = %#v/%v, want pending job", status, ok)
	}
	run, err := controller.Run(context.Background())
	if err != nil || run.Scheduled != 1 || run.Completed != 1 || runs != 1 {
		t.Fatalf("Run() = %#v/%v with runs=%d", run, err, runs)
	}
	status, ok := controller.Status(first.ID)
	if !ok || status.State != CompactionJobSucceeded || status.Attempts != 1 || status.LastError != "" {
		t.Fatalf("Status(after Run) = %#v/%v, want succeeded", status, ok)
	}
}

func TestCHU35CompactionControllerContextCancellationRequeues(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	attempts := 0
	job, accepted, err := controller.Submit(CompactionRequest{
		Target: "events/part-cancel",
		Run: func(ctx context.Context) error {
			attempts++
			return ctx.Err()
		},
	})
	if err != nil || !accepted {
		t.Fatalf("Submit() = %#v, %v, %v", job, accepted, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.Run(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(canceled) error = %v, want context.Canceled", err)
	}
	status, ok := controller.Status(job.ID)
	if !ok || status.State != CompactionJobRetryPending || status.Attempts != 1 {
		t.Fatalf("Status(after cancellation) = %#v/%v, want retry_pending", status, ok)
	}
	if _, err := controller.Run(context.Background()); err != nil || attempts != 2 {
		t.Fatalf("Run(retry) = %v with attempts=%d", err, attempts)
	}
	status, ok = controller.Status(job.ID)
	if !ok || status.State != CompactionJobSucceeded || status.Attempts != 2 {
		t.Fatalf("Status(after retry) = %#v/%v, want succeeded", status, ok)
	}
}

func TestCHU35CompactionControllerHistoryIsBounded(t *testing.T) {
	controller, err := NewCompactionController(CompactionControllerOptions{HistoryCapacity: 2})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	var jobs []CompactionJob
	for index := 0; index < 3; index++ {
		job, accepted, err := controller.Submit(CompactionRequest{
			Target: "events/history-" + string(rune('a'+index)),
			Run:    func(context.Context) error { return nil },
		})
		if err != nil || !accepted {
			t.Fatalf("Submit(%d) = %#v, %v, %v", index, job, accepted, err)
		}
		jobs = append(jobs, job)
		if _, err := controller.Run(context.Background()); err != nil {
			t.Fatalf("Run(%d) error = %v", index, err)
		}
	}
	if _, ok := controller.Status(jobs[0].ID); ok {
		t.Fatal("old terminal job remained after history capacity was exceeded")
	}
	if got := len(controller.List(0)); got != 2 {
		t.Fatalf("List() length = %d, want bounded history length 2", got)
	}
}

func TestCHU35CompactionControllerValidatesAndBoundsErrors(t *testing.T) {
	if _, err := NewCompactionController(CompactionControllerOptions{MaxPending: -1}); !errors.Is(err, ErrCompactionControllerOptionsInvalid) {
		t.Fatalf("negative MaxPending error = %v, want ErrCompactionControllerOptionsInvalid", err)
	}
	var nilController *CompactionController
	if _, _, err := nilController.Submit(CompactionRequest{Target: "x", Run: func(context.Context) error { return nil }}); !errors.Is(err, ErrCompactionControllerNil) {
		t.Fatalf("nil Submit() error = %v, want ErrCompactionControllerNil", err)
	}
	if _, err := nilController.Run(context.Background()); !errors.Is(err, ErrCompactionControllerNil) {
		t.Fatalf("nil Run() error = %v, want ErrCompactionControllerNil", err)
	}

	controller, err := NewCompactionController(CompactionControllerOptions{})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	job, accepted, err := controller.Submit(CompactionRequest{
		Target: "events/error",
		Run: func(context.Context) error {
			return errors.New("a very long compaction error that should be bounded by the controller status record")
		},
	})
	if err != nil || !accepted {
		t.Fatalf("Submit() = %#v, %v, %v", job, accepted, err)
	}
	if _, err := controller.Run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want callback error")
	}
	status, ok := controller.Status(job.ID)
	if !ok || status.State != CompactionJobRetryPending || len(status.LastError) > maxCompactionControllerErrorBytes {
		t.Fatalf("Status(after error) = %#v/%v, want bounded retry_pending", status, ok)
	}
}
