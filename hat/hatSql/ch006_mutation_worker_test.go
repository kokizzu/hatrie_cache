package hatSql

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSQLMutationDependencyQueueRunReady(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 8)
	if err != nil {
		t.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	defer queue.Close()
	for _, task := range []SQLMutationTask{
		{ID: "load"},
		{ID: "index", DependsOn: []string{"load"}},
		{ID: "purge"},
	} {
		if err := queue.Add(task); err != nil {
			t.Fatalf("Add(%q) error = %v", task.ID, err)
		}
	}

	var calls []string
	workerErr := errors.New("temporary worker failure")
	stats, err := queue.RunReady(context.Background(), 0, func(_ context.Context, task SQLMutationTaskRecord) error {
		calls = append(calls, task.ID)
		if task.ID == "purge" {
			return workerErr
		}
		return nil
	})
	if !errors.Is(err, workerErr) {
		t.Fatalf("RunReady() error = %v, want worker error", err)
	}
	if want := (SQLMutationDependencyQueueRunStats{Claimed: 2, Completed: 1, Failed: 1}); stats != want {
		t.Fatalf("RunReady() stats = %#v, want %#v", stats, want)
	}
	if want := []string{"load", "purge"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("RunReady() calls = %#v, want %#v", calls, want)
	}

	if err := queue.Retry("purge"); err != nil {
		t.Fatalf("Retry(purge) error = %v", err)
	}
	calls = calls[:0]
	stats, err = queue.RunReady(context.Background(), 0, func(_ context.Context, task SQLMutationTaskRecord) error {
		calls = append(calls, task.ID)
		return nil
	})
	if err != nil {
		t.Fatalf("second RunReady() error = %v", err)
	}
	if want := (SQLMutationDependencyQueueRunStats{Claimed: 2, Completed: 2}); stats != want {
		t.Fatalf("second RunReady() stats = %#v, want %#v", stats, want)
	}
	if want := []string{"index", "purge"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("second RunReady() calls = %#v, want %#v", calls, want)
	}
	for _, task := range queue.Snapshot() {
		if task.State != SQLMutationTaskCompleted {
			t.Fatalf("task %q state = %q, want completed", task.ID, task.State)
		}
	}
}

func TestSQLMutationDependencyQueueRunReadyCancellationAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 4)
	if err != nil {
		t.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	defer queue.Close()
	if err := queue.Add(SQLMutationTask{ID: "load"}); err != nil {
		t.Fatalf("Add(load) error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stats, err := queue.RunReady(ctx, 1, func(context.Context, SQLMutationTaskRecord) error {
		t.Fatal("canceled RunReady() invoked the worker")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled RunReady() error = %v, want context.Canceled", err)
	}
	if stats != (SQLMutationDependencyQueueRunStats{}) {
		t.Fatalf("canceled RunReady() stats = %#v, want zero", stats)
	}
	if task, ok := queue.Task("load"); !ok || task.State != SQLMutationTaskPending {
		t.Fatalf("canceled task = %#v/%v, want pending", task, ok)
	}

	if _, err := queue.RunReady(context.Background(), 1, nil); !errors.Is(err, ErrSQLMutationDependencyQueueHandlerNil) {
		t.Fatalf("nil handler error = %v, want ErrSQLMutationDependencyQueueHandlerNil", err)
	}
}

func TestSQLMutationDependencyQueueRunReadyBoundsFailureReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 2)
	if err != nil {
		t.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	defer queue.Close()
	if err := queue.Add(SQLMutationTask{ID: "load"}); err != nil {
		t.Fatalf("Add(load) error = %v", err)
	}
	workerErr := errors.New(strings.Repeat("x", maxSQLMutationTaskErrorBytes+128))
	stats, err := queue.RunReady(context.Background(), 1, func(context.Context, SQLMutationTaskRecord) error {
		return workerErr
	})
	if !errors.Is(err, workerErr) {
		t.Fatalf("long-error RunReady() error = %v, want worker error", err)
	}
	if stats != (SQLMutationDependencyQueueRunStats{Claimed: 1, Failed: 1}) {
		t.Fatalf("long-error RunReady() stats = %#v, want one failure", stats)
	}
	task, ok := queue.Task("load")
	if !ok || task.State != SQLMutationTaskFailed {
		t.Fatalf("long-error task = %#v/%v, want failed", task, ok)
	}
	if len(task.LastError) != maxSQLMutationTaskErrorBytes {
		t.Fatalf("long-error diagnostic length = %d, want %d", len(task.LastError), maxSQLMutationTaskErrorBytes)
	}
}

func BenchmarkSQLMutationDependencyQueueRunReady(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		queue := newSQLMutationWorkerBenchmarkQueue(b)
		b.StartTimer()
		stats, err := queue.RunReady(context.Background(), 0, func(context.Context, SQLMutationTaskRecord) error {
			return nil
		})
		if err != nil {
			b.Fatalf("RunReady() error = %v", err)
		}
		if stats.Claimed != 64 || stats.Completed != 64 || stats.Failed != 0 {
			b.Fatalf("RunReady() stats = %#v, want 64 completed", stats)
		}
		b.StopTimer()
		if err := queue.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
	}
}
