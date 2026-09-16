package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestSQLIndexRebuildQueueIsDisabledUntilStarted(t *testing.T) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); !errors.Is(err, ErrSQLIndexRebuildQueueDisabled) {
		t.Fatalf("Start() error = %v, want disabled error", err)
	}
	called := make(chan struct{}, 1)
	status, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:       "queued",
		Name:     "people",
		Priority: 1,
		Run: func(context.Context, SQLIndexRebuildProgressFunc) error {
			called <- struct{}{}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.State != SQLIndexRebuildQueued {
		t.Fatalf("status = %#v, want queued", status)
	}
	select {
	case <-called:
		t.Fatal("queued task ran before Start")
	case <-time.After(20 * time.Millisecond):
	}
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("queued")
	if !ok || status.State != SQLIndexRebuildCanceled {
		t.Fatalf("closed queued status = %#v/%v, want canceled", status, ok)
	}
}

func TestSQLIndexRebuildQueuePrioritizesAndReportsProgress(t *testing.T) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 3)
	release := make(chan struct{})
	var mu sync.Mutex
	order := make([]string, 0, 3)
	makeRun := func(id string) SQLIndexRebuildFunc {
		return func(ctx context.Context, progress SQLIndexRebuildProgressFunc) error {
			started <- id
			if id == "first" {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			progress(1, 2)
			progress(2, 2)
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "first", Name: "first", Run: makeRun("first")}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-started:
		if id != "first" {
			t.Fatalf("first started task = %q", id)
		}
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "low", Name: "low", Priority: 1, Run: makeRun("low")}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "high", Name: "high", Priority: 10, Run: makeRun("high")}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	mu.Unlock()
	if want := []string{"first", "high", "low"}; !reflect.DeepEqual(gotOrder, want) {
		t.Fatalf("completion order = %#v, want %#v", gotOrder, want)
	}
	for _, id := range []string{"first", "high", "low"} {
		status, ok := queue.Status(id)
		if !ok || status.State != SQLIndexRebuildSucceeded || status.Completed != 2 || status.Total != 2 {
			t.Fatalf("status(%q) = %#v/%v, want succeeded 2/2", id, status, ok)
		}
	}
}

func TestSQLIndexRebuildQueueWakesAllConfiguredWorkers(t *testing.T) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Capacity: 3, Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 4)
	release := make(chan struct{})
	makeRun := func(id string) SQLIndexRebuildFunc {
		return func(ctx context.Context, _ SQLIndexRebuildProgressFunc) error {
			started <- id
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "anchor", Name: "anchor", Run: makeRun("anchor")}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-started:
		if id != "anchor" {
			t.Fatalf("first started task = %q, want anchor", id)
		}
	case <-time.After(time.Second):
		t.Fatal("anchor task did not start")
	}
	for _, id := range []string{"pending-1", "pending-2", "pending-3"} {
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: id, Name: id, Run: makeRun(id)}); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool, 4)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(seen) < 3 {
		select {
		case id := <-started:
			if id != "anchor" {
				seen[id] = true
			}
		case <-deadline.C:
			t.Fatalf("started pending tasks = %#v, want all three", seen)
		}
	}
	close(release)
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSQLIndexRebuildQueueCancellationAndBoundsAreAtomic(t *testing.T) {
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Capacity: 1, Workers: 1, HistoryCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{
		ID:   "running",
		Name: "running",
		Run: func(ctx context.Context, _ SQLIndexRebuildProgressFunc) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "pending", Name: "pending", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "full", Name: "full", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }}); !errors.Is(err, ErrSQLIndexRebuildQueueFull) {
		t.Fatalf("full enqueue error = %v, want queue-full", err)
	}
	status, err := queue.Cancel("pending")
	if err != nil || status.State != SQLIndexRebuildCanceled {
		t.Fatalf("Cancel(pending) = %#v/%v, want canceled", status, err)
	}
	status, err = queue.Cancel("running")
	if err != nil || !status.CancelRequested {
		t.Fatalf("Cancel(running) = %#v/%v, want cancel requested", status, err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("running")
	if !ok || status.State != SQLIndexRebuildCanceled {
		t.Fatalf("running final status = %#v/%v, want canceled", status, ok)
	}
	if _, err := queue.Cancel("missing"); !errors.Is(err, ErrSQLIndexRebuildTaskNotFound) {
		t.Fatalf("Cancel(missing) error = %v, want not-found", err)
	}
	close(release)
}

func TestSQLIndexRebuildQueueRejectsInvalidRequestsAndBoundsHistory(t *testing.T) {
	if _, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Capacity: -1}); err == nil {
		t.Fatal("negative capacity accepted")
	}
	queue, err := NewSQLIndexRebuildQueue(SQLIndexRebuildQueueOptions{Workers: 1, HistoryCapacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	invalid := []SQLIndexRebuildRequest{
		{Name: "missing-id", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }},
		{ID: "missing-name", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }},
		{ID: "missing-run", Name: "missing-run"},
	}
	for _, request := range invalid {
		if _, err := queue.Enqueue(request); err == nil {
			t.Fatalf("invalid request %#v accepted", request)
		}
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "duplicate", Name: "duplicate", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: "duplicate", Name: "duplicate", Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }}); !errors.Is(err, ErrSQLIndexRebuildTaskExists) {
		t.Fatalf("duplicate enqueue error = %v, want duplicate", err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"one", "two", "three"} {
		if _, err := queue.Enqueue(SQLIndexRebuildRequest{ID: id, Name: id, Run: func(context.Context, SQLIndexRebuildProgressFunc) error { return nil }}); err != nil {
			t.Fatal(err)
		}
		if err := queue.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := queue.Status("three"); !ok {
		t.Fatal("most recent completed status was evicted too early")
	}
	if _, ok := queue.Status("duplicate"); ok {
		t.Fatal("old completed status was retained beyond history")
	}
	if statuses := queue.Snapshot(); len(statuses) > 2 {
		t.Fatalf("status history length = %d, want at most 2", len(statuses))
	}
}
