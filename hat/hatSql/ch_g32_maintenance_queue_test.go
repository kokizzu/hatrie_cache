package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestCHG32MaintenanceQueuePrioritizesKindsAndReportsProgress(t *testing.T) {
	queue, err := NewSQLMaintenanceQueue(SQLMaintenanceQueueOptions{
		Capacity:        4,
		Workers:         1,
		HistoryCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer queue.Close()

	var mu sync.Mutex
	var order []SQLMaintenanceJobKind
	run := func(kind SQLMaintenanceJobKind) SQLMaintenanceRunFunc {
		return func(_ context.Context, progress SQLMaintenanceProgressFunc) error {
			progress(1, 1)
			mu.Lock()
			order = append(order, kind)
			mu.Unlock()
			return nil
		}
	}
	if _, err := queue.Enqueue(SQLMaintenanceJobRequest{
		ID:       "merge-1",
		Name:     "merge old parts",
		Kind:     SQLMaintenanceJobMerge,
		Priority: 1,
		Run:      run(SQLMaintenanceJobMerge),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(SQLMaintenanceJobRequest{
		ID:       "optimize-1",
		Name:     "optimize orders",
		Kind:     SQLMaintenanceJobOptimize,
		Priority: 10,
		Run:      run(SQLMaintenanceJobOptimize),
		Verify: func(context.Context) error {
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	gotOrder := append([]SQLMaintenanceJobKind(nil), order...)
	mu.Unlock()
	if want := []SQLMaintenanceJobKind{SQLMaintenanceJobOptimize, SQLMaintenanceJobMerge}; !reflect.DeepEqual(gotOrder, want) {
		t.Fatalf("execution order = %#v, want %#v", gotOrder, want)
	}
	optimize, ok := queue.Status("optimize-1")
	if !ok {
		t.Fatal("optimized job status missing")
	}
	if optimize.State != SQLMaintenanceJobSucceeded || !optimize.Verified || optimize.Completed != 1 || optimize.Total != 1 || optimize.Name != "optimize orders" {
		t.Fatalf("optimized status = %#v, want succeeded, verified, and 1/1 progress", optimize)
	}
	if optimize.Kind != SQLMaintenanceJobOptimize {
		t.Fatalf("optimized kind = %q, want %q", optimize.Kind, SQLMaintenanceJobOptimize)
	}
	merge, ok := queue.Status("merge-1")
	if !ok || merge.State != SQLMaintenanceJobSucceeded || merge.Kind != SQLMaintenanceJobMerge {
		t.Fatalf("merge status = %#v/%v, want succeeded merge", merge, ok)
	}
	if statuses := queue.Snapshot(); len(statuses) != 2 {
		t.Fatalf("snapshot length = %d, want 2", len(statuses))
	}
}

func TestCHG32MaintenanceQueueIsDisabledByDefault(t *testing.T) {
	queue, err := NewSQLMaintenanceQueue(SQLMaintenanceQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := queue.Enqueue(SQLMaintenanceJobRequest{
		ID:   "disabled-1",
		Name: "disabled",
		Kind: SQLMaintenanceJobMutation,
		Run:  func(context.Context, SQLMaintenanceProgressFunc) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); !errors.Is(err, ErrSQLMaintenanceQueueDisabled) {
		t.Fatalf("Start() error = %v, want disabled error", err)
	}
	if err := queue.Flush(context.Background()); !errors.Is(err, ErrSQLMaintenanceQueueDisabled) {
		t.Fatalf("Flush() error = %v, want disabled error", err)
	}
}

func TestCHG32MaintenanceQueueCancelsQueuedWorkAndEnforcesCapacity(t *testing.T) {
	queue, err := NewSQLMaintenanceQueue(SQLMaintenanceQueueOptions{Capacity: 1, HistoryCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	request := SQLMaintenanceJobRequest{
		ID:   "queued-1",
		Name: "queued optimize",
		Kind: SQLMaintenanceJobOptimize,
		Run:  func(context.Context, SQLMaintenanceProgressFunc) error { return nil },
	}
	if _, err := queue.Enqueue(request); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(SQLMaintenanceJobRequest{
		ID:   "queued-2",
		Name: "queued merge",
		Kind: SQLMaintenanceJobMerge,
		Run:  request.Run,
	}); !errors.Is(err, ErrSQLMaintenanceQueueFull) {
		t.Fatalf("second enqueue error = %v, want full error", err)
	}
	status, err := queue.Cancel(request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != SQLMaintenanceJobCanceled || status.Kind != SQLMaintenanceJobOptimize || status.Name != request.Name {
		t.Fatalf("canceled status = %#v, want canceled optimize", status)
	}
	if snapshot := queue.Snapshot(); len(snapshot) != 1 || snapshot[0].State != SQLMaintenanceJobCanceled {
		t.Fatalf("snapshot after cancellation = %#v, want one canceled history entry", snapshot)
	}
}
