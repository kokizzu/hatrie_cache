package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCHU39WorkloadAdmissionPrioritizesAndBoundsStarvation(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{
		MaxConcurrent:    1,
		MaxPending:       8,
		MaxPriorityBurst: 2,
		Classes: []SQLWorkloadClass{
			{Name: "interactive", Priority: 10},
			{Name: "batch", Priority: 0},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := admission.Acquire(context.Background(), "interactive")
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	order := make([]string, 0, 4)
	done := make(chan error, 4)
	run := func(class string) {
		done <- admission.Run(context.Background(), class, func(context.Context) error {
			mu.Lock()
			order = append(order, class)
			mu.Unlock()
			return nil
		})
	}
	for range 3 {
		go run("interactive")
	}
	go run("batch")

	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := admission.Stats().Pending; got != 4 {
		t.Fatalf("pending = %d, want 4", got)
	}
	hold()
	for range 4 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"interactive", "interactive", "batch", "interactive"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("execution order = %#v, want %#v", order, want)
	}
	if stats := admission.Stats(); stats.Active != 0 || stats.Pending != 0 || stats.Admitted != 5 {
		t.Fatalf("final stats = %#v, want no active/pending and five admissions", stats)
	}
}

func TestCHU39WorkloadAdmissionCancellationAndClose(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{MaxConcurrent: 1, MaxPending: 2})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := admission.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.Run(ctx, "default", func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run error = %v, want context canceled", err)
	}

	closed := make(chan error, 1)
	go func() {
		closed <- admission.Run(context.Background(), "default", func(context.Context) error { return nil })
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	admission.Close()
	if err := <-closed; !errors.Is(err, ErrSQLWorkloadAdmissionClosed) {
		t.Fatalf("closed Run error = %v, want admission closed", err)
	}
	hold()
	if err := admission.Run(context.Background(), "default", func(context.Context) error { return nil }); !errors.Is(err, ErrSQLWorkloadAdmissionClosed) {
		t.Fatalf("Run after Close error = %v, want admission closed", err)
	}
}

func TestCHU39WorkloadAdmissionWakesCapacityWaiter(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{MaxConcurrent: 1, MaxPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	hold, err := admission.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	type acquireResult struct {
		release func()
		err     error
	}
	first := make(chan acquireResult, 1)
	go func() {
		release, acquireErr := admission.Acquire(context.Background(), "default")
		first <- acquireResult{release: release, err: acquireErr}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for admission.Stats().Pending != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := admission.Stats().Pending; got != 1 {
		t.Fatalf("first pending = %d, want 1", got)
	}
	second := make(chan acquireResult, 1)
	go func() {
		release, acquireErr := admission.Acquire(context.Background(), "default")
		second <- acquireResult{release: release, err: acquireErr}
	}()
	hold()
	select {
	case result := <-first:
		if result.err != nil {
			t.Fatal(result.err)
		}
		result.release()
	case <-time.After(2 * time.Second):
		t.Fatal("first queued acquire did not wake")
	}
	select {
	case result := <-second:
		if result.err != nil {
			t.Fatal(result.err)
		}
		result.release()
	case <-time.After(2 * time.Second):
		t.Fatal("capacity-blocked acquire did not wake")
	}
}

func TestCHU39WorkloadAdmissionRunReleasesAfterPanic(t *testing.T) {
	admission, err := NewSQLWorkloadAdmission(SQLWorkloadAdmissionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Run did not propagate callback panic")
			}
		}()
		_ = admission.Run(context.Background(), "default", func(context.Context) error {
			panic("callback panic")
		})
	}()
	completed := make(chan error, 1)
	go func() {
		completed <- admission.Run(context.Background(), "default", func(context.Context) error { return nil })
	}()
	select {
	case err := <-completed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permit was not released after callback panic")
	}
}

func TestCHU39WorkloadAdmissionValidatesOptions(t *testing.T) {
	for _, options := range []SQLWorkloadAdmissionOptions{
		{MaxConcurrent: -1},
		{MaxPending: -1},
		{MaxPriorityBurst: -1},
		{Classes: []SQLWorkloadClass{{Name: "", Priority: 1}}},
		{Classes: []SQLWorkloadClass{{Name: "duplicate"}, {Name: "duplicate"}}},
	} {
		if _, err := NewSQLWorkloadAdmission(options); !errors.Is(err, ErrSQLWorkloadAdmissionOptionsInvalid) {
			t.Fatalf("options %#v error = %v, want invalid-options error", options, err)
		}
	}
}
