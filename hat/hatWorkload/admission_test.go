package hatWorkload_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatWorkload"
)

type admissionResult struct {
	lease *hatWorkload.AdmissionLease
	err   error
}

func newAdmissionResult(lease hatWorkload.AdmissionLease, err error) admissionResult {
	if err != nil {
		return admissionResult{err: err}
	}
	return admissionResult{lease: &lease}
}

func TestAdmissionPrioritizesQueuedWorkloads(t *testing.T) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{
		MaxInFlight: 1,
		MaxQueued:   8,
	})
	if err != nil {
		t.Fatalf("NewAdmissionController() error = %v", err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "low", Priority: 1, MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "high", Priority: 10, MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	active, err := controller.Acquire(context.Background(), "low")
	if err != nil {
		t.Fatalf("initial Acquire() error = %v", err)
	}

	lowResult := make(chan admissionResult, 1)
	go func() {
		lease, acquireErr := controller.Acquire(context.Background(), "low")
		lowResult <- newAdmissionResult(lease, acquireErr)
	}()
	highResult := make(chan admissionResult, 1)
	go func() {
		lease, acquireErr := controller.Acquire(context.Background(), "high")
		highResult <- newAdmissionResult(lease, acquireErr)
	}()
	waitForQueued(t, controller, 2)

	active.Release()
	select {
	case result := <-highResult:
		if result.err != nil || result.lease == nil {
			t.Fatalf("high-priority Acquire() = %#v, want lease", result)
		}
		result.lease.Release()
	case <-time.After(time.Second):
		t.Fatal("high-priority waiter was not admitted")
	}
	select {
	case result := <-lowResult:
		if result.err != nil || result.lease == nil {
			t.Fatalf("low-priority Acquire() = %#v, want lease after high priority", result)
		}
		result.lease.Release()
	case <-time.After(time.Second):
		t.Fatal("low-priority waiter was not admitted")
	}
}

func TestAdmissionCancellationDoesNotConsumeCapacity(t *testing.T) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{MaxInFlight: 1, MaxQueued: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "default", MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	active, err := controller.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, acquireErr := controller.Acquire(ctx, "default")
		result <- acquireErr
	}()
	waitForQueued(t, controller, 1)
	cancel()
	select {
	case acquireErr := <-result:
		if !errors.Is(acquireErr, context.Canceled) {
			t.Fatalf("canceled Acquire() error = %v, want context.Canceled", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	active.Release()
	lease, err := controller.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatalf("Acquire() after cancellation = %v/%v, want capacity", lease, err)
	}
	lease.Release()
}

func TestAdmissionPriorityBurstPreventsStarvation(t *testing.T) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{
		MaxInFlight:   1,
		MaxQueued:     8,
		PriorityBurst: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, class := range []hatWorkload.ClassOptions{
		{Name: "seed", Priority: 0, MaxInFlight: 1},
		{Name: "high", Priority: 10, MaxInFlight: 1},
		{Name: "low", Priority: 1, MaxInFlight: 1},
	} {
		if err := controller.RegisterClass(class); err != nil {
			t.Fatal(err)
		}
	}
	seed, err := controller.Acquire(context.Background(), "seed")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan admissionResult, 4)
	for _, class := range []string{"high", "high", "high", "low"} {
		go func(class string) {
			lease, acquireErr := controller.Acquire(context.Background(), class)
			results <- newAdmissionResult(lease, acquireErr)
		}(class)
	}
	waitForQueued(t, controller, 4)
	seed.Release()
	for attempt := 0; attempt < 2; attempt++ {
		result := <-results
		if result.err != nil || result.lease == nil || result.lease.Class() != "high" {
			t.Fatalf("priority result %d = %#v, want high lease", attempt, result)
		}
		result.lease.Release()
	}
	result := <-results
	if result.err != nil || result.lease == nil || result.lease.Class() != "low" {
		t.Fatalf("anti-starvation result = %#v, want low lease", result)
	}
	result.lease.Release()
}

func TestAdmissionRejectsInvalidClassesAndQueueOverflow(t *testing.T) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{MaxInFlight: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Acquire(context.Background(), "missing"); !errors.Is(err, hatWorkload.ErrAdmissionClassNotFound) {
		t.Fatalf("missing class error = %v, want ErrAdmissionClassNotFound", err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "default", MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	active, err := controller.Acquire(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queued := make(chan error, 1)
	go func() {
		_, acquireErr := controller.Acquire(ctx, "default")
		queued <- acquireErr
	}()
	waitForQueued(t, controller, 1)
	if _, err := controller.Acquire(context.Background(), "default"); !errors.Is(err, hatWorkload.ErrAdmissionQueueFull) {
		t.Fatalf("overflow error = %v, want ErrAdmissionQueueFull", err)
	}
	cancel()
	<-queued
	active.Release()
}

func TestAdmissionReleaseIsIdempotentAndSnapshotIsDeterministic(t *testing.T) {
	controller, err := hatWorkload.NewAdmissionController(hatWorkload.AdmissionOptions{MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "z", Priority: 1, MaxInFlight: 2}); err != nil {
		t.Fatal(err)
	}
	if err := controller.RegisterClass(hatWorkload.ClassOptions{Name: "a", Priority: 2, MaxInFlight: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := controller.Acquire(context.Background(), "z")
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	first.Release()
	snapshot := controller.Snapshot()
	if snapshot.Active != 0 || snapshot.Queued != 0 || len(snapshot.Classes) != 2 {
		t.Fatalf("Snapshot() = %#v, want empty active/queue and two classes", snapshot)
	}
	if snapshot.Classes[0].Name != "a" || snapshot.Classes[1].Name != "z" {
		t.Fatalf("class order = %#v, want alphabetical", snapshot.Classes)
	}
}

func waitForQueued(t *testing.T, controller *hatWorkload.AdmissionController, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if controller.Snapshot().Queued >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queued count = %d, want at least %d", controller.Snapshot().Queued, want)
}
