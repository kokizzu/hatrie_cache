package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNamespaceResourceLimitsApplyOnlyTightensQueryOptions(t *testing.T) {
	limits := NamespaceResourceLimits{
		MaxRows:        25,
		MaxResultBytes: 512,
		MaxWorkers:     2,
		Timeout:        time.Second,
	}
	got := limits.Apply(SQLQueryOptions{
		MaxRows:        100,
		MaxResultBytes: 1024,
		Workers:        8,
		Timeout:        5 * time.Second,
	})
	if got.MaxRows != 25 || got.MaxResultBytes != 512 || got.Workers != 2 || got.Timeout != time.Second {
		t.Fatalf("limits were not applied: %+v", got)
	}

	// A namespace policy must never loosen the executor's safe default row cap.
	got = (NamespaceResourceLimits{MaxRows: 200000}).Apply(SQLQueryOptions{})
	if got.MaxRows != 0 {
		t.Fatalf("zero caller MaxRows must retain the executor default, got %d", got.MaxRows)
	}
}

func TestNewNamespaceQueryGovernorRejectsNegativeLimits(t *testing.T) {
	if _, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxRows: -1}, nil); err == nil {
		t.Fatal("negative namespace limit was accepted")
	}
}

func TestNamespaceQueryGovernorAdmitsNamespacesIndependently(t *testing.T) {
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxConcurrentQueries: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	blockingResolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		close(started)
		<-release
		return nil, nil
	})
	go func() {
		_, err := governor.Execute(context.Background(), "alpha", "SELECT * FROM CACHE('items')", blockingResolver, nil, SQLQueryOptions{})
		firstResult <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first namespace query did not reach its resolver")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = governor.Execute(ctx, "alpha", "SELECT * FROM CACHE('items')", SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return nil, nil
	}), nil, SQLQueryOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting query error = %v, want context deadline exceeded", err)
	}

	_, err = governor.Execute(context.Background(), "beta", "SELECT * FROM CACHE('items')", SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return nil, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("independent namespace query failed: %v", err)
	}
	close(release)
	select {
	case err := <-firstResult:
		if err != nil {
			t.Fatalf("first namespace query failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first namespace query did not finish")
	}
}

func TestNamespaceQueryGateAdmitsWaitersFIFO(t *testing.T) {
	gate := newNamespaceQueryGate(1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatalf("initial acquire() error = %v", err)
	}

	started := make(chan int, 2)
	release := make(chan struct{})
	for index := 1; index <= 2; index++ {
		index := index
		go func() {
			if err := gate.acquire(context.Background()); err != nil {
				return
			}
			started <- index
			<-release
			gate.release()
		}()
		waitForNamespaceGateWaiters(t, gate, index)
	}

	gate.release()
	select {
	case got := <-started:
		if got != 1 {
			t.Fatalf("first admitted waiter = %d, want 1", got)
		}
	case <-time.After(time.Second):
		t.Fatal("first waiter was not admitted")
	}
	release <- struct{}{}
	select {
	case got := <-started:
		if got != 2 {
			t.Fatalf("second admitted waiter = %d, want 2", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second waiter was not admitted")
	}
	release <- struct{}{}
}

func TestNamespaceQueryGateSkipsCanceledWaiter(t *testing.T) {
	gate := newNamespaceQueryGate(1)
	if err := gate.acquire(context.Background()); err != nil {
		t.Fatalf("initial acquire() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() { canceled <- gate.acquire(ctx) }()
	waitForNamespaceGateWaiters(t, gate, 1)
	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled acquire() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}

	admitted := make(chan struct{})
	go func() {
		if err := gate.acquire(context.Background()); err == nil {
			close(admitted)
		}
	}()
	waitForNamespaceGateWaiters(t, gate, 1)
	gate.release()
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("live waiter was blocked behind canceled waiter")
	}
	gate.release()
}

func waitForNamespaceGateWaiters(t *testing.T, gate *namespaceQueryGate, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		gate.mu.Lock()
		count := len(gate.waiters)
		gate.mu.Unlock()
		if count >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiting gate waiters = fewer than %d", want)
}
