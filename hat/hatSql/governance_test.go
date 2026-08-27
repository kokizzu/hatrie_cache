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
