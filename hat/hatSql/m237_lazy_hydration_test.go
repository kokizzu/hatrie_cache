package hatSql

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestM237LazyHydrationSingleFlightsAndInvalidates(t *testing.T) {
	registry := NewSQLLazyHydrationRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	if err := registry.Register(SQLMaintainedObject{
		Kind:         SQLMaintainedObjectView,
		Name:         "orders_view",
		Dependencies: []string{"orders"},
	}, func(ctx context.Context, object SQLMaintainedObject) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			close(started)
		}
		object.Dependencies[0] = "mutated-by-hydrator"
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- registry.Ensure(context.Background(), SQLMaintainedObjectView, "orders_view") }()
	<-started
	secondDone := make(chan error, 1)
	go func() { secondDone <- registry.Ensure(context.Background(), SQLMaintainedObjectView, "orders_view") }()
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Ensure() error = %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second Ensure() error = %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("hydration calls = %d, want one single-flight call", got)
	}

	snapshot := registry.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Dependencies[0] != "orders" {
		t.Fatalf("Snapshot() = %#v, want detached metadata", snapshot)
	}
	if err := registry.Invalidate(SQLMaintainedObjectView, "orders_view"); err != nil {
		t.Fatalf("Invalidate() error = %v", err)
	}
	if err := registry.Ensure(context.Background(), SQLMaintainedObjectView, "orders_view"); err != nil {
		t.Fatalf("post-invalidation Ensure() error = %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("post-invalidation hydration calls = %d, want 2", got)
	}
}

func TestM237LazyHydrationCanceledWaiterDoesNotCancelOwner(t *testing.T) {
	registry := NewSQLLazyHydrationRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	if err := registry.Register(SQLMaintainedObject{Kind: SQLMaintainedObjectIndex, Name: "orders_by_email", Dependencies: []string{"orders"}}, func(ctx context.Context, _ SQLMaintainedObject) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- registry.Ensure(context.Background(), SQLMaintainedObjectIndex, "orders_by_email")
	}()
	<-started

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- registry.Ensure(waiterCtx, SQLMaintainedObjectIndex, "orders_by_email") }()
	cancel()
	select {
	case err := <-waiterDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner Ensure() error = %v", err)
	}
}

func TestM237LazyHydrationValidatesRegistrationAndRetriesAfterFailure(t *testing.T) {
	registry := NewSQLLazyHydrationRegistry()
	if err := registry.Register(SQLMaintainedObject{Kind: SQLMaintainedObjectView, Name: "missing", Dependencies: []string{"source"}}, nil); !errors.Is(err, ErrSQLLazyHydrationRequired) {
		t.Fatalf("nil hydrator error = %v, want %v", err, ErrSQLLazyHydrationRequired)
	}
	if err := registry.Ensure(context.Background(), SQLMaintainedObjectView, "missing"); !errors.Is(err, ErrSQLLazyHydrationNotFound) {
		t.Fatalf("unknown Ensure() error = %v, want %v", err, ErrSQLLazyHydrationNotFound)
	}
	var calls int32
	wantErr := errors.New("hydrate failed")
	if err := registry.Register(SQLMaintainedObject{Kind: SQLMaintainedObjectView, Name: "retry", Dependencies: []string{"source"}}, func(context.Context, SQLMaintainedObject) error {
		if atomic.AddInt32(&calls, 1) == 1 {
			return wantErr
		}
		return nil
	}); err != nil {
		t.Fatalf("retry Register() error = %v", err)
	}
	if err := registry.Register(SQLMaintainedObject{Kind: SQLMaintainedObjectView, Name: "retry", Dependencies: []string{"other"}}, func(context.Context, SQLMaintainedObject) error { return nil }); !errors.Is(err, ErrSQLLazyHydrationAlreadyRegistered) {
		t.Fatalf("duplicate Register() error = %v, want %v", err, ErrSQLLazyHydrationAlreadyRegistered)
	}
	if err := registry.Ensure(context.Background(), SQLMaintainedObjectView, "retry"); !errors.Is(err, wantErr) {
		t.Fatalf("failed Ensure() error = %v, want %v", err, wantErr)
	}
	if err := registry.Ensure(context.Background(), SQLMaintainedObjectView, "retry"); err != nil {
		t.Fatalf("retry Ensure() error = %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("retry hydration calls = %d, want 2", got)
	}
}
