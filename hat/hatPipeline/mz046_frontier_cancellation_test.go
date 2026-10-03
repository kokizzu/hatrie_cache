package hatPipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMZ046FrontierCancellationCancelsAtTarget(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}

	watcher, err := NewFrontierCancellation(context.Background(), registry, "orders", 5)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-watcher.Context.Done():
		t.Fatal("watcher canceled before frontier target")
	default:
	}
	if err := registry.Advance("orders", 5, 5); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("Wait() error = %v, want nil after target", err)
	}
	select {
	case <-watcher.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("watcher context was not canceled at target")
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("second Wait() error = %v, want nil", err)
	}
}

func TestMZ046FrontierCancellationHandlesAlreadyCoveredTarget(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("orders", 9, 9); err != nil {
		t.Fatal(err)
	}

	watcher, err := NewFrontierCancellation(context.Background(), registry, "orders", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := watcher.Wait(); err != nil {
		t.Fatalf("Wait() error = %v, want nil for already covered target", err)
	}
	select {
	case <-watcher.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("already covered watcher context was not canceled")
	}
}

func TestMZ046FrontierCancellationReportsParentAndManualCancellation(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}

	parent, cancelParent := context.WithCancel(context.Background())
	parentWatcher, err := NewFrontierCancellation(parent, registry, "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	cancelParent()
	if err := parentWatcher.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent cancellation error = %v, want context.Canceled", err)
	}

	manualWatcher, err := NewFrontierCancellation(context.Background(), registry, "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	manualWatcher.Cancel()
	if err := manualWatcher.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("manual cancellation error = %v, want context.Canceled", err)
	}
}

func TestMZ046FrontierCancellationReportsRegistryErrors(t *testing.T) {
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("orders"); err != nil {
		t.Fatal(err)
	}
	watcher, err := NewFrontierCancellation(context.Background(), registry, "orders", 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Wait(); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("registry close error = %v, want ErrFrontierClosed", err)
	}

	if _, err := NewFrontierCancellation(context.Background(), registry, "orders", 1); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("new watcher on closed registry error = %v, want ErrFrontierClosed", err)
	}
}

func TestMZ046FrontierCancellationValidatesInputs(t *testing.T) {
	if _, err := NewFrontierCancellation(context.Background(), nil, "orders", 1); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("nil registry error = %v, want ErrFrontierClosed", err)
	}
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewFrontierCancellation(context.Background(), registry, "", 1); !errors.Is(err, ErrFrontierIDEmpty) {
		t.Fatalf("empty ID error = %v, want ErrFrontierIDEmpty", err)
	}
	if _, err := NewFrontierCancellation(context.Background(), registry, "missing", 1); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("missing ID error = %v, want ErrFrontierNotFound", err)
	}
	var nilWatcher *FrontierCancellation
	if err := nilWatcher.Wait(); !errors.Is(err, ErrFrontierCancellationNil) {
		t.Fatalf("nil watcher Wait() error = %v, want ErrFrontierCancellationNil", err)
	}
}
