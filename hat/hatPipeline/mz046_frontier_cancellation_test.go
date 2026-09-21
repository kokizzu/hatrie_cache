package hatPipeline

import (
	"context"
	"errors"
	"testing"
)

func TestMZ046FrontierCancellationCancelsAtTarget(t *testing.T) {
	registry := newMZ046Registry(t)
	cancellation, err := NewFrontierCancellation(context.Background(), registry, "source", 5)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancellation.Context().Done():
		t.Fatal("cancellation fired before the frontier reached the target")
	default:
	}
	if err := registry.Advance("source", 5, 5); err != nil {
		t.Fatal(err)
	}
	if err := cancellation.Wait(); !errors.Is(err, ErrFrontierTargetReached) {
		t.Fatalf("wait error = %v, want %v", err, ErrFrontierTargetReached)
	}
	if cause := cancellation.Cause(); !errors.Is(cause, ErrFrontierTargetReached) {
		t.Fatalf("cause = %v, want %v", cause, ErrFrontierTargetReached)
	}
}

func TestMZ046FrontierCancellationAlreadyReached(t *testing.T) {
	registry := newMZ046Registry(t)
	if err := registry.Advance("source", 9, 9); err != nil {
		t.Fatal(err)
	}
	cancellation, err := NewFrontierCancellation(context.Background(), registry, "source", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := cancellation.Wait(); !errors.Is(err, ErrFrontierTargetReached) {
		t.Fatalf("wait error = %v, want %v", err, ErrFrontierTargetReached)
	}
}

func TestMZ046FrontierCancellationParentAndClose(t *testing.T) {
	registry := newMZ046Registry(t)
	parent, cancelParent := context.WithCancel(context.Background())
	cancellation, err := NewFrontierCancellation(parent, registry, "source", 5)
	if err != nil {
		t.Fatal(err)
	}
	cancelParent()
	if err := cancellation.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("parent wait error = %v, want %v", err, context.Canceled)
	}

	cancellation, err = NewFrontierCancellation(context.Background(), registry, "source", 5)
	if err != nil {
		t.Fatal(err)
	}
	cancellation.Close()
	if err := cancellation.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close wait error = %v, want %v", err, context.Canceled)
	}
}

func TestMZ046FrontierCancellationFrontierDisappears(t *testing.T) {
	registry := newMZ046Registry(t)
	cancellation, err := NewFrontierCancellation(context.Background(), registry, "source", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Unregister("source"); err != nil {
		t.Fatal(err)
	}
	if err := cancellation.Wait(); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("wait error = %v, want %v", err, ErrFrontierNotFound)
	}
}

func TestMZ046FrontierCancellationValidatesInputs(t *testing.T) {
	registry := newMZ046Registry(t)
	if _, err := NewFrontierCancellation(context.Background(), registry, "missing", 1); !errors.Is(err, ErrFrontierNotFound) {
		t.Fatalf("missing error = %v, want %v", err, ErrFrontierNotFound)
	}
	if _, err := NewFrontierCancellation(context.Background(), registry, "", 1); !errors.Is(err, ErrFrontierIDEmpty) {
		t.Fatalf("empty ID error = %v, want %v", err, ErrFrontierIDEmpty)
	}
	if _, err := NewFrontierCancellation(context.Background(), nil, "source", 1); !errors.Is(err, ErrFrontierClosed) {
		t.Fatalf("nil registry error = %v, want %v", err, ErrFrontierClosed)
	}
}

func newMZ046Registry(t *testing.T) *FrontierRegistry {
	t.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("source"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	return registry
}
