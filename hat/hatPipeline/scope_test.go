package hatPipeline

import (
	"context"
	"errors"
	"testing"
)

func TestScopeWaitsForNestedWorkersAndPropagatesCancellation(t *testing.T) {
	scope := NewScope(context.Background())
	started := make(chan struct{})
	if err := scope.GoChild(func(child *Scope) error {
		if err := child.Go(func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}); err != nil {
			return err
		}
		<-child.Context().Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	scope.Cancel()
	if err := scope.Wait(); err != nil {
		t.Fatalf("Scope.Wait() error = %v, want nil", err)
	}
}

func TestScopeReturnsFirstWorkerErrorAndClosesAdmissions(t *testing.T) {
	scope := NewScope(context.Background())
	want := errors.New("worker failed")
	if err := scope.Go(func(context.Context) error { return want }); err != nil {
		t.Fatal(err)
	}
	if err := scope.Wait(); !errors.Is(err, want) {
		t.Fatalf("Scope.Wait() error = %v, want %v", err, want)
	}
	if err := scope.Go(func(context.Context) error { return nil }); !errors.Is(err, ErrScopeClosed) {
		t.Fatalf("Go() after Wait() error = %v, want %v", err, ErrScopeClosed)
	}
}
