package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

// ErrScopeClosed reports an attempt to add work after a scope began waiting.
var ErrScopeClosed = errors.New("hatPipeline: scope is closed")

// ErrScopeInvalid reports a nil scope or worker callback.
var ErrScopeInvalid = errors.New("hatPipeline: invalid scope")

// Scope owns a cancellable group of workers. A child scope inherits its
// parent's cancellation and is waited as one parent worker, which makes
// nested dataflow operators finish before their enclosing operator returns.
type Scope struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	err    error
	wg     sync.WaitGroup

	waitOnce sync.Once
	done     chan struct{}
}

// NewScope creates a cancellable worker scope. A nil parent uses a background
// context.
func NewScope(parent context.Context) *Scope {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Scope{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// Context returns the context shared by all workers in the scope.
func (scope *Scope) Context() context.Context {
	if scope == nil {
		return nil
	}
	return scope.ctx
}

// Go starts one worker. The first worker error cancels the scope and is
// returned by Wait. Go rejects work after Wait or Close begins.
func (scope *Scope) Go(worker func(context.Context) error) error {
	if scope == nil || worker == nil {
		return ErrScopeInvalid
	}
	scope.mu.Lock()
	if scope.closed {
		scope.mu.Unlock()
		return ErrScopeClosed
	}
	scope.wg.Add(1)
	ctx := scope.ctx
	scope.mu.Unlock()
	go func() {
		defer scope.wg.Done()
		if err := worker(ctx); err != nil {
			scope.recordError(err)
		}
	}()
	return nil
}

// GoChild starts a nested scope as one worker in the parent. The child is
// always waited before the parent worker completes, including on callback
// error.
func (scope *Scope) GoChild(worker func(*Scope) error) error {
	if worker == nil {
		return ErrScopeInvalid
	}
	return scope.Go(func(ctx context.Context) error {
		child := NewScope(ctx)
		if err := worker(child); err != nil {
			child.Cancel()
			_ = child.Wait()
			return err
		}
		return child.Wait()
	})
}

// Cancel asks all workers in this scope and its descendants to stop.
func (scope *Scope) Cancel() {
	if scope != nil && scope.cancel != nil {
		scope.cancel()
	}
}

// Wait closes admissions, waits for all workers, and returns the first worker
// error. It is safe to call more than once. Callers must not call Wait from a
// worker belonging to the same scope.
func (scope *Scope) Wait() error {
	if scope == nil {
		return ErrScopeInvalid
	}
	scope.waitOnce.Do(func() {
		scope.mu.Lock()
		scope.closed = true
		scope.mu.Unlock()
		scope.wg.Wait()
		scope.cancel()
		close(scope.done)
	})
	<-scope.done
	scope.mu.Lock()
	err := scope.err
	scope.mu.Unlock()
	return err
}

// Close cancels and waits for all workers in the scope.
func (scope *Scope) Close() error {
	if scope == nil {
		return ErrScopeInvalid
	}
	scope.Cancel()
	return scope.Wait()
}

func (scope *Scope) recordError(err error) {
	scope.mu.Lock()
	if scope.err == nil {
		scope.err = err
		scope.cancel()
	}
	scope.mu.Unlock()
}
