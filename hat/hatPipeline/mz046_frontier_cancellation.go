package hatPipeline

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrFrontierCancellationNil indicates that a method was called on a nil
	// frontier cancellation watcher.
	ErrFrontierCancellationNil = errors.New("hatPipeline: frontier cancellation watcher is nil")
)

// FrontierCancellation derives a context that is canceled when a named
// frontier reaches a target. Wait returns nil when the target is reached, or
// returns the parent/manual cancellation or registry error that stopped the
// watcher.
//
// The watcher is opt-in and owns one goroutine until Wait returns or Cancel is
// called. Callers should call Cancel when the query finishes before the target
// and then call Wait; a deferred Cancel is useful for early-return paths.
type FrontierCancellation struct {
	// Context is canceled when the target frontier is reached or the watcher
	// stops for another reason.
	Context context.Context

	cancel context.CancelFunc
	done   chan struct{}

	mu  sync.Mutex
	err error
}

// NewFrontierCancellation creates a frontier-triggered cancellation context.
// The frontier must already be registered. A nil parent is treated as a
// background context.
func NewFrontierCancellation(parent context.Context, registry *FrontierRegistry, id string, target uint64) (*FrontierCancellation, error) {
	if registry == nil {
		return nil, ErrFrontierClosed
	}
	if id == "" {
		return nil, ErrFrontierIDEmpty
	}
	registry.mu.RLock()
	closed := registry.closed
	_, exists := registry.objects[id]
	registry.mu.RUnlock()
	if closed {
		return nil, ErrFrontierClosed
	}
	if !exists {
		return nil, ErrFrontierNotFound
	}
	snapshot, _ := registry.Snapshot(id)
	if parent == nil {
		parent = context.Background()
	}
	watchContext, cancel := context.WithCancel(parent)
	watcher := &FrontierCancellation{
		Context: watchContext,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	if snapshot.Lower >= target {
		cancel()
		close(watcher.done)
		return watcher, nil
	}
	go watcher.run(registry, id, target)
	return watcher, nil
}

func (watcher *FrontierCancellation) run(registry *FrontierRegistry, id string, target uint64) {
	err := registry.WaitUntil(watcher.Context, id, target)
	watcher.mu.Lock()
	watcher.err = err
	watcher.mu.Unlock()
	watcher.cancel()
	close(watcher.done)
}

// Cancel stops the watcher. Wait returns context.Canceled unless the watcher
// had already completed for the target or another terminal error.
func (watcher *FrontierCancellation) Cancel() {
	if watcher != nil && watcher.cancel != nil {
		watcher.cancel()
	}
}

// Wait joins the watcher and returns its terminal reason. It is safe to call
// Wait more than once.
func (watcher *FrontierCancellation) Wait() error {
	if watcher == nil {
		return ErrFrontierCancellationNil
	}
	<-watcher.done
	watcher.mu.Lock()
	err := watcher.err
	watcher.mu.Unlock()
	return err
}
