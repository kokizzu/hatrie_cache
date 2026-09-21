package hatPipeline

import (
	"context"
	"errors"
)

var (
	// ErrFrontierTargetReached identifies cancellation caused by reaching the
	// requested lower frontier.
	ErrFrontierTargetReached = errors.New("hatPipeline: frontier target reached")
	// ErrFrontierCancellationNil identifies a nil FrontierCancellation.
	ErrFrontierCancellationNil = errors.New("hatPipeline: frontier cancellation is nil")
)

var closedFrontierCancellationDone = func() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

// FrontierCancellation is a context that is canceled when a named frontier
// reaches a target. It is an opt-in control-plane helper; it does not change
// any query or pipeline default.
type FrontierCancellation struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	registry *FrontierRegistry
	id       string
	target   uint64
}

// NewFrontierCancellation creates a context canceled when registry's lower
// frontier for id reaches target. A nil parent is treated as Background.
// Existing registry and frontier validation errors are returned synchronously.
func NewFrontierCancellation(parent context.Context, registry *FrontierRegistry, id string, target uint64) (*FrontierCancellation, error) {
	if registry == nil {
		return nil, ErrFrontierClosed
	}
	if id == "" {
		return nil, ErrFrontierIDEmpty
	}
	snapshot, ok := registry.Snapshot(id)
	if !ok {
		return nil, ErrFrontierNotFound
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	watcher := &FrontierCancellation{
		ctx:      ctx,
		cancel:   cancel,
		registry: registry,
		id:       id,
		target:   target,
	}
	if snapshot.Lower >= target {
		cancel(ErrFrontierTargetReached)
		return watcher, nil
	}
	if err := ctx.Err(); err != nil {
		cancel(err)
		return watcher, nil
	}
	go watcher.watch()
	return watcher, nil
}

// Context returns the cancellation context for the operation being guarded.
func (cancellation *FrontierCancellation) Context() context.Context {
	if cancellation == nil {
		return context.Background()
	}
	return cancellation.ctx
}

// Done returns the channel closed when the target is reached, the parent is
// canceled, the frontier disappears, or Close is called.
func (cancellation *FrontierCancellation) Done() <-chan struct{} {
	if cancellation == nil {
		return closedFrontierCancellationDone
	}
	return cancellation.ctx.Done()
}

// Wait waits for the guarded context to finish and returns its cancellation
// cause. It is useful for callers that need to distinguish target completion
// from parent cancellation.
func (cancellation *FrontierCancellation) Wait() error {
	if cancellation == nil {
		return ErrFrontierCancellationNil
	}
	<-cancellation.ctx.Done()
	return context.Cause(cancellation.ctx)
}

// Cause returns the cancellation cause, or nil while the context is active.
func (cancellation *FrontierCancellation) Cause() error {
	if cancellation == nil {
		return ErrFrontierCancellationNil
	}
	return context.Cause(cancellation.ctx)
}

// Close stops watching and cancels the guarded context. It is safe to call
// more than once and does not replace an earlier cancellation cause.
func (cancellation *FrontierCancellation) Close() {
	if cancellation == nil {
		return
	}
	cancellation.cancel(context.Canceled)
}

func (cancellation *FrontierCancellation) watch() {
	err := cancellation.registry.WaitUntil(cancellation.ctx, cancellation.id, cancellation.target)
	if err == nil {
		cancellation.cancel(ErrFrontierTargetReached)
		return
	}
	if cancellation.ctx.Err() == nil {
		cancellation.cancel(err)
	}
}
