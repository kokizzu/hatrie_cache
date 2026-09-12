package hatPeer

import (
	"context"
	"time"
)

// DoWithLifecycleContext is like Do, but also cancels the handler context when
// the pool is closed. Use it for peer operations that must not outlive the
// pool; the caller's cancellation and deadline are preserved as well.
func (pool *ConnectionPool) DoWithLifecycleContext(ctx context.Context, fn func(context.Context, Connection) error) error {
	if pool == nil {
		return ErrConnectionPoolNil
	}
	if fn == nil {
		return ErrConnectionPoolHandlerRequired
	}
	if ctx == nil {
		return ErrConnectionPoolContextRequired
	}
	if err := pool.begin(); err != nil {
		return err
	}
	defer pool.end()

	callContext, cancel, stopPoolCancel := pool.lifecycleContext(ctx)
	if cancel != nil {
		defer cancel()
		defer stopPoolCancel()
	}
	connection, err := pool.acquire(callContext)
	if err != nil {
		return err
	}

	reusable := false
	defer func() {
		_ = pool.release(connection, reusable)
	}()
	err = fn(callContext, connection)
	reusable = err == nil
	return err
}

func (pool *ConnectionPool) lifecycleContext(ctx context.Context) (context.Context, context.CancelFunc, func() bool) {
	if ctx == context.Background() || ctx == context.TODO() {
		return pool.poolCtx, nil, nil
	}
	if ctx.Done() == nil {
		return connectionPoolLifecycleContext{parent: ctx, poolDone: pool.poolCtx.Done()}, nil, nil
	}
	callContext, cancel := context.WithCancel(ctx)
	stopPoolCancel := context.AfterFunc(pool.poolCtx, cancel)
	return callContext, cancel, stopPoolCancel
}

type connectionPoolLifecycleContext struct {
	parent   context.Context
	poolDone <-chan struct{}
}

func (ctx connectionPoolLifecycleContext) Deadline() (time.Time, bool) {
	if ctx.parent == nil {
		return time.Time{}, false
	}
	return ctx.parent.Deadline()
}

func (ctx connectionPoolLifecycleContext) Done() <-chan struct{} {
	return ctx.poolDone
}

func (ctx connectionPoolLifecycleContext) Err() error {
	select {
	case <-ctx.poolDone:
		return context.Canceled
	default:
		if ctx.parent == nil {
			return nil
		}
		return ctx.parent.Err()
	}
}

func (ctx connectionPoolLifecycleContext) Value(key any) any {
	if ctx.parent == nil {
		return nil
	}
	return ctx.parent.Value(key)
}
