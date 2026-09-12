package hatPeer_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatPeer"
)

type cancellationTestConnection struct {
	closeOnce sync.Once
	closed    chan struct{}
}

func (connection *cancellationTestConnection) Close() error {
	if connection.closed != nil {
		connection.closeOnce.Do(func() { close(connection.closed) })
	}
	return nil
}

func noopConnectionPoolHandler(context.Context, hatPeer.Connection) error { return nil }

type lifecycleContextKey struct{}

func TestConnectionPoolCloseCancelsActiveHandlerContext(t *testing.T) {
	handlerStarted := make(chan struct{})
	callDone := make(chan error, 1)
	fallback := make(chan struct{})
	connection := &cancellationTestConnection{closed: make(chan struct{})}
	var fallbackOnce sync.Once
	releaseFallback := func() {
		fallbackOnce.Do(func() { close(fallback) })
	}
	defer releaseFallback()

	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	go func() {
		callDone <- pool.DoWithLifecycleContext(context.Background(), func(ctx context.Context, _ hatPeer.Connection) error {
			close(handlerStarted)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-fallback:
				return errors.New("fallback release")
			}
		})
	}()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}

	closeContext, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	closeErr := pool.Close(closeContext)
	cancel()
	releaseFallback()
	callErr := <-callDone
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Close() error = %v, want nil after canceling active handler", closeErr)
	}
	if !errors.Is(callErr, context.Canceled) {
		t.Fatalf("DoWithLifecycleContext() error = %v, want context.Canceled", callErr)
	}
	select {
	case <-connection.closed:
	case <-time.After(time.Second):
		t.Fatal("canceled connection was not closed")
	}
}

func TestConnectionPoolLifecycleContextPropagatesCallerCancellation(t *testing.T) {
	callerContext, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	handlerStarted := make(chan struct{})
	callDone := make(chan error, 1)

	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return &cancellationTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	go func() {
		callDone <- pool.DoWithLifecycleContext(callerContext, func(ctx context.Context, _ hatPeer.Connection) error {
			close(handlerStarted)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	cancelCaller()
	if err := <-callDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("DoWithLifecycleContext() error = %v, want context.Canceled", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestConnectionPoolLifecycleContextPreservesCallerValuesAndDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	callerContext, cancelCaller := context.WithDeadline(
		context.WithValue(context.Background(), lifecycleContextKey{}, "value"),
		deadline,
	)
	defer cancelCaller()
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return &cancellationTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	err = pool.DoWithLifecycleContext(callerContext, func(ctx context.Context, _ hatPeer.Connection) error {
		if got := ctx.Value(lifecycleContextKey{}); got != "value" {
			t.Fatalf("context value = %v, want value", got)
		}
		gotDeadline, ok := ctx.Deadline()
		if !ok || !gotDeadline.Equal(deadline) {
			t.Fatalf("context deadline = %v, %v; want %v, true", gotDeadline, ok, deadline)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DoWithLifecycleContext() error = %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func BenchmarkConnectionPoolDoContext(b *testing.B) {
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return &cancellationTestConnection{}, nil
		},
	})
	if err != nil {
		b.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	if err := pool.Do(context.Background(), noopConnectionPoolHandler); err != nil {
		b.Fatalf("warmup Do() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.Do(context.Background(), noopConnectionPoolHandler); err != nil {
			b.Fatalf("Do() error = %v", err)
		}
	}
}

func BenchmarkConnectionPoolDoWithLifecycleContext(b *testing.B) {
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return &cancellationTestConnection{}, nil
		},
	})
	if err != nil {
		b.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()

	if err := pool.DoWithLifecycleContext(context.Background(), noopConnectionPoolHandler); err != nil {
		b.Fatalf("warmup DoWithLifecycleContext() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.DoWithLifecycleContext(context.Background(), noopConnectionPoolHandler); err != nil {
			b.Fatalf("DoWithLifecycleContext() error = %v", err)
		}
	}
}

func BenchmarkConnectionPoolDoWithLifecycleContextCancelable(b *testing.B) {
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return &cancellationTestConnection{}, nil
		},
	})
	if err != nil {
		b.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer func() { _ = pool.Close(context.Background()) }()
	callerContext, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()

	if err := pool.DoWithLifecycleContext(callerContext, noopConnectionPoolHandler); err != nil {
		b.Fatalf("warmup DoWithLifecycleContext() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.DoWithLifecycleContext(callerContext, noopConnectionPoolHandler); err != nil {
			b.Fatalf("DoWithLifecycleContext() error = %v", err)
		}
	}
}
