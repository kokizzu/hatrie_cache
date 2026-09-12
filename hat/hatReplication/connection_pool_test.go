package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func ExampleConnectionPool() {
	pool, _ := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial:    func(context.Context) (int, error) { return 7, nil },
	})
	connection, _ := pool.Acquire(context.Background())
	fmt.Println(connection)
	pool.Release(connection)
	_ = pool.Close()
	// Output:
	// 7
}

func TestConnectionPoolReusesIdleConnections(t *testing.T) {
	var created int32
	var closed int32
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 2,
		MaxIdle: 2,
		Dial: func(context.Context) (int, error) {
			return int(atomic.AddInt32(&created, 1)), nil
		},
		Close: func(int) error {
			atomic.AddInt32(&closed, 1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool returned error: %v", err)
	}

	first, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("first Acquire returned error: %v", err)
	}
	if first != 1 {
		t.Fatalf("first connection = %d, want 1", first)
	}
	assertConnectionPoolStats(t, pool.Stats(), ConnectionPoolStats{MaxOpen: 2, MaxIdle: 2, Open: 1, InUse: 1})
	pool.Release(first)
	assertConnectionPoolStats(t, pool.Stats(), ConnectionPoolStats{MaxOpen: 2, MaxIdle: 2, Open: 1, Idle: 1})

	reused, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("reused Acquire returned error: %v", err)
	}
	if reused != first {
		t.Fatalf("reused connection = %d, want %d", reused, first)
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("created connections = %d, want 1", got)
	}
	pool.Release(reused)
	if err := pool.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Fatalf("closed connections = %d, want 1", got)
	}
}

func TestConnectionPoolCancellationAndCloseWakeWaiters(t *testing.T) {
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial:    func(context.Context) (int, error) { return 1, nil },
	})
	if err != nil {
		t.Fatalf("NewConnectionPool returned error: %v", err)
	}
	held, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("held Acquire returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, acquireErr := pool.Acquire(ctx)
		canceled <- acquireErr
	}()
	cancel()
	select {
	case acquireErr := <-canceled:
		if !errors.Is(acquireErr, context.Canceled) {
			t.Fatalf("canceled Acquire error = %v, want context.Canceled", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Acquire did not return")
	}

	waiter := make(chan error, 1)
	go func() {
		_, acquireErr := pool.Acquire(context.Background())
		waiter <- acquireErr
	}()
	if err := pool.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	select {
	case acquireErr := <-waiter:
		if !errors.Is(acquireErr, ErrConnectionPoolClosed) {
			t.Fatalf("closed Acquire error = %v, want ErrConnectionPoolClosed", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("closed Acquire did not return")
	}
	pool.Release(held)
}

func TestConnectionPoolDiscardReturnsCapacityAndPropagatesDialError(t *testing.T) {
	dialErr := errors.New("dial failed")
	var calls int32
	var closed int32
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (int, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return 0, dialErr
			}
			return 2, nil
		},
		Close: func(int) error {
			atomic.AddInt32(&closed, 1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool returned error: %v", err)
	}
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, dialErr) {
		t.Fatalf("dial error = %v, want %v", err, dialErr)
	}
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("second Acquire returned error: %v", err)
	}
	pool.Discard(connection)
	if got := atomic.LoadInt32(&closed); got != 1 {
		t.Fatalf("closed connections after Discard = %d, want 1", got)
	}
	if got := pool.Stats().Open; got != 0 {
		t.Fatalf("open connections after Discard = %d, want 0", got)
	}
	if err := pool.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func assertConnectionPoolStats(t *testing.T, got, want ConnectionPoolStats) {
	t.Helper()
	if got != want {
		t.Fatalf("pool stats = %+v, want %+v", got, want)
	}
}
