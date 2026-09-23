package hatPeer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

var errT237StaleConnection = errors.New("t237: stale connection")

type t237HealthConnection struct {
	closed atomic.Bool
}

func (connection *t237HealthConnection) Close() error {
	connection.closed.Store(true)
	return nil
}

func TestT237ConnectionPoolReplacesUnhealthyIdleConnection(t *testing.T) {
	t.Helper()
	var dialed atomic.Int32
	var checked atomic.Int32
	stale := &t237HealthConnection{}
	healthy := &t237HealthConnection{}

	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			if dialed.Add(1) == 1 {
				return stale, nil
			}
			return healthy, nil
		},
		HealthCheck: func(context.Context, Connection) error {
			checked.Add(1)
			if stale.closed.Load() {
				return nil
			}
			return errT237StaleConnection
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pool.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()

	if err := pool.Do(context.Background(), func(_ context.Context, connection Connection) error {
		if connection != stale {
			t.Fatalf("first connection = %T, want stale connection", connection)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Do(context.Background(), func(_ context.Context, connection Connection) error {
		if connection != healthy {
			t.Fatalf("recovered connection = %T, want healthy connection", connection)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !stale.closed.Load() {
		t.Fatal("stale connection was not closed")
	}
	if got := checked.Load(); got != 1 {
		t.Fatalf("health checks = %d, want 1", got)
	}
	if got := dialed.Load(); got != 2 {
		t.Fatalf("dial count = %d, want 2", got)
	}
	stats := pool.Stats()
	if stats.HealthChecks != 1 || stats.HealthCheckFailures != 1 {
		t.Fatalf("health stats = %#v, want one check and one failure", stats)
	}
}

func TestT237ConnectionPoolHealthCheckCancellationDoesNotRedial(t *testing.T) {
	var dialed atomic.Int32
	connection := &t237HealthConnection{}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return connection, nil
		},
		HealthCheck: func(ctx context.Context, _ Connection) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pool.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pool.Do(ctx, t237BaselineHandler); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled health check error = %v, want context.Canceled", err)
	}
	if got := dialed.Load(); got != 1 {
		t.Fatalf("dial count after canceled health check = %d, want 1", got)
	}
}

func TestT237ConnectionPoolHealthCheckIsOptIn(t *testing.T) {
	var dialed atomic.Int32
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			dialed.Add(1)
			return &t237HealthConnection{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := pool.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	for range 2 {
		if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
			t.Fatal(err)
		}
	}
	stats := pool.Stats()
	if stats.HealthChecks != 0 || stats.HealthCheckFailures != 0 {
		t.Fatalf("disabled health stats = %#v, want zero counters", stats)
	}
	if got := dialed.Load(); got != 1 {
		t.Fatalf("dial count = %d, want one reused connection", got)
	}
}

func BenchmarkT237ConnectionPoolHealthCheck(b *testing.B) {
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			return &t237HealthConnection{}, nil
		},
		HealthCheck: func(context.Context, Connection) error {
			return nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := pool.Close(context.Background()); err != nil {
			b.Error(err)
		}
	}()
	if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := pool.Do(context.Background(), t237BaselineHandler); err != nil {
			b.Fatal(err)
		}
	}
}
