package hatPeer

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type t028LifecycleConnection struct {
	once   sync.Once
	closed chan struct{}
}

func newT028LifecycleConnection() *t028LifecycleConnection {
	return &t028LifecycleConnection{closed: make(chan struct{})}
}

func (connection *t028LifecycleConnection) Close() error {
	connection.once.Do(func() { close(connection.closed) })
	return nil
}

func TestConnectionPoolEmitsLifecycleEventsAndSchemaReload(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan PeerLifecycleEvent, 8)
	for _, kind := range []PeerLifecycleKind{
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleShutdown,
		PeerLifecycleSchemaReloaded,
	} {
		if _, err := registry.Register(kind, func(event PeerLifecycleEvent) { events <- event }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}
	connection := newT028LifecycleConnection()
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Lifecycle: registry,
		PeerID:    "pool-a",
		Dial: func(context.Context) (Connection, error) {
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("pool.Do() error = %v", err)
	}
	assertT028LifecycleEvent(t, <-events, PeerLifecycleConnected)
	if err := pool.NotifySchemaReloaded(); err != nil {
		t.Fatalf("NotifySchemaReloaded() error = %v", err)
	}
	assertT028LifecycleEvent(t, <-events, PeerLifecycleSchemaReloaded)

	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("pool.Close() error = %v", err)
	}
	assertT028LifecycleEvent(t, <-events, PeerLifecycleDisconnected)
	assertT028LifecycleEvent(t, <-events, PeerLifecycleShutdown)
	select {
	case <-connection.closed:
	default:
		t.Fatal("pool close did not close the physical connection")
	}

	history := registry.Snapshot()
	if len(history) != 4 {
		t.Fatalf("lifecycle history length = %d, want 4", len(history))
	}
	for _, event := range history {
		if event.PeerID != "pool-a" {
			t.Fatalf("lifecycle event peer ID = %q, want pool-a", event.PeerID)
		}
		if event.At.IsZero() {
			t.Fatal("lifecycle event has zero timestamp")
		}
	}
}

func TestConnectionPoolLifecycleIsOptIn(t *testing.T) {
	connection := newT028LifecycleConnection()
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial: func(context.Context) (Connection, error) {
			return connection, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("pool.Do() error = %v", err)
	}
	if err := pool.NotifySchemaReloaded(); err != nil {
		t.Fatalf("NotifySchemaReloaded() without registry error = %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("pool.Close() error = %v", err)
	}
}

func TestConnectionPoolEmitsDisconnectedAfterHandlerError(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 4})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan PeerLifecycleKind, 4)
	for _, kind := range []PeerLifecycleKind{PeerLifecycleConnected, PeerLifecycleDisconnected, PeerLifecycleShutdown} {
		if _, err := registry.Register(kind, func(event PeerLifecycleEvent) { events <- event.Kind }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Lifecycle: registry,
		Dial: func(context.Context) (Connection, error) {
			return newT028LifecycleConnection(), nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	operationErr := errors.New("operation failed")
	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return operationErr }); !errors.Is(err, operationErr) {
		t.Fatalf("pool.Do() error = %v, want %v", err, operationErr)
	}
	if got := <-events; got != PeerLifecycleConnected {
		t.Fatalf("first lifecycle event = %s, want connected", got)
	}
	if got := <-events; got != PeerLifecycleDisconnected {
		t.Fatalf("second lifecycle event = %s, want disconnected", got)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("pool.Close() error = %v", err)
	}
	if got := <-events; got != PeerLifecycleShutdown {
		t.Fatalf("third lifecycle event = %s, want shutdown", got)
	}
}

func TestConnectionPoolLifecyclePropagatesNilAndClosedErrors(t *testing.T) {
	var nilPool *ConnectionPool
	if err := nilPool.NotifySchemaReloaded(); !errors.Is(err, ErrConnectionPoolNil) {
		t.Fatalf("nil pool NotifySchemaReloaded() error = %v, want %v", err, ErrConnectionPoolNil)
	}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		Dial:    func(context.Context) (Connection, error) { return newT028LifecycleConnection(), nil },
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("pool.Close() error = %v", err)
	}
	if err := pool.NotifySchemaReloaded(); !errors.Is(err, ErrConnectionPoolClosed) {
		t.Fatalf("closed pool NotifySchemaReloaded() error = %v, want %v", err, ErrConnectionPoolClosed)
	}
}

func assertT028LifecycleEvent(t *testing.T, event PeerLifecycleEvent, want PeerLifecycleKind) {
	t.Helper()
	if event.Kind != want {
		t.Fatalf("lifecycle event = %s, want %s", event.Kind, want)
	}
}

func BenchmarkT028ConnectionPoolLifecycleNotify(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 64})
	if err != nil {
		b.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	if _, err := registry.Register(PeerLifecycleSchemaReloaded, func(PeerLifecycleEvent) {}); err != nil {
		b.Fatalf("Register() error = %v", err)
	}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		Lifecycle: registry,
		PeerID:    "benchmark",
		Dial: func(context.Context) (Connection, error) {
			return newT028LifecycleConnection(), nil
		},
	})
	if err != nil {
		b.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer pool.Close(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := pool.NotifySchemaReloaded(); err != nil {
			b.Fatalf("NotifySchemaReloaded() error = %v", err)
		}
	}
}
