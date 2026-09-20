package hatPeer

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

type tu28LifecycleConnection struct {
	closeCount atomic.Int32
	closeErr   error
}

func (connection *tu28LifecycleConnection) Close() error {
	connection.closeCount.Add(1)
	return connection.closeErr
}

func TestTU28ConnectionPoolLifecycleEventsCoverReuseAndShutdown(t *testing.T) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	connection := &tu28LifecycleConnection{}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Dial:      func(context.Context) (Connection, error) { return connection, nil },
		Lifecycle: lifecycle,
		PeerID:    "node-a",
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	for index := 0; index < 2; index++ {
		if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
			t.Fatalf("Do(%d) error = %v", index, err)
		}
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	events := lifecycle.Snapshot()
	got := make([]PeerLifecycleKind, len(events))
	for index, event := range events {
		got[index] = event.Kind
		if event.PeerID != "node-a" {
			t.Fatalf("event %d peer ID = %q, want node-a", index, event.PeerID)
		}
	}
	if want := []PeerLifecycleKind{PeerLifecycleConnected, PeerLifecycleDisconnected, PeerLifecycleShutdown}; !reflect.DeepEqual(got, want) {
		t.Fatalf("lifecycle kinds = %#v, want %#v", got, want)
	}
	if connection.closeCount.Load() != 1 {
		t.Fatalf("connection close count = %d, want 1", connection.closeCount.Load())
	}
}

func TestTU28ConnectionPoolLifecycleDisconnectIncludesCloseError(t *testing.T) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	connection := &tu28LifecycleConnection{closeErr: errors.New("peer close failed")}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Dial:      func(context.Context) (Connection, error) { return connection, nil },
		Lifecycle: lifecycle,
		PeerID:    "node-b",
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer pool.Close(context.Background())

	handlerErr := errors.New("handler failed")
	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return handlerErr }); !errors.Is(err, handlerErr) {
		t.Fatalf("Do() error = %v, want handler error", err)
	}
	events := lifecycle.Snapshot()
	if len(events) != 2 || events[0].Kind != PeerLifecycleConnected || events[1].Kind != PeerLifecycleDisconnected {
		t.Fatalf("events before shutdown = %#v, want connected/disconnected", events)
	}
	if events[1].Error != "peer close failed" {
		t.Fatalf("disconnect error = %q, want peer close failed", events[1].Error)
	}
}

func TestTU28ConnectionPoolLifecycleDefaultsOff(t *testing.T) {
	connection := &tu28LifecycleConnection{}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial:    func(context.Context) (Connection, error) { return connection, nil },
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestTU28LifecycleMetadataIsBounded(t *testing.T) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 2})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	if err := lifecycle.Emit(PeerLifecycleEvent{
		Kind:   PeerLifecycleDisconnected,
		PeerID: string(make([]byte, MaxPeerLifecyclePeerIDBytes+1)),
		Error:  string(make([]byte, MaxPeerLifecycleErrorBytes+1)),
	}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	events := lifecycle.Snapshot()
	if len(events) != 1 {
		t.Fatalf("event count = %d, want 1", len(events))
	}
	if len([]byte(events[0].PeerID)) != MaxPeerLifecyclePeerIDBytes {
		t.Fatalf("peer ID bytes = %d, want %d", len([]byte(events[0].PeerID)), MaxPeerLifecyclePeerIDBytes)
	}
	if len([]byte(events[0].Error)) != MaxPeerLifecycleErrorBytes {
		t.Fatalf("error bytes = %d, want %d", len([]byte(events[0].Error)), MaxPeerLifecycleErrorBytes)
	}
}

func TestTU28ConnectionPoolLifecycleShutdownAfterCloseTimeout(t *testing.T) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	shutdown := make(chan struct{}, 1)
	if _, err := lifecycle.Register(PeerLifecycleShutdown, func(PeerLifecycleEvent) { shutdown <- struct{}{} }); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Lifecycle: lifecycle,
		Dial:      func(context.Context) (Connection, error) { return tu28BenchmarkConnection{}, nil },
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	defer pool.Close(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- pool.Do(context.Background(), func(context.Context, Connection) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = pool.Close(closeContext)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want deadline exceeded", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	select {
	case <-shutdown:
	case <-time.After(time.Second):
		t.Fatal("shutdown lifecycle event was not emitted after drain")
	}
}

func BenchmarkTU28PoolDialEachCallWithLifecycle(b *testing.B) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 32})
	if err != nil {
		b.Fatal(err)
	}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Lifecycle:       lifecycle,
		PeerID:          "node-a",
		Dial: func(context.Context) (Connection, error) {
			return tu28BenchmarkConnection{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close(context.Background())
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		err := pool.Do(context.Background(), func(context.Context, Connection) error {
			return errors.New("force close")
		})
		if err == nil {
			b.Fatal("Do() unexpectedly succeeded")
		}
	}
}

func BenchmarkTU28PoolReuseWithLifecycle(b *testing.B) {
	lifecycle, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 32})
	if err != nil {
		b.Fatal(err)
	}
	connection := &tu28LifecycleConnection{}
	pool, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:   1,
		MaxIdle:   1,
		Lifecycle: lifecycle,
		PeerID:    "node-a",
		Dial:      func(context.Context) (Connection, error) { return connection, nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	defer pool.Close(context.Background())
	if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := pool.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}
