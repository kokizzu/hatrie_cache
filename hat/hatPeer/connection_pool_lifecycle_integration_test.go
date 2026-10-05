package hatPeer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectionPoolLifecycleEvents(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan PeerLifecycleEvent, 8)
	for _, kind := range []PeerLifecycleKind{
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleShutdown,
	} {
		if _, err := registry.Register(kind, func(event PeerLifecycleEvent) { events <- event }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}

	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Lifecycle:       registry,
		PeerID:          "pool-peer",
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}

	if err := p.Do(context.Background(), func(context.Context, Connection) error {
		return errors.New("force disconnect")
	}); err == nil {
		t.Fatal("Do() error = nil, want handler failure")
	}
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("second Do() error = %v", err)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantKinds := []PeerLifecycleKind{
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleShutdown,
	}
	for index, want := range wantKinds {
		select {
		case event := <-events:
			if event.Kind != want || event.PeerID != "pool-peer" || event.At.IsZero() {
				t.Fatalf("event %d = %#v, want kind=%s peer=pool-peer timestamp", index, event, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("event %d (%s) was not emitted", index, want)
		}
	}
	if got := registry.Snapshot(); len(got) != len(wantKinds) {
		t.Fatalf("lifecycle history length = %d, want %d", len(got), len(wantKinds))
	}
}

func TestConnectionPoolLifecycleShutdownWaitsForActiveHandler(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 4})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan PeerLifecycleKind, 4)
	for _, kind := range []PeerLifecycleKind{
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleShutdown,
	} {
		if _, err := registry.Register(kind, func(event PeerLifecycleEvent) { events <- event.Kind }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Lifecycle:       registry,
		PeerID:          "draining-peer",
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	go func() {
		finished <- p.Do(context.Background(), func(context.Context, Connection) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	if got := <-events; got != PeerLifecycleConnected {
		t.Fatalf("first event = %s, want connected", got)
	}
	closeContext, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := p.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("early Close() error = %v, want deadline exceeded", err)
	}
	select {
	case event := <-events:
		t.Fatalf("event before active handler release = %s, want no terminal event", event)
	case <-time.After(2 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := <-events; got != PeerLifecycleDisconnected {
		t.Fatalf("post-drain event = %s, want disconnected", got)
	}
	if got := <-events; got != PeerLifecycleShutdown {
		t.Fatalf("final event = %s, want shutdown", got)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("final Close() error = %v", err)
	}
	select {
	case event := <-events:
		t.Fatalf("duplicate terminal event = %s", event)
	case <-time.After(2 * time.Millisecond):
	}
}

func TestConnectionPoolLifecycleShutdownIsNotLostForCanceledClose(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{HistoryLimit: 4})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan PeerLifecycleKind, 4)
	for _, kind := range []PeerLifecycleKind{
		PeerLifecycleConnected,
		PeerLifecycleDisconnected,
		PeerLifecycleShutdown,
	} {
		if _, err := registry.Register(kind, func(event PeerLifecycleEvent) { events <- event.Kind }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Lifecycle:       registry,
		PeerID:          "canceled-close-peer",
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	if got := <-events; got != PeerLifecycleConnected {
		t.Fatalf("first event = %s, want connected", got)
	}
	closeContext, cancel := context.WithCancel(context.Background())
	cancel()
	_ = p.Close(closeContext)
	if got := <-events; got != PeerLifecycleDisconnected {
		t.Fatalf("post-close event = %s, want disconnected", got)
	}
	if got := <-events; got != PeerLifecycleShutdown {
		t.Fatalf("final event = %s, want shutdown", got)
	}
}

func BenchmarkConnectionPoolDoLifecycle(b *testing.B) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{MaxHooks: 1, HistoryLimit: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) {}); err != nil {
		b.Fatal(err)
	}
	p, err := NewConnectionPool(ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Lifecycle:       registry,
		PeerID:          "benchmark-peer",
		Dial: func(context.Context) (Connection, error) {
			return &connectionPoolTestConnection{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := p.Do(context.Background(), func(context.Context, Connection) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := p.Close(context.Background()); err != nil {
		b.Fatal(err)
	}
}
