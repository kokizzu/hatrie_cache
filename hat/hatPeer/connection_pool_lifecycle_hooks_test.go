package hatPeer_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"hatrie_cache/hat/hatPeer"
)

type lifecycleHookConnection struct {
	closed atomic.Int64
}

func (connection *lifecycleHookConnection) Close() error {
	connection.closed.Add(1)
	return nil
}

func TestConnectionPoolEmitsPhysicalLifecycleEvents(t *testing.T) {
	registry, err := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan hatPeer.PeerLifecycleEvent, 8)
	for _, kind := range []hatPeer.PeerLifecycleKind{
		hatPeer.PeerLifecycleConnected,
		hatPeer.PeerLifecycleDisconnected,
		hatPeer.PeerLifecycleShutdown,
	} {
		if _, err := registry.Register(kind, func(event hatPeer.PeerLifecycleEvent) { events <- event }); err != nil {
			t.Fatalf("Register(%s) error = %v", kind, err)
		}
	}
	connection := &lifecycleHookConnection{}
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxOpen:         1,
		MaxIdle:         1,
		MaxDialAttempts: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return connection, nil
		},
		Lifecycle: registry,
		PeerID:    "peer-west",
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := pool.Do(context.Background(), func(context.Context, hatPeer.Connection) error {
		return errors.New("handler failed")
	}); err == nil {
		t.Fatal("Do() error = nil, want handler failure")
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if got := connection.closed.Load(); got != 1 {
		t.Fatalf("connection close calls = %d, want 1", got)
	}

	wantKinds := []hatPeer.PeerLifecycleKind{
		hatPeer.PeerLifecycleConnected,
		hatPeer.PeerLifecycleDisconnected,
		hatPeer.PeerLifecycleShutdown,
	}
	for _, want := range wantKinds {
		select {
		case event := <-events:
			if event.Kind != want || event.PeerID != "peer-west" {
				t.Fatalf("event = %+v, want kind %s and peer-west", event, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing lifecycle event %s", want)
		}
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected extra event: %+v", event)
	default:
	}
}

func TestConnectionPoolEmitsDialFailureLifecycleEvent(t *testing.T) {
	registry, err := hatPeer.NewPeerLifecycleRegistry(hatPeer.PeerLifecycleOptions{HistoryLimit: 4})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	events := make(chan hatPeer.PeerLifecycleEvent, 4)
	if _, err := registry.Register(hatPeer.PeerLifecycleConnectFailed, func(event hatPeer.PeerLifecycleEvent) { events <- event }); err != nil {
		t.Fatalf("Register(connect failed) error = %v", err)
	}
	pool, err := hatPeer.NewConnectionPool(hatPeer.ConnectionPoolOptions{
		MaxDialAttempts: 1,
		Dial: func(context.Context) (hatPeer.Connection, error) {
			return nil, errors.New("dial refused")
		},
		Lifecycle: registry,
		PeerID:    "peer-east",
	})
	if err != nil {
		t.Fatalf("NewConnectionPool() error = %v", err)
	}
	if err := pool.Do(context.Background(), func(context.Context, hatPeer.Connection) error { return nil }); err == nil {
		t.Fatal("Do() error = nil, want dial failure")
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case event := <-events:
		if event.Kind != hatPeer.PeerLifecycleConnectFailed || event.PeerID != "peer-east" || event.Error != "dial refused" {
			t.Fatalf("dial failure event = %+v", event)
		}
	default:
		t.Fatal("missing dial failure lifecycle event")
	}
}
