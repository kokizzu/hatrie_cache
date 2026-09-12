package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPeerLifecycleRegistryEmitsOrderedBoundedEvents(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{MaxHooks: 4, HistoryLimit: 2})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	var mu sync.Mutex
	var seen []PeerLifecycleEvent
	hookID, err := registry.Register(PeerLifecycleConnected, func(event PeerLifecycleEvent) {
		mu.Lock()
		seen = append(seen, event)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if hookID == 0 {
		t.Fatal("Register() returned zero hook ID")
	}
	if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected, PeerID: "node-a"}); err != nil {
		t.Fatalf("Emit(connected) error = %v", err)
	}
	if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleDisconnected, PeerID: "node-a"}); err != nil {
		t.Fatalf("Emit(disconnected) error = %v", err)
	}
	if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleSchemaReloaded, PeerID: "node-a"}); err != nil {
		t.Fatalf("Emit(schema) error = %v", err)
	}
	mu.Lock()
	if len(seen) != 1 || seen[0].PeerID != "node-a" || seen[0].At.IsZero() {
		t.Fatalf("connected hook events = %#v, want one timestamped event", seen)
	}
	mu.Unlock()
	history := registry.Snapshot()
	if len(history) != 2 || history[0].Kind != PeerLifecycleDisconnected || history[1].Kind != PeerLifecycleSchemaReloaded {
		t.Fatalf("history = %#v, want last two events", history)
	}
	if !registry.Unregister(PeerLifecycleConnected, hookID) || registry.Unregister(PeerLifecycleConnected, hookID) {
		t.Fatal("Unregister() was not idempotent")
	}
}

func TestCompactPeerSessionEmitsLifecycleEvents(t *testing.T) {
	serverConn, clientConn := net.Pipe()
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
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Lifecycle: registry,
		PeerID:    "server-a",
		Handler: func(context.Context, CompactFrame) (CompactFrame, error) {
			return CompactFrame{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	if got := <-events; got != PeerLifecycleConnected {
		t.Fatalf("first lifecycle event = %s, want connected", got)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close() error = %v", err)
	}
	seen := map[PeerLifecycleKind]bool{}
	for index := 0; index < 2; index++ {
		select {
		case kind := <-events:
			seen[kind] = true
		case <-time.After(time.Second):
			t.Fatal("session lifecycle event was not emitted")
		}
	}
	if !seen[PeerLifecycleDisconnected] || !seen[PeerLifecycleShutdown] {
		t.Fatalf("session lifecycle events = %#v, want disconnected and shutdown", seen)
	}
	_ = client.Close()
}

func TestPeerLifecycleRegistryValidatesOptionsAndInputs(t *testing.T) {
	if _, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{MaxHooks: -1}); !errors.Is(err, ErrPeerLifecycleOptionsInvalid) {
		t.Fatalf("invalid hook limit error = %v", err)
	}
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	if _, err := registry.Register(PeerLifecycleKind(99), func(PeerLifecycleEvent) {}); !errors.Is(err, ErrPeerLifecycleKindInvalid) {
		t.Fatalf("invalid kind error = %v", err)
	}
	if _, err := registry.Register(PeerLifecycleConnected, nil); !errors.Is(err, ErrPeerLifecycleHookRequired) {
		t.Fatalf("nil hook error = %v", err)
	}
	if err := registry.Emit(PeerLifecycleEvent{}); !errors.Is(err, ErrPeerLifecycleKindInvalid) {
		t.Fatalf("zero event kind error = %v", err)
	}
}

func TestPeerLifecycleRegistryIsolatesHookPanicsAndBoundsHooks(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{MaxHooks: 2, HistoryLimit: 1})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	if _, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) { panic("hook failure") }); err != nil {
		t.Fatalf("Register(panicking hook) error = %v", err)
	}
	var called bool
	secondID, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) { called = true })
	if err != nil {
		t.Fatalf("Register(second hook) error = %v", err)
	}
	if _, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) {}); !errors.Is(err, ErrPeerLifecycleHookLimit) {
		t.Fatalf("third Register() error = %v", err)
	}
	if err := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if !called {
		t.Fatal("hook after panic was not called")
	}
	if !registry.Unregister(PeerLifecycleConnected, secondID) {
		t.Fatal("Unregister(second hook) = false")
	}
	if _, err := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) {}); err != nil {
		t.Fatalf("Register(after unregister) error = %v", err)
	}
}

func TestPeerLifecycleRegistryConcurrentUse(t *testing.T) {
	registry, err := NewPeerLifecycleRegistry(PeerLifecycleOptions{MaxHooks: 64, HistoryLimit: 8})
	if err != nil {
		t.Fatalf("NewPeerLifecycleRegistry() error = %v", err)
	}
	const workers = 8
	var wait sync.WaitGroup
	wait.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wait.Done()
			for index := 0; index < 100; index++ {
				id, registerErr := registry.Register(PeerLifecycleConnected, func(PeerLifecycleEvent) {})
				if registerErr == nil {
					registry.Unregister(PeerLifecycleConnected, id)
				}
				if emitErr := registry.Emit(PeerLifecycleEvent{Kind: PeerLifecycleConnected}); emitErr != nil {
					t.Errorf("Emit() error = %v", emitErr)
					return
				}
				_ = registry.Snapshot()
			}
		}()
	}
	wait.Wait()
}
