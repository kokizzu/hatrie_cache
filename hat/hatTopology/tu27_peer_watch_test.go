package hatTopology

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"hatrie_cache/hat/hatPeer"
)

func TestConfigWatchPeerContract(t *testing.T) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		Authorizer: func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{
		Handler: NewConfigWatchPeerHandler(log),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher, err := NewConfigWatchPeerWatcher(ctx, ConfigWatchPeerOptions{
		Principal: "operator",
		Prefix:    "app:",
		Dial: func(context.Context) (*hatPeer.CompactPeerSession, error) {
			return hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "other:key", Value: []byte("ignored")}); err != nil {
		t.Fatal(err)
	}
	if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:key", Value: []byte("value")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-watcher.Events():
		if event.Key != "app:key" || string(event.Value) != "value" {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for peer config event")
	}
}

func TestConfigWatchPeerHandlerBindsPrincipal(t *testing.T) {
	readPrincipal := make(chan string, 1)
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		Authorizer: func(_ context.Context, authorization ConfigWatchAuthorization) error {
			if authorization.Action == ConfigWatchRead {
				readPrincipal <- authorization.Principal
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewConfigWatchPeerHandlerForPrincipal(log, "operator")
	if err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	watcher, err := NewConfigWatchPeerWatcher(context.Background(), ConfigWatchPeerOptions{
		Principal: "spoofed-client-value",
		Dial: func(context.Context) (*hatPeer.CompactPeerSession, error) {
			return hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:key"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-watcher.Events():
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bound-principal event")
	}
	select {
	case principal := <-readPrincipal:
		if principal != "operator" {
			t.Fatalf("authorizer principal = %q, want operator", principal)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for authorizer")
	}
	watcher.Close()
}

func TestConfigWatchPrefixFiltersAndAuthorizesPrefix(t *testing.T) {
	var mu sync.Mutex
	var readKeys []string
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		Authorizer: func(_ context.Context, authorization ConfigWatchAuthorization) error {
			if authorization.Action == ConfigWatchRead {
				mu.Lock()
				readKeys = append(readKeys, authorization.Key)
				mu.Unlock()
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []ConfigWatchEvent{
		{Source: "node-a", Key: "app:one", Value: []byte("one")},
		{Source: "node-a", Key: "other:two", Value: []byte("two")},
		{Source: "node-a", Key: "app:three", Value: []byte("three")},
	} {
		if err := log.Publish(context.Background(), "operator", event); err != nil {
			t.Fatal(err)
		}
	}
	events, cursor, err := log.Read(context.Background(), ConfigWatchRequest{Principal: "operator", Prefix: "app:", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if cursor != 3 || len(events) != 2 || events[0].Key != "app:one" || events[1].Key != "app:three" {
		t.Fatalf("Read() = %#v, cursor=%d", events, cursor)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(readKeys) != 1 || readKeys[0] != "app:" {
		t.Fatalf("read authorization keys = %#v, want [app:]", readKeys)
	}
}

func TestConfigWatchPeerReconnectsFromVersionCursor(t *testing.T) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		Authorizer: func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	servers := make(chan *hatPeer.CompactPeerSession, 4)
	dial := func(context.Context) (*hatPeer.CompactPeerSession, error) {
		serverConn, clientConn := net.Pipe()
		server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{
			Handler: NewConfigWatchPeerHandler(log),
		})
		if err != nil {
			return nil, err
		}
		servers <- server
		return hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher, err := NewConfigWatchPeerWatcher(ctx, ConfigWatchPeerOptions{
		Principal:     "operator",
		Prefix:        "app:",
		RetryDelay:    time.Millisecond,
		RetryMaxDelay: 10 * time.Millisecond,
		Dial:          dial,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstServer := <-servers
	if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:one", Value: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-watcher.Events():
		if event.Version != 1 || event.Key != "app:one" {
			t.Fatalf("first event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first event")
	}
	if err := firstServer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:two", Value: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-watcher.Events():
		if event.Version != 2 || event.Key != "app:two" {
			t.Fatalf("reconnected event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reconnected event")
	}
	watcher.Close()
	for {
		select {
		case server := <-servers:
			_ = server.Close()
		default:
			if err := watcher.Err(); err != nil {
				t.Fatalf("watcher.Err() = %v after explicit close", err)
			}
			return
		}
	}
}

func TestConfigWatchPeerStopsOnHistoryGap(t *testing.T) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		HistoryLimit: 1,
		Authorizer:   func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"app:one", "app:two"} {
		if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: key}); err != nil {
			t.Fatal(err)
		}
	}
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{
		Handler: NewConfigWatchPeerHandler(log),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	watcher, err := NewConfigWatchPeerWatcher(context.Background(), ConfigWatchPeerOptions{
		Principal:    "operator",
		AfterVersion: 0,
		RetryDelay:   time.Millisecond,
		Dial: func(context.Context) (*hatPeer.CompactPeerSession, error) {
			return hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range watcher.Events() {
		t.Fatal("history gap unexpectedly delivered an event")
	}
	var gap *ConfigWatchGapError
	if !errors.As(watcher.Err(), &gap) || gap.EarliestVersion != 2 || gap.CurrentVersion != 2 {
		t.Fatalf("watcher.Err() = %v, want history gap", watcher.Err())
	}
	watcher.Close()
}
