package hatPeer_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hatPeer "hatrie_cache/hat/hatPeer"
)

type tu27ScriptedWatchTransport struct {
	mu       sync.Mutex
	results  []tu27WatchResult
	requests []hatPeer.PeerConfigWatchRequest
}

type tu27WatchResult struct {
	batch hatPeer.PeerConfigWatchBatch
	err   error
}

func (transport *tu27ScriptedWatchTransport) Watch(_ context.Context, request hatPeer.PeerConfigWatchRequest) (hatPeer.PeerConfigWatchBatch, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	transport.requests = append(transport.requests, request)
	if len(transport.results) == 0 {
		return hatPeer.PeerConfigWatchBatch{}, errors.New("script exhausted")
	}
	result := transport.results[0]
	transport.results = transport.results[1:]
	return result.batch, result.err
}

func (transport *tu27ScriptedWatchTransport) Requests() []hatPeer.PeerConfigWatchRequest {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return append([]hatPeer.PeerConfigWatchRequest(nil), transport.requests...)
}

func TestPeerConfigWatcherDeliversCopiesAndAdvancesCursor(t *testing.T) {
	value := []byte("enabled")
	transport := &tu27ScriptedWatchTransport{results: []tu27WatchResult{{
		batch: hatPeer.PeerConfigWatchBatch{
			Events:     []hatPeer.PeerConfigWatchEvent{{Cursor: 4, Key: "config/db/enabled", Value: value}},
			NextCursor: 4,
		},
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got hatPeer.PeerConfigWatchEvent
	watcher, err := hatPeer.NewPeerConfigWatcher(transport, hatPeer.PeerConfigWatchOptions{
		Prefix: "config/db/",
		Handler: func(_ context.Context, event hatPeer.PeerConfigWatchEvent) error {
			got = event
			got.Value = append([]byte(nil), event.Value...)
			event.Value[0] = 'X'
			cancel()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := watcher.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context cancellation", err)
	}
	if got.Cursor != 4 || got.Key != "config/db/enabled" || string(got.Value) != "enabled" {
		t.Fatalf("event = %#v", got)
	}
	if string(value) != "enabled" {
		t.Fatalf("transport value mutated: %q", value)
	}
	if watcher.Cursor() != 4 {
		t.Fatalf("Cursor() = %d, want 4", watcher.Cursor())
	}
}

func TestPeerConfigWatcherReconnectsWithSameCursor(t *testing.T) {
	transport := &tu27ScriptedWatchTransport{results: []tu27WatchResult{
		{batch: hatPeer.PeerConfigWatchBatch{Events: []hatPeer.PeerConfigWatchEvent{{Cursor: 1, Key: "config/db/a", Value: []byte("1")}}, NextCursor: 1}},
		{err: errors.New("connection reset")},
		{batch: hatPeer.PeerConfigWatchBatch{Events: []hatPeer.PeerConfigWatchEvent{{Cursor: 2, Key: "config/db/b", Value: []byte("2")}}, NextCursor: 2}},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var delays []time.Duration
	var keys []string
	watcher, err := hatPeer.NewPeerConfigWatcher(transport, hatPeer.PeerConfigWatchOptions{
		Prefix:           "config/db/",
		ReconnectInitial: 5 * time.Millisecond,
		ReconnectMax:     20 * time.Millisecond,
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
		Handler: func(_ context.Context, event hatPeer.PeerConfigWatchEvent) error {
			keys = append(keys, event.Key)
			if len(keys) == 2 {
				cancel()
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := watcher.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context cancellation", err)
	}
	if strings.Join(keys, ",") != "config/db/a,config/db/b" {
		t.Fatalf("keys = %v", keys)
	}
	if len(delays) != 1 || delays[0] != 5*time.Millisecond {
		t.Fatalf("delays = %v", delays)
	}
	requests := transport.Requests()
	if len(requests) != 3 || requests[0].Cursor != 0 || requests[1].Cursor != 1 || requests[2].Cursor != 1 {
		t.Fatalf("requests = %#v, want cursors 0, 1, 1", requests)
	}
}

func TestPeerConfigWatcherRejectsInvalidBatches(t *testing.T) {
	tests := []struct {
		name   string
		cursor uint64
		batch  hatPeer.PeerConfigWatchBatch
	}{
		{name: "prefix", batch: hatPeer.PeerConfigWatchBatch{Events: []hatPeer.PeerConfigWatchEvent{{Cursor: 1, Key: "other/key"}}, NextCursor: 1}},
		{name: "cursor", batch: hatPeer.PeerConfigWatchBatch{Events: []hatPeer.PeerConfigWatchEvent{{Cursor: 0, Key: "config/db/a"}}, NextCursor: 0}},
		{name: "regression", cursor: 1, batch: hatPeer.PeerConfigWatchBatch{NextCursor: 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &tu27ScriptedWatchTransport{results: []tu27WatchResult{{batch: test.batch}}}
			watcher, err := hatPeer.NewPeerConfigWatcher(transport, hatPeer.PeerConfigWatchOptions{
				Prefix:  "config/db/",
				Cursor:  test.cursor,
				Handler: func(context.Context, hatPeer.PeerConfigWatchEvent) error { return nil },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := watcher.Run(context.Background()); err == nil {
				t.Fatal("Run() succeeded for invalid batch")
			}
		})
	}
}

func TestPeerConfigWatcherValidatesOptions(t *testing.T) {
	transport := &tu27ScriptedWatchTransport{}
	for _, options := range []hatPeer.PeerConfigWatchOptions{
		{Handler: func(context.Context, hatPeer.PeerConfigWatchEvent) error { return nil }},
		{Prefix: "config/", BatchSize: -1, Handler: func(context.Context, hatPeer.PeerConfigWatchEvent) error { return nil }},
		{Prefix: "config/", ReconnectInitial: -time.Second, Handler: func(context.Context, hatPeer.PeerConfigWatchEvent) error { return nil }},
	} {
		if _, err := hatPeer.NewPeerConfigWatcher(transport, options); err == nil {
			t.Fatalf("NewPeerConfigWatcher(%#v) succeeded", options)
		}
	}
}
