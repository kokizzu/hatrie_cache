package hatCache

import (
	"context"
	"net"
	"testing"
	"time"

	"hatrie_cache/hat/hatPeer"
)

func TestT027RemotePrefixWatchReattachesAndReportsGap(t *testing.T) {
	trie := newTestTrie(t)
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerWatchServer(serverConn, hatPeer.CompactPeerWatchServerOptions{
		Provider: trie,
	})
	if err != nil {
		t.Fatalf("NewCompactPeerWatchServer() error = %v", err)
	}
	client, err := hatPeer.NewCompactPeerWatchClient(clientConn, hatPeer.CompactPeerWatchClientOptions{})
	if err != nil {
		server.Close()
		t.Fatalf("NewCompactPeerWatchClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	watcher, err := client.Watch(context.Background(), hatPeer.CompactPeerWatchRequest{
		Prefix: "cfg:",
		Buffer: 4,
	})
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	t.Cleanup(func() { _ = watcher.Close() })

	if err := trie.UpsertStringChecked("cfg:first", "one"); err != nil {
		t.Fatal(err)
	}
	first := receiveT027WatchEvent(t, watcher.Events())
	if first.Key != "cfg:first" || first.Operation != string(KeyChangeSet) || first.Gap {
		t.Fatalf("first event = %#v", first)
	}

	if err := server.Close(); err != nil {
		t.Fatalf("Close(first server) error = %v", err)
	}
	serverConn, clientConn = net.Pipe()
	server, err = hatPeer.NewCompactPeerWatchServer(serverConn, hatPeer.CompactPeerWatchServerOptions{
		Provider: trie,
	})
	if err != nil {
		t.Fatalf("NewCompactPeerWatchServer(same epoch) error = %v", err)
	}
	if err := client.Reconnect(clientConn); err != nil {
		t.Fatalf("Reconnect(same epoch) error = %v", err)
	}
	if err := trie.UpsertStringChecked("cfg:same-epoch", "two"); err != nil {
		t.Fatal(err)
	}
	sameEpoch := receiveT027WatchEvent(t, watcher.Events())
	if sameEpoch.Gap || sameEpoch.Key != "cfg:same-epoch" {
		t.Fatalf("same-epoch event = %#v", sameEpoch)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close(same-epoch server) error = %v", err)
	}
	if err := trie.UpsertStringChecked("cfg:missed", "between connections"); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn = net.Pipe()
	server, err = hatPeer.NewCompactPeerWatchServer(serverConn, hatPeer.CompactPeerWatchServerOptions{
		Provider: trie,
	})
	if err != nil {
		t.Fatalf("NewCompactPeerWatchServer(reconnect) error = %v", err)
	}
	if err := client.Reconnect(clientConn); err != nil {
		t.Fatalf("Reconnect() error = %v", err)
	}
	gap := receiveT027WatchEvent(t, watcher.Events())
	if !gap.Gap || gap.Epoch <= first.Epoch {
		t.Fatalf("gap event = %#v, want a later explicit gap", gap)
	}

	if err := trie.UpsertStringChecked("cfg:after", "three"); err != nil {
		t.Fatal(err)
	}
	after := receiveT027WatchEvent(t, watcher.Events())
	if after.Key != "cfg:after" || after.Operation != string(KeyChangeSet) || after.Gap {
		t.Fatalf("after reconnect event = %#v", after)
	}
}

func receiveT027WatchEvent(t *testing.T, events <-chan hatPeer.CompactPeerWatchEvent) hatPeer.CompactPeerWatchEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for remote watch event")
		return hatPeer.CompactPeerWatchEvent{}
	}
}
