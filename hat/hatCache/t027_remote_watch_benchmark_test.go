package hatCache

import (
	"context"
	"net"
	"testing"

	"hatrie_cache/hat/hatPeer"
)

func BenchmarkT027RemoteWatchEvent(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerWatchServer(serverConn, hatPeer.CompactPeerWatchServerOptions{
		Provider: trie,
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := hatPeer.NewCompactPeerWatchClient(clientConn, hatPeer.CompactPeerWatchClientOptions{})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	watcher, err := client.Watch(context.Background(), hatPeer.CompactPeerWatchRequest{Prefix: "cfg:", Buffer: 64})
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = watcher.Close()
		_ = client.Close()
		_ = server.Close()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := trie.UpsertStringChecked("cfg:benchmark", "value"); err != nil {
			b.Fatal(err)
		}
		event := <-watcher.Events()
		if event.Key != "cfg:benchmark" || event.Gap {
			b.Fatalf("event = %#v", event)
		}
	}
}
