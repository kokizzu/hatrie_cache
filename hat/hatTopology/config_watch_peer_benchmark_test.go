package hatTopology

import (
	"context"
	"runtime"
	"net"
	"testing"

	"hatrie_cache/hat/hatPeer"
)

func BenchmarkConfigWatchLogReadWithPrefix(b *testing.B) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		HistoryLimit: 64,
		Authorizer:  func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:" + string(rune('a'+index%26))}); err != nil {
			b.Fatal(err)
		}
	}
	request := ConfigWatchRequest{Principal: "operator", Prefix: "app:", AfterVersion: 0, Limit: 64}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		events, cursor, err := log.Read(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(events)
		runtime.KeepAlive(cursor)
	}
}

func BenchmarkConfigWatchPeerReadRoundTrip(b *testing.B) {
	log, err := NewConfigWatchLog(ConfigWatchOptions{
		HistoryLimit: 64,
		Authorizer:  func(context.Context, ConfigWatchAuthorization) error { return nil },
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		if err := log.Publish(context.Background(), "operator", ConfigWatchEvent{Source: "node-a", Key: "app:" + string(rune('a'+index%26))}); err != nil {
			b.Fatal(err)
		}
	}
	serverConn, clientConn := net.Pipe()
	server, err := hatPeer.NewCompactPeerSession(serverConn, hatPeer.CompactPeerSessionOptions{
		Handler: NewConfigWatchPeerHandler(log),
	})
	if err != nil {
		b.Fatal(err)
	}
	defer server.Close()
	client, err := hatPeer.NewCompactPeerSession(clientConn, hatPeer.CompactPeerSessionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	payload, err := encodeConfigWatchPeerRequest(configWatchPeerRequest{
		Operation:    configWatchPeerRead,
		Principal:    "operator",
		Prefix:       "app:",
		AfterVersion: 0,
		Limit:        64,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		frame, err := client.Call(context.Background(), []byte(ConfigWatchPeerCommand), payload)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(frame)
	}
}
