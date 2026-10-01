package hatPeer

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

var compactPeerConfigWatchBenchmarkSink any

type compactPeerConfigWatchBenchmarkEnvelope struct {
	Next   uint64                         `json:"next"`
	Events []hatTopology.ConfigWatchEvent `json:"events"`
}

func benchmarkConfigWatchLog(b testing.TB) *hatTopology.ConfigWatchLog {
	b.Helper()
	log, err := hatTopology.NewConfigWatchLog(hatTopology.ConfigWatchOptions{
		HistoryLimit: 8,
		Authorizer: func(context.Context, hatTopology.ConfigWatchAuthorization) error {
			return nil
		},
	})
	if err != nil {
		b.Fatalf("NewConfigWatchLog() error = %v", err)
	}
	if err := log.Publish(context.Background(), "writer", hatTopology.ConfigWatchEvent{
		Source: "benchmark",
		Key:    "db/key",
		Value:  []byte("value"),
	}); err != nil {
		b.Fatalf("Publish() error = %v", err)
	}
	return log
}

func benchmarkConfigWatchPair(b testing.TB, log *hatTopology.ConfigWatchLog) (*CompactPeerSession, func()) {
	b.Helper()
	clientConn, serverConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: NewCompactPeerConfigWatchHandler(log),
	})
	if err != nil {
		_ = clientConn.Close()
		_ = serverConn.Close()
		b.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		b.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	return client, func() {
		_ = client.Close()
		_ = server.Close()
	}
}

func BenchmarkConfigWatchDirectReadOneEvent(b *testing.B) {
	log := benchmarkConfigWatchLog(b)
	request := hatTopology.ConfigWatchRequest{Principal: "reader", Limit: 1}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		events, _, err := log.Read(ctx, request)
		if err != nil {
			b.Fatal(err)
		}
		compactPeerConfigWatchBenchmarkSink = events
	}
}

func BenchmarkCompactPeerConfigWatchReadOneEvent(b *testing.B) {
	log := benchmarkConfigWatchLog(b)
	session, closePair := benchmarkConfigWatchPair(b, log)
	defer closePair()
	client, err := NewCompactPeerConfigWatchClient(CompactPeerConfigWatchClientOptions{
		Session:   session,
		Principal: "reader",
		Limit:     1,
	})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := client.ResetCursor(0); err != nil {
			b.Fatal(err)
		}
		events, err := client.Read(ctx)
		if err != nil {
			b.Fatal(err)
		}
		compactPeerConfigWatchBenchmarkSink = events
	}
}

func BenchmarkCompactPeerConfigWatchEncode(b *testing.B) {
	events := []hatTopology.ConfigWatchEvent{{
		Version: 1,
		Source:  "benchmark",
		Key:     "db/key",
		Value:   []byte("value"),
	}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := encodeCompactPeerConfigWatchResponse(events, 1)
		if err != nil {
			b.Fatal(err)
		}
		compactPeerConfigWatchBenchmarkSink = payload
	}
}

func BenchmarkJSONConfigWatchEncode(b *testing.B) {
	envelope := compactPeerConfigWatchBenchmarkEnvelope{
		Next: 1,
		Events: []hatTopology.ConfigWatchEvent{{
			Version: 1,
			Source:  "benchmark",
			Key:     "db/key",
			Value:   []byte("value"),
		}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(envelope)
		if err != nil {
			b.Fatal(err)
		}
		compactPeerConfigWatchBenchmarkSink = payload
	}
}

func TestCompactPeerConfigWatchWireSize(t *testing.T) {
	events := []hatTopology.ConfigWatchEvent{{
		Version: 1,
		Source:  "benchmark",
		Key:     "db/key",
		Value:   []byte("value"),
	}}
	compactPayload, err := encodeCompactPeerConfigWatchResponse(events, 1)
	if err != nil {
		t.Fatalf("encodeCompactPeerConfigWatchResponse() error = %v", err)
	}
	jsonPayload, err := json.Marshal(compactPeerConfigWatchBenchmarkEnvelope{Next: 1, Events: events})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	t.Logf("compact wire bytes = %d, JSON wire bytes = %d", len(compactPayload), len(jsonPayload))
	if len(compactPayload) >= len(jsonPayload) {
		t.Fatalf("compact payload = %d bytes, JSON payload = %d bytes; compact payload should be smaller", len(compactPayload), len(jsonPayload))
	}
}
