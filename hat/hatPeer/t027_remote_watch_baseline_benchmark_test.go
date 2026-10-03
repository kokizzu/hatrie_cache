package hatPeer

import (
	"context"
	"net"
	"testing"
)

func BenchmarkT027CompactPeerCall(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	ctx := context.Background()
	payload := []byte("cfg:benchmark")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		response, err := client.Call(ctx, []byte("echo"), payload)
		if err != nil || string(response.Payload) != string(payload) {
			b.Fatalf("Call() response=%q err=%v", response.Payload, err)
		}
	}
}
