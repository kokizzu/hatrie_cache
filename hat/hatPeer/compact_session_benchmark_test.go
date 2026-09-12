package hatPeer

import (
	"context"
	"net"
	"testing"
)

func BenchmarkCompactPeerSessionCall(b *testing.B) {
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
		b.Fatal(err)
	}
	command := []byte("GET")
	payload := []byte("benchmark-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		response, err := client.Call(context.Background(), command, payload)
		if err != nil {
			b.Fatal(err)
		}
		if string(response.Payload) != string(payload) {
			b.Fatalf("response payload = %q, want %q", response.Payload, payload)
		}
	}
	b.StopTimer()
	_ = client.Close()
	_ = server.Close()
}
