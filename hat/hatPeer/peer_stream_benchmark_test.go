package hatPeer

import (
	"context"
	"net"
	"testing"
)

func BenchmarkCompactPeerStreamCall(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerStreamEndpoint(serverConn, CompactPeerStreamOptions{
		Handler: func(_ context.Context, request CompactPeerStreamRequest) (CompactFrame, error) {
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := NewCompactPeerStreamEndpoint(clientConn, CompactPeerStreamOptions{})
	if err != nil {
		b.Fatal(err)
	}
	stream, err := client.OpenStream(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := stream.Call(context.Background(), []byte("GET"), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = client.Close()
	_ = server.Close()
}
