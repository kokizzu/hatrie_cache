package hatPeer

import (
	"context"
	"net"
	"testing"
	"time"
)

func benchmarkT238CompactPeer(b *testing.B, batch bool, serviceDelay time.Duration) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			if serviceDelay > 0 {
				time.Sleep(serviceDelay)
			}
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		b.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		b.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()
	requests := make([]CompactPeerBatchRequest, 8)
	for index := range requests {
		requests[index] = CompactPeerBatchRequest{
			Command: []byte("GET"),
			Payload: []byte{byte(index)},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if batch {
			if _, err := client.CallBatch(context.Background(), requests); err != nil {
				b.Fatal(err)
			}
			continue
		}
		for _, request := range requests {
			if _, err := client.Call(context.Background(), request.Command, request.Payload); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkT238CompactPeerBatch(b *testing.B) {
	benchmarkT238CompactPeer(b, true, 0)
}

func BenchmarkT238CompactPeerSequential(b *testing.B) {
	benchmarkT238CompactPeer(b, false, 0)
}

func BenchmarkT238CompactPeerBatchWithServiceLatency(b *testing.B) {
	benchmarkT238CompactPeer(b, true, 100*time.Microsecond)
}

func BenchmarkT238CompactPeerSequentialWithServiceLatency(b *testing.B) {
	benchmarkT238CompactPeer(b, false, 100*time.Microsecond)
}
