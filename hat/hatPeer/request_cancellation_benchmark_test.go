package hatPeer

import (
	"context"
	"net"
	"testing"
)

var compactPeerCancellationBenchmarkSink error

func BenchmarkCompactPeerSessionRequestCancellation(b *testing.B) {
	b.Run("disabled_local_only", func(b *testing.B) {
		benchmarkCompactPeerSessionRequestCancellation(b, false)
	})
	b.Run("enabled_remote_context", func(b *testing.B) {
		benchmarkCompactPeerSessionRequestCancellation(b, true)
	})
}

func benchmarkCompactPeerSessionRequestCancellation(b *testing.B, enabled bool) {
	serverConn, clientConn := net.Pipe()
	started := make(chan struct{}, 1)
	completed := make(chan struct{}, 1)
	release := make(chan struct{})
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		EnableRequestCancellation: enabled,
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			started <- struct{}{}
			if enabled {
				<-ctx.Done()
			} else {
				<-release
			}
			completed <- struct{}{}
			return CompactFrame{}, ctx.Err()
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{EnableRequestCancellation: enabled})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, callErr := client.Call(ctx, []byte("BLOCK"), nil)
			result <- callErr
		}()
		<-started
		cancel()
		if !enabled {
			release <- struct{}{}
		}
		compactPeerCancellationBenchmarkSink = <-result
		<-completed
	}
	b.StopTimer()
}
