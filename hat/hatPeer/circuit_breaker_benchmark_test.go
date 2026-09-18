package hatPeer

import (
	"context"
	"errors"
	"net"
	"testing"
)

var errBenchmarkPeerUnavailable = errors.New("benchmark peer unavailable")

func BenchmarkCompactPeerCircuitBreakerCall(b *testing.B) {
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
	breaker, err := NewCompactPeerCircuitBreaker(client, CompactPeerCircuitBreakerOptions{})
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	command := []byte("ECHO")
	payload := []byte("payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := breaker.Call(context.Background(), command, payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompactPeerSessionRemoteError(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, _ CompactFrame) (CompactFrame, error) {
			return CompactFrame{}, errBenchmarkPeerUnavailable
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
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := client.Call(context.Background(), []byte("ECHO"), nil); err == nil {
			b.Fatal("Call() unexpectedly succeeded")
		}
	}
}

func BenchmarkCompactPeerCircuitBreakerOpen(b *testing.B) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, _ CompactFrame) (CompactFrame, error) {
			return CompactFrame{}, errBenchmarkPeerUnavailable
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
	breaker, err := NewCompactPeerCircuitBreaker(client, CompactPeerCircuitBreakerOptions{Failures: 1})
	if err != nil {
		_ = client.Close()
		_ = server.Close()
		b.Fatal(err)
	}
	if _, err := breaker.Call(context.Background(), []byte("ECHO"), nil); err == nil {
		_ = client.Close()
		_ = server.Close()
		b.Fatal("warm-up Call() unexpectedly succeeded")
	}
	b.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := breaker.Call(context.Background(), []byte("ECHO"), nil); err != ErrCompactPeerCircuitOpen {
			b.Fatalf("open Call() error = %v, want %v", err, ErrCompactPeerCircuitOpen)
		}
	}
}
