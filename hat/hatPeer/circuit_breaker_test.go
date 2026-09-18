package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompactPeerCircuitBreakerOpensAndRecovers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	var failing atomic.Bool
	failing.Store(true)
	var calls atomic.Int64
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			calls.Add(1)
			if failing.Load() {
				return CompactFrame{}, errors.New("backend unavailable")
			}
			return CompactFrame{Payload: append([]byte(nil), request.Payload...)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	now := time.Unix(100, 0)
	breaker, err := NewCompactPeerCircuitBreaker(client, CompactPeerCircuitBreakerOptions{
		Failures: 2,
		Cooldown: time.Second,
		Now:      func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := breaker.Call(context.Background(), []byte("ECHO"), []byte("payload")); !errors.Is(err, ErrCompactPeerRemote) {
			t.Fatalf("failing call %d error = %v, want remote error", attempt+1, err)
		}
	}
	callCount := calls.Load()
	if _, err := breaker.Call(context.Background(), []byte("ECHO"), nil); !errors.Is(err, ErrCompactPeerCircuitOpen) {
		t.Fatalf("open breaker error = %v, want circuit-open", err)
	}
	if calls.Load() != callCount {
		t.Fatalf("open breaker forwarded request: calls = %d, want %d", calls.Load(), callCount)
	}
	status := breaker.Status()
	if status.State != CompactPeerCircuitOpen || status.ConsecutiveFailures != 2 || status.HealthScore != 0 {
		t.Fatalf("open status = %#v", status)
	}

	now = now.Add(time.Second)
	failing.Store(false)
	response, err := breaker.Call(context.Background(), []byte("ECHO"), []byte("recovered"))
	if err != nil || string(response.Payload) != "recovered" {
		t.Fatalf("half-open recovery = %#v, %v", response, err)
	}
	status = breaker.Status()
	if status.State != CompactPeerCircuitClosed || status.ConsecutiveFailures != 0 || status.HealthScore != 100 {
		t.Fatalf("recovered status = %#v", status)
	}
}

func TestCompactPeerCircuitBreakerDoesNotCountCallerCancellation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	var calls atomic.Int64
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			calls.Add(1)
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()
	breaker, err := NewCompactPeerCircuitBreaker(client, CompactPeerCircuitBreakerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := breaker.Call(ctx, []byte("ECHO"), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call error = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("canceled call reached peer: calls = %d", calls.Load())
	}
	status := breaker.Status()
	if status.State != CompactPeerCircuitClosed || status.ConsecutiveFailures != 0 || status.HealthScore != 100 {
		t.Fatalf("canceled status = %#v", status)
	}
}

func TestCompactPeerCircuitBreakerValidatesConstructionAndZeroValue(t *testing.T) {
	if _, err := NewCompactPeerCircuitBreaker(nil, CompactPeerCircuitBreakerOptions{}); !errors.Is(err, ErrCompactPeerCircuitBreakerSessionRequired) {
		t.Fatalf("expected required session error, got %v", err)
	}
	if _, err := NewCompactPeerCircuitBreaker(&CompactPeerSession{}, CompactPeerCircuitBreakerOptions{Failures: -1}); !errors.Is(err, ErrCompactPeerCircuitBreakerOptionsInvalid) {
		t.Fatalf("expected invalid failures error, got %v", err)
	}
	if _, err := NewCompactPeerCircuitBreaker(&CompactPeerSession{}, CompactPeerCircuitBreakerOptions{Cooldown: -time.Second}); !errors.Is(err, ErrCompactPeerCircuitBreakerOptionsInvalid) {
		t.Fatalf("expected invalid cooldown error, got %v", err)
	}

	var breaker CompactPeerCircuitBreaker
	if _, err := breaker.Call(context.Background(), []byte("PING"), nil); !errors.Is(err, ErrCompactPeerCircuitBreakerSessionRequired) {
		t.Fatalf("expected zero-value session error, got %v", err)
	}
}
