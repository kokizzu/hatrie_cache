package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompactPeerListenerNegotiatesAuthorizesAndServes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	var authCalls atomic.Int32
	server, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{
		Handshake: CompactPeerHandshakeOptions{Features: 0x03, Timeout: time.Second},
		Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error {
			authCalls.Add(1)
			return nil
		},
		Session: CompactPeerSessionOptions{
			Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
				return CompactFrame{Payload: append([]byte(nil), request.Payload...)}, nil
			},
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerListener() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background()) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	negotiated, err := PerformCompactPeerHandshake(context.Background(), conn, CompactPeerHandshakeOptions{Features: 0x07, Timeout: time.Second})
	if err != nil {
		t.Fatalf("PerformCompactPeerHandshake() error = %v", err)
	}
	if negotiated.Version != CompactPeerHandshakeVersion1 || negotiated.Features != 0x03 {
		t.Fatalf("negotiated handshake = %#v, want version 1 and intersected features", negotiated)
	}
	session, err := NewCompactPeerSession(conn, CompactPeerSessionOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	response, err := session.Call(context.Background(), []byte("ECHO"), []byte("payload"))
	if err != nil {
		t.Fatalf("session.Call() error = %v", err)
	}
	if string(response.Payload) != "payload" {
		t.Fatalf("response payload = %q, want payload", response.Payload)
	}
	if got := authCalls.Load(); got != 1 {
		t.Fatalf("authorization calls = %d, want 1", got)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("client session Close() error = %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server Close() error = %v", err)
	}
	select {
	case err := <-serveDone:
		if !errors.Is(err, ErrCompactPeerListenerClosed) {
			t.Fatalf("Serve() error = %v, want closed error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after Close()")
	}
	stats := server.Stats()
	if stats.Accepted != 1 || stats.Negotiated != 1 || stats.AuthFailures != 0 || stats.Active != 0 {
		t.Fatalf("listener stats = %#v, want one negotiated inactive session", stats)
	}
}

func TestCompactPeerListenerRequiresAuthorization(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()
	if _, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{}); !errors.Is(err, ErrCompactPeerAuthorizationRequired) {
		t.Fatalf("NewCompactPeerListener() error = %v, want authorization error", err)
	}
}

func TestCompactPeerListenerRejectsUnsupportedVersion(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	server, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{
		Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewCompactPeerListener() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background()) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	if _, err := PerformCompactPeerHandshake(context.Background(), conn, CompactPeerHandshakeOptions{Version: CompactPeerHandshakeVersion1 + 1}); !errors.Is(err, ErrCompactPeerHandshakeRejected) {
		t.Fatalf("unsupported handshake error = %v, want rejection", err)
	}
	_ = conn.Close()
	if err := server.Close(); err != nil {
		t.Fatalf("server Close() error = %v", err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after Close()")
	}
	stats := server.Stats()
	if stats.HandshakeFailures != 1 || stats.Negotiated != 0 {
		t.Fatalf("listener stats = %#v, want one failed handshake", stats)
	}
}

func TestCompactPeerListenerRejectsUnauthorizedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	server, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{
		Authorize: func(context.Context, net.Conn, CompactPeerHandshake) error {
			return errors.New("certificate is not trusted")
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerListener() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background()) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	if _, err := PerformCompactPeerHandshake(context.Background(), conn, CompactPeerHandshakeOptions{}); !errors.Is(err, ErrCompactPeerHandshakeRejected) {
		t.Fatalf("unauthorized handshake error = %v, want rejection", err)
	}
	_ = conn.Close()
	if err := server.Close(); err != nil {
		t.Fatalf("server Close() error = %v", err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after Close()")
	}
	stats := server.Stats()
	if stats.AuthFailures != 1 || stats.HandshakeFailures != 1 || stats.Negotiated != 0 || stats.Active != 0 {
		t.Fatalf("listener stats = %#v, want one authorization failure and no session", stats)
	}
}

func TestCompactPeerListenerRequiresTLSWhenConfigured(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	server, err := NewCompactPeerListener(listener, CompactPeerListenerOptions{
		RequireTLS: true,
		Authorize:  func(context.Context, net.Conn, CompactPeerHandshake) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewCompactPeerListener() error = %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background()) }()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("net.Dial() error = %v", err)
	}
	if _, err := PerformCompactPeerHandshake(context.Background(), conn, CompactPeerHandshakeOptions{}); err == nil {
		t.Fatal("raw handshake unexpectedly succeeded with RequireTLS")
	}
	_ = conn.Close()
	if err := server.Close(); err != nil {
		t.Fatalf("server Close() error = %v", err)
	}
	select {
	case <-serveDone:
	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after Close()")
	}
	stats := server.Stats()
	if stats.HandshakeFailures != 1 || stats.AuthFailures != 0 || stats.Negotiated != 0 {
		t.Fatalf("listener stats = %#v, want one TLS policy failure", stats)
	}
}

func TestCompactPeerListenerValidatesBoundsAndVersion(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	defer listener.Close()
	authorize := func(context.Context, net.Conn, CompactPeerHandshake) error { return nil }
	for _, options := range []CompactPeerListenerOptions{
		{MaxConnections: -1, Authorize: authorize},
		{MaxConnections: maxCompactPeerListenerMaxConnections + 1, Authorize: authorize},
		{Handshake: CompactPeerHandshakeOptions{Timeout: -1}, Authorize: authorize},
		{Handshake: CompactPeerHandshakeOptions{Version: CompactPeerHandshakeVersion1 + 1}, Authorize: authorize},
	} {
		if _, err := NewCompactPeerListener(listener, options); !errors.Is(err, ErrCompactPeerListenerOptionsInvalid) {
			t.Fatalf("NewCompactPeerListener(%#v) error = %v, want options error", options, err)
		}
	}
}
