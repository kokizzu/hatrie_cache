package hatPeer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestCompactPeerSessionPropagatesRequestCancellation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverStarted := make(chan struct{}, 2)
	serverCanceled := make(chan struct{}, 2)
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		EnableRequestCancellation: true,
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			if string(request.Command) == "BLOCK" {
				serverStarted <- struct{}{}
				<-ctx.Done()
				serverCanceled <- struct{}{}
				return CompactFrame{}, ctx.Err()
			}
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{EnableRequestCancellation: true})
	if err != nil {
		_ = server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, callErr := client.Call(ctx, []byte("BLOCK"), nil)
		result <- callErr
	}()

	select {
	case <-serverStarted:
	case <-time.After(time.Second):
		t.Fatal("server handler did not start")
	}
	cancel()
	select {
	case callErr := <-result:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call() error = %v, want context.Canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Call() did not return")
	}
	select {
	case <-serverCanceled:
	case <-time.After(time.Second):
		t.Fatal("server handler did not receive cancellation")
	}

	response, err := client.Call(context.Background(), []byte("ECHO"), []byte("still-alive"))
	if err != nil {
		t.Fatalf("Call() after cancellation error = %v", err)
	}
	if string(response.Payload) != "still-alive" {
		t.Fatalf("response after cancellation = %q, want still-alive", response.Payload)
	}

	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer deadlineCancel()
	result = make(chan error, 1)
	go func() {
		_, callErr := client.Call(deadlineCtx, []byte("BLOCK"), nil)
		result <- callErr
	}()
	select {
	case <-serverStarted:
	case <-time.After(time.Second):
		t.Fatal("deadline handler did not start")
	}
	select {
	case callErr := <-result:
		if !errors.Is(callErr, context.DeadlineExceeded) {
			t.Fatalf("deadline Call() error = %v, want context.DeadlineExceeded", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline Call() did not return")
	}
	select {
	case <-serverCanceled:
	case <-time.After(time.Second):
		t.Fatal("server handler did not receive deadline cancellation")
	}
}
