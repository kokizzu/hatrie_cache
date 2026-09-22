package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestCompactPeerSessionCallBatchReturnsOrderedResponses(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			if string(request.Command) == "slow" {
				timer := time.NewTimer(30 * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					return CompactFrame{}, ctx.Err()
				}
			}
			return CompactFrame{Payload: append([]byte("reply:"), request.Payload...)}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	responses, err := client.CallBatch(context.Background(), []CompactPeerBatchRequest{
		{Command: []byte("slow"), Payload: []byte("one")},
		{Command: []byte("fast"), Payload: []byte("two")},
		{Command: []byte("fast"), Payload: []byte("three")},
	})
	if err != nil {
		t.Fatalf("CallBatch() error = %v", err)
	}
	if len(responses) != 3 {
		t.Fatalf("CallBatch() returned %d responses, want 3", len(responses))
	}
	wantPayloads := []string{"reply:one", "reply:two", "reply:three"}
	for index, response := range responses {
		if response.Kind != CompactResponse || string(response.Payload) != wantPayloads[index] {
			t.Fatalf("response[%d] = %#v, want payload %q", index, response, wantPayloads[index])
		}
	}
	empty, err := client.CallBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("CallBatch(empty) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("CallBatch(empty) returned %d responses, want 0", len(empty))
	}
}

func TestCompactPeerSessionCallBatchCleansUpOnContextCancellation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	started := make(chan struct{})
	var startOnce sync.Once
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		EnableRequestCancellation: true,
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			if string(request.Command) == "wait" {
				startOnce.Do(func() { close(started) })
				<-ctx.Done()
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
		_, callErr := client.CallBatch(ctx, []CompactPeerBatchRequest{
			{Command: []byte("wait"), Payload: []byte("one")},
			{Command: []byte("wait"), Payload: []byte("two")},
		})
		result <- callErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("batch handler did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CallBatch(canceled) error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CallBatch(canceled) did not return")
	}

	response, err := client.Call(context.Background(), []byte("after"), []byte("ok"))
	if err != nil {
		t.Fatalf("Call(after canceled batch) error = %v", err)
	}
	if string(response.Payload) != "ok" {
		t.Fatalf("Call(after canceled batch) payload = %q, want ok", response.Payload)
	}
}
