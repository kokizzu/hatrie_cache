package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestCompactPeerSessionRoundTripAndOutOfOrderResponses(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			if request.Command[0] == 's' {
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
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}

	type result struct {
		frame CompactFrame
		err   error
	}
	results := make(chan result, 2)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		frame, err := client.Call(context.Background(), []byte("slow"), []byte("one"))
		results <- result{frame: frame, err: err}
	}()
	go func() {
		defer wait.Done()
		frame, err := client.Call(context.Background(), []byte("fast"), []byte("two"))
		results <- result{frame: frame, err: err}
	}()
	wait.Wait()
	close(results)

	responses := make(map[string]string)
	for response := range results {
		if response.err != nil {
			t.Fatalf("Call() error = %v", response.err)
		}
		responses[string(response.frame.Payload)] = string(response.frame.Command)
	}
	if responses["reply:one"] != "slow" || responses["reply:two"] != "fast" {
		t.Fatalf("responses = %#v, want both correlated replies", responses)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client.Close() error = %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close() error = %v", err)
	}
}

func TestCompactPeerSessionHandlerErrorsAndCancellation(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(context.Context, CompactFrame) (CompactFrame, error) {
			return CompactFrame{}, errors.New("handler failed")
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}

	frame, err := client.Call(context.Background(), []byte("fail"), nil)
	if !errors.Is(err, ErrCompactPeerRemote) || frame.RequestID != 0 {
		t.Fatalf("handler error = (%#v, %v), want remote error", frame, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Call(ctx, []byte("canceled"), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Call() error = %v, want context.Canceled", err)
	}
	_ = client.Close()
	_ = server.Close()
}

func TestCompactPeerSessionValidatesConnectionAndOptions(t *testing.T) {
	if _, err := NewCompactPeerSession(nil, CompactPeerSessionOptions{}); !errors.Is(err, ErrCompactPeerConnectionRequired) {
		t.Fatalf("nil connection error = %v", err)
	}
	if _, err := NewCompactPeerSession(&stubConn{}, CompactPeerSessionOptions{MaxInFlight: -1}); !errors.Is(err, ErrCompactPeerOptionsInvalid) {
		t.Fatalf("invalid options error = %v", err)
	}
}

func TestCompactPeerSessionCloseWakesPendingCall(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	started := make(chan struct{})
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			close(started)
			<-ctx.Done()
			return CompactFrame{}, ctx.Err()
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, callErr := client.Call(context.Background(), []byte("wait"), nil)
		result <- callErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server handler did not start")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("client.Close() error = %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrCompactPeerClosed) {
			t.Fatalf("pending Call() error = %v, want ErrCompactPeerClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending Call() was not released by Close")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close() error = %v", err)
	}
}

func TestCompactPeerSessionBoundsInboundHandlers(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	started := make(chan struct{})
	release := make(chan struct{})
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		MaxInFlight: 1,
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			if string(request.Command) == "block" {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
					return CompactFrame{}, ctx.Err()
				}
			}
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{MaxInFlight: 2})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	first := make(chan error, 1)
	go func() {
		_, callErr := client.Call(context.Background(), []byte("block"), []byte("one"))
		first <- callErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not start")
	}
	_, secondErr := client.Call(context.Background(), []byte("second"), []byte("two"))
	if !errors.Is(secondErr, ErrCompactPeerRemote) {
		t.Fatalf("bounded second Call() error = %v, want remote error", secondErr)
	}
	close(release)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first Call() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first Call() did not finish after release")
	}
	_ = client.Close()
	_ = server.Close()
}

type stubConn struct{}

func (*stubConn) Read([]byte) (int, error)         { return 0, errors.New("stub") }
func (*stubConn) Write([]byte) (int, error)        { return 0, errors.New("stub") }
func (*stubConn) Close() error                     { return nil }
func (*stubConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (*stubConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (*stubConn) SetDeadline(time.Time) error      { return nil }
func (*stubConn) SetReadDeadline(time.Time) error  { return nil }
func (*stubConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr string

func (address stubAddr) Network() string { return "stub" }
func (address stubAddr) String() string  { return string(address) }
