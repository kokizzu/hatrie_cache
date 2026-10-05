package hatPeer

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCompactPeerSessionCallCancellationInterruptsBlockedWrite(t *testing.T) {
	connection := newTU47BlockedWriteConn()
	session, err := NewCompactPeerSession(connection, CompactPeerSessionOptions{EnableWriteCancellation: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession() error = %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, callErr := session.Call(ctx, []byte("BLOCK"), []byte("payload"))
		callDone <- callErr
	}()

	select {
	case <-connection.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("peer write did not block")
	}
	cancel()

	select {
	case callErr := <-callDone:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call() error = %v, want context.Canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Call() remained blocked in Write")
	}
}

func TestCompactPeerSessionCallTemplateCancellationInterruptsBlockedWrite(t *testing.T) {
	connection := newTU47BlockedWriteConn()
	session, err := NewCompactPeerSession(connection, CompactPeerSessionOptions{EnableWriteCancellation: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession() error = %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	template, err := NewCompactRequestTemplate([]byte("BLOCK"))
	if err != nil {
		t.Fatalf("NewCompactRequestTemplate() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, callErr := session.CallTemplate(ctx, template, []byte("payload"))
		callDone <- callErr
	}()

	select {
	case <-connection.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("peer template write did not block")
	}
	cancel()

	select {
	case callErr := <-callDone:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("CallTemplate() error = %v, want context.Canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled CallTemplate() remained blocked in Write")
	}
}

func TestCompactPeerSessionWriteCancellationBeatsLaterDeadline(t *testing.T) {
	connection := newTU47BlockedWriteConn()
	session, err := NewCompactPeerSession(connection, CompactPeerSessionOptions{EnableWriteCancellation: true})
	if err != nil {
		t.Fatalf("NewCompactPeerSession() error = %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	callDone := make(chan error, 1)
	go func() {
		_, callErr := session.Call(ctx, []byte("BLOCK"), []byte("payload"))
		callDone <- callErr
	}()
	select {
	case <-connection.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("peer write did not block")
	}
	cancel()

	select {
	case callErr := <-callDone:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call() error = %v, want context.Canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("early cancellation did not interrupt write before deadline")
	}
}

func TestTU47DisabledCancellationProbe(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverStarted := make(chan struct{}, 1)
	serverCompleted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			serverStarted <- struct{}{}
			<-release
			serverCompleted <- struct{}{}
			return CompactFrame{}, ctx.Err()
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
	t.Cleanup(func() {
		releaseHandler()
		_ = client.Close()
		_ = server.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	callDone := make(chan error, 1)
	go func() {
		_, callErr := client.Call(ctx, []byte("BLOCK"), nil)
		callDone <- callErr
	}()
	select {
	case <-serverStarted:
	case <-time.After(time.Second):
		t.Fatal("server handler did not start")
	}
	cancel()
	releaseHandler()
	select {
	case callErr := <-callDone:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call() error = %v, want context.Canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("disabled cancellation call did not return")
	}
}

func TestTU47DisabledCancellationRepeated(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	serverStarted := make(chan struct{}, 1)
	serverCompleted := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseHandler := func() { releaseOnce.Do(func() { close(release) }) }
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
			serverStarted <- struct{}{}
			<-release
			serverCompleted <- struct{}{}
			return CompactFrame{}, ctx.Err()
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
	t.Cleanup(func() {
		releaseHandler()
		_ = client.Close()
		_ = server.Close()
	})

	for iteration := 0; iteration < 10; iteration++ {
		release = make(chan struct{})
		releaseOnce = sync.Once{}
		ctx, cancel := context.WithCancel(context.Background())
		callDone := make(chan error, 1)
		go func() {
			_, callErr := client.Call(ctx, []byte("BLOCK"), nil)
			callDone <- callErr
		}()
		select {
		case <-serverStarted:
		case <-time.After(time.Second):
			cancel()
			t.Fatalf("iteration %d: server handler did not start", iteration)
		}
		cancel()
		releaseHandler()
		select {
		case callErr := <-callDone:
			if callErr != nil && !errors.Is(callErr, context.Canceled) {
				t.Fatalf("iteration %d: Call() error = %v, want context.Canceled", iteration, callErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: disabled cancellation call did not return", iteration)
		}
		select {
		case <-serverCompleted:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: server handler did not complete", iteration)
		}
		if sessionErr := client.Err(); sessionErr != nil {
			t.Fatalf("iteration %d: client session error = %v", iteration, sessionErr)
		}
		if sessionErr := server.Err(); sessionErr != nil {
			t.Fatalf("iteration %d: server session error = %v", iteration, sessionErr)
		}
	}
}

func BenchmarkTU47WriteCancellation(b *testing.B) {
	b.Run("disabled_context", func(b *testing.B) {
		benchmarkTU47WriteCancellation(b, false)
	})
	b.Run("enabled_context", func(b *testing.B) {
		benchmarkTU47WriteCancellation(b, true)
	})
}

func benchmarkTU47WriteCancellation(b *testing.B, enabled bool) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			return CompactFrame{Payload: request.Payload}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{EnableWriteCancellation: enabled})
	if err != nil {
		_ = server.Close()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	command := []byte("GET")
	payload := []byte("benchmark-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		response, callErr := client.Call(ctx, command, payload)
		cancel()
		if callErr != nil {
			b.Fatal(callErr)
		}
		if string(response.Payload) != string(payload) {
			b.Fatalf("response payload = %q, want %q", response.Payload, payload)
		}
	}
	b.StopTimer()
}

type tu47BlockedWriteConn struct {
	closed       chan struct{}
	writeStarted chan struct{}
	closeOnce    sync.Once
	mu           sync.Mutex
	deadline     time.Time
	deadlineWake chan struct{}
}

func newTU47BlockedWriteConn() *tu47BlockedWriteConn {
	return &tu47BlockedWriteConn{
		closed:       make(chan struct{}),
		writeStarted: make(chan struct{}),
		deadlineWake: make(chan struct{}),
	}
}

func (connection *tu47BlockedWriteConn) Read([]byte) (int, error) {
	<-connection.closed
	return 0, net.ErrClosed
}

func (connection *tu47BlockedWriteConn) Write([]byte) (int, error) {
	select {
	case <-connection.writeStarted:
	default:
		close(connection.writeStarted)
	}
	for {
		connection.mu.Lock()
		deadline := connection.deadline
		deadlineWake := connection.deadlineWake
		connection.mu.Unlock()
		if deadline.IsZero() {
			select {
			case <-connection.closed:
				return 0, net.ErrClosed
			case <-deadlineWake:
			}
			continue
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-connection.closed:
			if !timer.Stop() {
				<-timer.C
			}
			return 0, net.ErrClosed
		case <-deadlineWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			return 0, os.ErrDeadlineExceeded
		}
	}
}

func (connection *tu47BlockedWriteConn) Close() error {
	connection.closeOnce.Do(func() { close(connection.closed) })
	return nil
}

func (connection *tu47BlockedWriteConn) LocalAddr() net.Addr  { return tu47Addr("local") }
func (connection *tu47BlockedWriteConn) RemoteAddr() net.Addr { return tu47Addr("remote") }

func (connection *tu47BlockedWriteConn) SetDeadline(deadline time.Time) error {
	return connection.SetWriteDeadline(deadline)
}

func (connection *tu47BlockedWriteConn) SetReadDeadline(time.Time) error { return nil }

func (connection *tu47BlockedWriteConn) SetWriteDeadline(deadline time.Time) error {
	connection.mu.Lock()
	connection.deadline = deadline
	close(connection.deadlineWake)
	connection.deadlineWake = make(chan struct{})
	connection.mu.Unlock()
	return nil
}

type tu47Addr string

func (address tu47Addr) Network() string { return "tu47" }
func (address tu47Addr) String() string  { return string(address) }
