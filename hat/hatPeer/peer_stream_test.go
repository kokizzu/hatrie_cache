package hatPeer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

func TestCompactPeerStreamRoundTripAndLifecycle(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	var mu sync.Mutex
	var operations []CompactPeerStreamOperation
	server, err := NewCompactPeerStreamEndpoint(serverConn, CompactPeerStreamOptions{
		MaxStreams: 2,
		Handler: func(_ context.Context, request CompactPeerStreamRequest) (CompactFrame, error) {
			mu.Lock()
			operations = append(operations, request.Operation)
			mu.Unlock()
			switch request.Operation {
			case CompactPeerStreamBegin:
				return CompactFrame{Payload: []byte("begun")}, nil
			case CompactPeerStreamCall:
				return CompactFrame{Payload: append([]byte("reply:"), request.Payload...)}, nil
			case CompactPeerStreamCommit:
				return CompactFrame{Payload: []byte("committed")}, nil
			default:
				return CompactFrame{}, errors.New("unexpected stream operation")
			}
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(server) error = %v", err)
	}
	client, err := NewCompactPeerStreamEndpoint(clientConn, CompactPeerStreamOptions{MaxStreams: 2})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	stream, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("OpenStream() error = %v", err)
	}
	response, err := stream.Call(context.Background(), []byte("PUT"), []byte("value"))
	if err != nil {
		t.Fatalf("stream.Call() error = %v", err)
	}
	if string(response.Payload) != "reply:value" {
		t.Fatalf("stream.Call() payload = %q, want reply:value", response.Payload)
	}
	response, err = stream.Commit(context.Background())
	if err != nil {
		t.Fatalf("stream.Commit() error = %v", err)
	}
	if string(response.Payload) != "committed" {
		t.Fatalf("stream.Commit() payload = %q, want committed", response.Payload)
	}
	if _, err := stream.Call(context.Background(), []byte("GET"), nil); !errors.Is(err, ErrCompactPeerStreamClosed) {
		t.Fatalf("Call(after commit) error = %v, want %v", err, ErrCompactPeerStreamClosed)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []CompactPeerStreamOperation{CompactPeerStreamBegin, CompactPeerStreamCall, CompactPeerStreamCommit}
	if len(operations) != len(want) {
		t.Fatalf("operations = %v, want %v", operations, want)
	}
	for index := range want {
		if operations[index] != want[index] {
			t.Fatalf("operations = %v, want %v", operations, want)
		}
	}
}

func TestCompactPeerStreamCloseRollsBackAndBoundsStreams(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	var mu sync.Mutex
	var operations []CompactPeerStreamOperation
	server, err := NewCompactPeerStreamEndpoint(serverConn, CompactPeerStreamOptions{
		Handler: func(_ context.Context, request CompactPeerStreamRequest) (CompactFrame, error) {
			mu.Lock()
			operations = append(operations, request.Operation)
			mu.Unlock()
			return CompactFrame{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(server) error = %v", err)
	}
	client, err := NewCompactPeerStreamEndpoint(clientConn, CompactPeerStreamOptions{MaxStreams: 1})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()

	first, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatalf("first OpenStream() error = %v", err)
	}
	if _, err := client.OpenStream(context.Background()); !errors.Is(err, ErrCompactPeerStreamLimit) {
		t.Fatalf("second OpenStream() error = %v, want %v", err, ErrCompactPeerStreamLimit)
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatalf("stream.Close() error = %v", err)
	}
	if _, err := client.OpenStream(context.Background()); err != nil {
		t.Fatalf("OpenStream(after rollback) error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []CompactPeerStreamOperation{CompactPeerStreamBegin, CompactPeerStreamRollback, CompactPeerStreamBegin}
	if len(operations) != len(want) {
		t.Fatalf("operations = %v, want %v", operations, want)
	}
	for index := range want {
		if operations[index] != want[index] {
			t.Fatalf("operations = %v, want %v", operations, want)
		}
	}
}

func TestCompactPeerStreamValidatesOptionsAndState(t *testing.T) {
	if _, err := NewCompactPeerStreamEndpoint(nil, CompactPeerStreamOptions{}); !errors.Is(err, ErrCompactPeerConnectionRequired) {
		t.Fatalf("nil connection error = %v", err)
	}
	conn := &stubConn{}
	if _, err := NewCompactPeerStreamEndpoint(conn, CompactPeerStreamOptions{MaxStreams: -1}); !errors.Is(err, ErrCompactPeerStreamOptionsInvalid) {
		t.Fatalf("invalid stream limit error = %v", err)
	}
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerStreamEndpoint(serverConn, CompactPeerStreamOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(server) error = %v", err)
	}
	client, err := NewCompactPeerStreamEndpoint(clientConn, CompactPeerStreamOptions{})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(client) error = %v", err)
	}
	if _, err := client.OpenStream(context.Background()); !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("missing remote handler error = %v, want remote error", err)
	}
	_ = client.Close()
	_ = server.Close()
}

func TestCompactPeerStreamRejectsMalformedEnvelopeAndReleasesFailedBegin(t *testing.T) {
	for _, payload := range [][]byte{
		nil,
		{0, byte(CompactPeerStreamBegin), 1},
		{compactPeerStreamVersion, 99, 1},
		{compactPeerStreamVersion, byte(CompactPeerStreamBegin), 0},
	} {
		if _, err := decodeCompactPeerStreamRequest(payload); !errors.Is(err, ErrCompactPeerStreamProtocol) {
			t.Fatalf("decode(%v) error = %v, want %v", payload, err, ErrCompactPeerStreamProtocol)
		}
	}

	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerStreamEndpoint(serverConn, CompactPeerStreamOptions{
		MaxStreams: 1,
		Handler: func(_ context.Context, request CompactPeerStreamRequest) (CompactFrame, error) {
			if request.Operation == CompactPeerStreamBegin {
				return CompactFrame{}, errors.New("begin failed")
			}
			return CompactFrame{}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(server) error = %v", err)
	}
	client, err := NewCompactPeerStreamEndpoint(clientConn, CompactPeerStreamOptions{MaxStreams: 1})
	if err != nil {
		t.Fatalf("NewCompactPeerStreamEndpoint(client) error = %v", err)
	}
	defer func() {
		_ = client.Close()
		_ = server.Close()
	}()
	if _, err := client.OpenStream(context.Background()); !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("failed OpenStream() error = %v, want remote error", err)
	}
	if _, err := client.OpenStream(context.Background()); !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("OpenStream(after failed begin) error = %v, want remote error", err)
	}
}

func TestCompactPeerStreamEnvelopeUsesExactVarintCapacity(t *testing.T) {
	envelope, err := encodeCompactPeerStreamRequest(CompactPeerStreamBegin, 1, nil, nil, 5)
	if err != nil {
		t.Fatalf("encode(begin) error = %v", err)
	}
	if len(envelope) != 5 {
		t.Fatalf("begin envelope length = %d, want 5", len(envelope))
	}
}
