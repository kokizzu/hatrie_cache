package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestTT035CompactProtocolDeadlineRoundTripAndCompatibility(t *testing.T) {
	deadline := time.Now().Add(time.Minute).UnixNano()
	frame := CompactFrame{
		Kind:             CompactRequest,
		RequestID:        7,
		Flags:            CompactFrameFlagDeadline,
		DeadlineUnixNano: deadline,
		Command:          []byte("WAIT"),
		Payload:          []byte("payload"),
	}

	protocol, err := NewCompactProtocol(CompactProtocolOptions{EnableRequestDeadlines: true})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protocol.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Kind != frame.Kind || decoded.RequestID != frame.RequestID || decoded.Flags != frame.Flags || decoded.DeadlineUnixNano != deadline || !bytes.Equal(decoded.Command, frame.Command) || !bytes.Equal(decoded.Payload, frame.Payload) {
		t.Fatalf("decoded deadline frame = %+v, want %+v", decoded, frame)
	}

	legacy, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Marshal(frame); !errors.Is(err, ErrCompactProtocolDeadlineUnsupported) {
		t.Fatalf("legacy deadline marshal error = %v, want ErrCompactProtocolDeadlineUnsupported", err)
	}
	plain := CompactFrame{Kind: CompactRequest, RequestID: 8, Command: []byte("PING"), Payload: []byte("payload")}
	legacyBytes, err := legacy.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	enabledBytes, err := protocol.Marshal(plain)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacyBytes, enabledBytes) {
		t.Fatalf("deadline-enabled protocol changed legacy frame: legacy=%x enabled=%x", legacyBytes, enabledBytes)
	}
}

func TestTT035CompactProtocolRejectsInvalidDeadlineMetadata(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{EnableRequestDeadlines: true})
	if err != nil {
		t.Fatal(err)
	}
	base := CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("WAIT")}
	for name, frame := range map[string]CompactFrame{
		"zero deadline flag":    {Kind: base.Kind, RequestID: base.RequestID, Flags: CompactFrameFlagDeadline, Command: base.Command},
		"deadline without flag": {Kind: base.Kind, RequestID: base.RequestID, DeadlineUnixNano: 1, Command: base.Command},
		"deadline response":     {Kind: CompactResponse, RequestID: base.RequestID, Flags: CompactFrameFlagDeadline, DeadlineUnixNano: 1, Command: base.Command},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := protocol.Marshal(frame); !errors.Is(err, ErrCompactProtocolDeadlineInvalid) {
				t.Fatalf("Marshal() error = %v, want ErrCompactProtocolDeadlineInvalid", err)
			}
		})
	}
}

func TestTT035CompactPeerPropagatesCallDeadline(t *testing.T) {
	handlerStarted := make(chan time.Time, 1)
	handlerResult := make(chan error, 1)
	client, server, _, _ := newTT035PeerPair(t, true, true, func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			return CompactFrame{}, errors.New("handler context has no deadline")
		}
		handlerStarted <- deadline
		<-ctx.Done()
		handlerErr := ctx.Err()
		handlerResult <- handlerErr
		return CompactFrame{}, handlerErr
	})

	callContext, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	wantDeadline, _ := callContext.Deadline()
	if _, err := client.Call(callContext, []byte("WAIT"), nil); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrCompactPeerRemote) {
		t.Fatalf("Call() error = %v, want local or remote deadline error", err)
	}
	select {
	case gotDeadline := <-handlerStarted:
		if gotDeadline.After(wantDeadline) || gotDeadline.Before(wantDeadline.Add(-time.Millisecond)) {
			t.Fatalf("remote deadline = %v, want approximately %v", gotDeadline, wantDeadline)
		}
	case <-time.After(time.Second):
		t.Fatal("remote handler did not receive propagated deadline")
	}
	select {
	case gotErr := <-handlerResult:
		if !errors.Is(gotErr, context.DeadlineExceeded) {
			t.Fatalf("remote handler error = %v, want context.DeadlineExceeded", gotErr)
		}
	case <-time.After(time.Second):
		t.Fatal("remote handler did not observe deadline")
	}
	_ = server
}

func TestTT035CompactPeerTemplatePropagatesDeadline(t *testing.T) {
	requestDeadline := make(chan int64, 1)
	client, _, _, _ := newTT035PeerPair(t, true, true, func(_ context.Context, request CompactFrame) (CompactFrame, error) {
		requestDeadline <- request.DeadlineUnixNano
		return CompactFrame{Payload: []byte("ok")}, nil
	})
	template, err := NewCompactRequestTemplate([]byte("WAIT"))
	if err != nil {
		t.Fatal(err)
	}
	callContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if response, err := client.CallTemplate(callContext, template, []byte("payload")); err != nil || string(response.Payload) != "ok" {
		t.Fatalf("CallTemplate() = (%+v, %v), want ok", response, err)
	}
	select {
	case got := <-requestDeadline:
		if got == 0 {
			t.Fatal("template request did not carry deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("template handler did not run")
	}
}

func TestTT035CompactPeerKeepsDeadlineOptIn(t *testing.T) {
	deadlinePresent := make(chan bool, 1)
	client, _, _, _ := newTT035PeerPair(t, false, false, func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
		_, hasDeadline := ctx.Deadline()
		deadlinePresent <- hasDeadline || request.DeadlineUnixNano != 0
		return CompactFrame{Payload: []byte("ok")}, nil
	})
	callContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Call(callContext, []byte("WAIT"), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-deadlinePresent:
		if got {
			t.Fatal("deadline propagated while feature was disabled")
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not run")
	}
}

func newTT035PeerPair(t *testing.T, clientDeadlines, serverDeadlines bool, handler CompactPeerHandler) (*CompactPeerSession, *CompactPeerSession, chan time.Time, chan error) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	handlerStarted := make(chan time.Time, 1)
	handlerResult := make(chan error, 1)
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Handler:                handler,
		EnableRequestDeadlines: serverDeadlines,
	})
	if err != nil {
		clientConn.Close()
		serverConn.Close()
		t.Fatal(err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{
		EnableRequestDeadlines: clientDeadlines,
	})
	if err != nil {
		_ = server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server, handlerStarted, handlerResult
}
