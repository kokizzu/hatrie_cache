package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
)

func TestMZ010SQLSubscriptionWireTransportRoundTrip(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	clientRing, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v1"))
	if err != nil {
		t.Fatal(err)
	}
	serverRing, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v1"))
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSQLSubscriptionWireTransport(client, clientRing)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewSQLSubscriptionWireTransport(server, serverRing)
	if err != nil {
		t.Fatal(err)
	}
	want := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Diff:           -2,
		Payload:        []byte(`{"id":7,"name":"Ada"}`),
	}
	type receiveResult struct {
		envelope SQLSubscriptionWireEnvelope
		err      error
	}
	resultCh := make(chan receiveResult, 1)
	go func() {
		got, receiveErr := receiver.Receive(context.Background())
		resultCh <- receiveResult{envelope: got, err: receiveErr}
	}()
	if err := sender.Send(context.Background(), want); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("Receive() error = %v", result.err)
	}
	if result.envelope.Mode != want.Mode || result.envelope.SubscriptionID != want.SubscriptionID || result.envelope.Sequence != want.Sequence || result.envelope.Diff != want.Diff || string(result.envelope.Payload) != string(want.Payload) {
		t.Fatalf("round trip = %#v, want %#v", result.envelope, want)
	}
}

func TestMZ010SQLSubscriptionWireTransportAcceptsRotatedPreviousKey(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	clientRing, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v1"))
	if err != nil {
		t.Fatal(err)
	}
	serverRing, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key-v1"))
	if err != nil {
		t.Fatal(err)
	}
	sender, err := NewSQLSubscriptionWireTransport(client, clientRing)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := NewSQLSubscriptionWireTransport(server, serverRing)
	if err != nil {
		t.Fatal(err)
	}
	if err := serverRing.Rotate([]byte("subscription-key-v2")); err != nil {
		t.Fatal(err)
	}
	assertMZ010TransportRoundTrip(t, sender, receiver)
	if err := clientRing.Rotate([]byte("subscription-key-v2")); err != nil {
		t.Fatal(err)
	}
	assertMZ010TransportRoundTrip(t, sender, receiver)
}

func TestMZ010SQLSubscriptionWireTransportRejectsOversizedFrameBeforeAllocation(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key"))
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewSQLSubscriptionWireTransport(server, ring)
	if err != nil {
		t.Fatal(err)
	}
	writeResult := make(chan error, 1)
	go func() {
		var prefix [SQLSubscriptionWireTransportFrameHeaderBytes]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(MaxSQLSubscriptionWireTransportFrameBytes)+1)
		_, writeErr := client.Write(prefix[:])
		writeResult <- writeErr
	}()
	if _, err := transport.Receive(context.Background()); !errors.Is(err, ErrSQLSubscriptionWireTransportFrameTooLarge) {
		t.Fatalf("Receive() error = %v, want ErrSQLSubscriptionWireTransportFrameTooLarge", err)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("prefix write error = %v", err)
	}
}

func TestMZ010SQLSubscriptionWireTransportRejectsCorruptFrame(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	key := []byte("subscription-key")
	ring, err := NewSQLSubscriptionWireKeyring(key)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewSQLSubscriptionWireTransport(server, ring)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := SealSQLSubscriptionWireEnvelope(key, SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeSnapshot,
		SubscriptionID: "snapshot",
		Sequence:       1,
		Payload:        []byte(`[]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	wire[len(wire)-1] ^= 1
	frame := make([]byte, SQLSubscriptionWireTransportFrameHeaderBytes+len(wire))
	binary.BigEndian.PutUint32(frame, uint32(len(wire)))
	copy(frame[SQLSubscriptionWireTransportFrameHeaderBytes:], wire)
	writeResult := make(chan error, 1)
	go func() {
		_, writeErr := client.Write(frame)
		writeResult <- writeErr
	}()
	if _, err := transport.Receive(context.Background()); !errors.Is(err, ErrSQLSubscriptionWireEnvelopeAuthentication) {
		t.Fatalf("Receive() error = %v, want authentication error", err)
	}
	if err := <-writeResult; err != nil {
		t.Fatalf("frame write error = %v", err)
	}
}

func TestMZ010SQLSubscriptionWireTransportHonorsContextDeadline(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key"))
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewSQLSubscriptionWireTransport(client, ring)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = transport.Send(ctx, SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeSnapshot,
		SubscriptionID: "snapshot",
		Payload:        []byte(`[]`),
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send() error = %v, want context deadline", err)
	}
}

func TestMZ010SQLSubscriptionWireTransportRejectsInvalidConfiguration(t *testing.T) {
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSQLSubscriptionWireTransport(nil, ring); !errors.Is(err, ErrSQLSubscriptionWireTransportInvalid) {
		t.Fatalf("nil connection error = %v", err)
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if _, err := NewSQLSubscriptionWireTransport(client, nil); !errors.Is(err, ErrSQLSubscriptionWireTransportInvalid) {
		t.Fatalf("nil codec error = %v", err)
	}
}

func assertMZ010TransportRoundTrip(t *testing.T, sender, receiver *SQLSubscriptionWireTransport) {
	t.Helper()
	want := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       1,
		Diff:           1,
		Payload:        []byte(`{"id":1}`),
	}
	resultCh := make(chan error, 1)
	go func() {
		got, err := receiver.Receive(context.Background())
		if err == nil && string(got.Payload) != string(want.Payload) {
			err = errors.New("transport payload mismatch")
		}
		resultCh <- err
	}()
	if err := sender.Send(context.Background(), want); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := <-resultCh; err != nil {
		t.Fatalf("Receive() error = %v", err)
	}
}
