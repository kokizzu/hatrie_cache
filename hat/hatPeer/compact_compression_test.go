package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

func TestCompactProtocolPayloadCompression(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{CompressPayloadsAbove: 32})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	wantPayload := bytes.Repeat([]byte("compact-payload-"), 128)
	frame := CompactFrame{Kind: CompactRequest, RequestID: 7, Command: []byte("BATCH"), Payload: wantPayload}
	encoded, err := protocol.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	plainProtocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatalf("NewCompactProtocol(plain) error = %v", err)
	}
	plain, err := plainProtocol.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal(plain) error = %v", err)
	}
	if len(encoded) >= len(plain) {
		t.Fatalf("compressed frame bytes = %d, plain frame bytes = %d", len(encoded), len(plain))
	}
	decoded, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if decoded.Flags&CompactFrameFlagPayloadCompressed == 0 {
		t.Fatalf("decoded flags = %d, want compressed flag", decoded.Flags)
	}
	if !bytes.Equal(decoded.Payload, wantPayload) {
		t.Fatalf("decoded payload mismatch: got %d bytes, want %d", len(decoded.Payload), len(wantPayload))
	}
}

func TestCompactProtocolCompressionIsDefaultOff(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	frame := CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("SET"), Payload: bytes.Repeat([]byte("x"), 256)}
	encoded, err := protocol.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	decoded, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if decoded.Flags&CompactFrameFlagPayloadCompressed != 0 {
		t.Fatal("default protocol unexpectedly compressed payload")
	}
	if !bytes.Equal(decoded.Payload, frame.Payload) {
		t.Fatal("default protocol changed payload")
	}
}

func TestCompactProtocolCompressionFallsBackForIncompressiblePayload(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{CompressPayloadsAbove: 1})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	payload := make([]byte, 4096)
	state := uint32(0x12345678)
	for index := range payload {
		state = state*1664525 + 1013904223
		payload[index] = byte(state >> 24)
	}
	encoded, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("SET"), Payload: payload})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	decoded, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if decoded.Flags&CompactFrameFlagPayloadCompressed != 0 {
		t.Fatal("incompressible payload was compressed")
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatal("incompressible payload changed")
	}
}

func TestCompactProtocolRejectsInvalidCompressionBounds(t *testing.T) {
	invalid := []CompactProtocolOptions{
		{CompressPayloadsAbove: -1},
		{MaxDecompressedPayloadBytes: -1},
		{MaxDecompressedPayloadBytes: maxCompactProtocolPayloadBytes + 1},
	}
	for _, options := range invalid {
		if _, err := NewCompactProtocol(options); !errors.Is(err, ErrCompactProtocolOptionsInvalid) {
			t.Fatalf("NewCompactProtocol(%+v) error = %v, want %v", options, err, ErrCompactProtocolOptionsInvalid)
		}
	}
}

func TestCompactProtocolRejectsOversizedCompressedPayload(t *testing.T) {
	encoder, err := NewCompactProtocol(CompactProtocolOptions{
		MaxFrameBytes:               1024,
		MaxPayloadBytes:             512,
		MaxDecompressedPayloadBytes: 512,
		CompressPayloadsAbove:       1,
	})
	if err != nil {
		t.Fatalf("NewCompactProtocol(encoder) error = %v", err)
	}
	protocol, err := NewCompactProtocol(CompactProtocolOptions{
		MaxFrameBytes:               1024,
		MaxPayloadBytes:             512,
		MaxDecompressedPayloadBytes: 32,
	})
	if err != nil {
		t.Fatalf("NewCompactProtocol(reader) error = %v", err)
	}
	frame := CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("SET"), Payload: bytes.Repeat([]byte("x"), 256)}
	encoded, err := encoder.Marshal(frame)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if _, err := protocol.Read(bytes.NewReader(encoded)); !errors.Is(err, ErrCompactProtocolDecompressedPayloadTooLarge) {
		t.Fatalf("Read() error = %v, want %v", err, ErrCompactProtocolDecompressedPayloadTooLarge)
	}
}

func TestCompactProtocolRejectsMalformedCompressedPayload(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{CompressPayloadsAbove: 1})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	encoded, err := protocol.Marshal(CompactFrame{
		Kind:      CompactRequest,
		RequestID: 1,
		Command:   []byte("SET"),
		Payload:   bytes.Repeat([]byte("x"), 64),
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := protocol.Read(bytes.NewReader(encoded)); !errors.Is(err, ErrCompactProtocolCompressedPayloadInvalid) {
		t.Fatalf("Read() error = %v, want %v", err, ErrCompactProtocolCompressedPayloadInvalid)
	}
}

func TestCompactPeerSessionPayloadCompression(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{
		Protocol: CompactProtocolOptions{CompressPayloadsAbove: 1},
		Handler: func(_ context.Context, request CompactFrame) (CompactFrame, error) {
			if request.Flags&CompactFrameFlagPayloadCompressed == 0 {
				t.Error("server handler did not receive compressed request flag")
			}
			return CompactFrame{Payload: append([]byte("reply:"), request.Payload...)}, nil
		},
	})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{
		Protocol: CompactProtocolOptions{CompressPayloadsAbove: 1},
	})
	if err != nil {
		server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer server.Close()
	defer client.Close()
	payload := bytes.Repeat([]byte("request-"), 64)
	response, err := client.Call(context.Background(), []byte("BATCH"), payload)
	if err != nil {
		t.Fatalf("Call() error = %v", err)
	}
	if response.Flags&CompactFrameFlagPayloadCompressed == 0 {
		t.Fatal("client response did not retain compressed flag")
	}
	if !bytes.Equal(response.Payload, append([]byte("reply:"), payload...)) {
		t.Fatalf("response payload mismatch: got %d bytes, want %d", len(response.Payload), len(payload)+len("reply:"))
	}
}
