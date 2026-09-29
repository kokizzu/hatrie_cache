package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
)

func TestT238CompactBatchPreservesOrderedResponses(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	batchHandler, err := NewCompactBatchHandler(func(_ context.Context, frame CompactFrame) (CompactFrame, error) {
		if bytes.Equal(frame.Command, []byte("FAIL")) {
			return CompactFrame{}, errors.New("item failed")
		}
		return CompactFrame{
			Kind:    CompactResponse,
			Command: append([]byte(nil), frame.Command...),
			Payload: append([]byte("ok:"), frame.Payload...),
		}, nil
	})
	if err != nil {
		t.Fatalf("NewCompactBatchHandler() error = %v", err)
	}
	server, err := NewCompactPeerSession(serverConn, CompactPeerSessionOptions{Handler: batchHandler})
	if err != nil {
		t.Fatalf("NewCompactPeerSession(server) error = %v", err)
	}
	client, err := NewCompactPeerSession(clientConn, CompactPeerSessionOptions{})
	if err != nil {
		_ = server.Close()
		t.Fatalf("NewCompactPeerSession(client) error = %v", err)
	}
	defer server.Close()
	defer client.Close()

	responses, err := client.CallBatch(context.Background(), []CompactBatchRequest{
		{Command: []byte("FIRST"), Payload: []byte("one")},
		{Command: []byte("FAIL"), Payload: []byte("two")},
		{Command: []byte("THIRD"), Payload: []byte("three")},
	})
	if err != nil {
		t.Fatalf("CallBatch() error = %v", err)
	}
	if len(responses) != 3 {
		t.Fatalf("responses = %d, want 3", len(responses))
	}
	if !bytes.Equal(responses[0].Command, []byte("FIRST")) || !bytes.Equal(responses[0].Payload, []byte("ok:one")) || responses[0].Kind != CompactResponse {
		t.Fatalf("first response = %#v", responses[0])
	}
	if responses[1].Kind != CompactError || !bytes.Contains(responses[1].Payload, []byte("item failed")) {
		t.Fatalf("second response = %#v, want item error", responses[1])
	}
	if !bytes.Equal(responses[2].Command, []byte("THIRD")) || !bytes.Equal(responses[2].Payload, []byte("ok:three")) || responses[2].Kind != CompactResponse {
		t.Fatalf("third response = %#v", responses[2])
	}
}

func TestT238CompactBatchFrameEncoderRoundTripsAndReusesDestination(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{MaxFrameBytes: 512, MaxCommandBytes: 16, MaxPayloadBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	requests := []CompactBatchRequest{
		{Command: []byte("GET"), Payload: []byte("orders:1")},
		{Command: []byte("SET"), Payload: []byte("ready")},
	}
	expectedPayload, err := protocol.MarshalBatch(requests)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protocol.marshalCompactBatchFrameInto(7, requests, nil)
	if err != nil {
		t.Fatalf("marshalCompactBatchFrameInto() error = %v", err)
	}
	frame, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if frame.Kind != CompactRequest || frame.RequestID != 7 || !bytes.Equal(frame.Command, []byte(CompactBatchCommand)) {
		t.Fatalf("frame = %#v", frame)
	}
	if !bytes.Equal(frame.Payload, expectedPayload) {
		t.Fatalf("payload = %x, want %x", frame.Payload, expectedPayload)
	}
	decoded, err := protocol.UnmarshalBatch(frame.Payload)
	if err != nil {
		t.Fatalf("UnmarshalBatch() error = %v", err)
	}
	if len(decoded) != len(requests) || !bytes.Equal(decoded[0].Payload, requests[0].Payload) || !bytes.Equal(decoded[1].Command, requests[1].Command) {
		t.Fatalf("decoded = %#v", decoded)
	}
	withPrefix := make([]byte, 2, 2+len(encoded))
	withPrefix[0], withPrefix[1] = 'x', 'y'
	reused, err := protocol.marshalCompactBatchFrameInto(7, requests, withPrefix)
	if err != nil {
		t.Fatalf("marshalCompactBatchFrameInto(reused) error = %v", err)
	}
	if !bytes.Equal(reused[2:], encoded) {
		t.Fatalf("reused frame differs from fresh frame")
	}
}

func TestT238CompactBatchCodecValidatesBoundsAndMalformedWire(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{MaxFrameBytes: 512, MaxCommandBytes: 16, MaxPayloadBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.MarshalBatch(nil); !errors.Is(err, ErrCompactBatchInvalid) {
		t.Fatalf("empty batch error = %v", err)
	}
	if _, err := protocol.MarshalBatch([]CompactBatchRequest{{Command: []byte("command-that-is-too-long")}}); !errors.Is(err, ErrCompactBatchCommandTooLarge) {
		t.Fatalf("long command error = %v", err)
	}
	if _, err := protocol.UnmarshalBatch([]byte{1, 1, 4, 'G'}); !errors.Is(err, ErrCompactBatchInvalid) {
		t.Fatalf("malformed request batch error = %v", err)
	}
	if _, err := protocol.UnmarshalBatchResponses([]byte{1, 1, byte(CompactResponse), 1, 3, 'G'}); !errors.Is(err, ErrCompactBatchResponseInvalid) {
		t.Fatalf("malformed response batch error = %v", err)
	}
	tooMany := make([]CompactBatchRequest, DefaultCompactBatchMaxItems+1)
	for index := range tooMany {
		tooMany[index] = CompactBatchRequest{Command: []byte("GET")}
	}
	if _, err := protocol.MarshalBatch(tooMany); !errors.Is(err, ErrCompactBatchTooManyItems) {
		t.Fatalf("too many items error = %v", err)
	}
}

func TestT238CompactBatchHandlerRejectsNilHandler(t *testing.T) {
	if _, err := NewCompactBatchHandler(nil); !errors.Is(err, ErrCompactBatchHandlerRequired) {
		t.Fatalf("nil handler error = %v", err)
	}
}
