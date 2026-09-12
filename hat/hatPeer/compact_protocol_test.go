package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

func TestCompactProtocolRoundTrip(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := CompactFrame{
		Kind:      CompactRequest,
		RequestID: 17,
		Flags:     3,
		Command:   []byte("SETSTR"),
		Payload:   []byte(`{"key":"orders:1","value":"ready"}`),
	}
	encoded, err := protocol.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) == 0 || len(encoded) >= len(want.Payload)+len(want.Command)+32 {
		t.Fatalf("unexpected compact frame size %d", len(encoded))
	}
	if encoded[1] != compactProtocolMagic0 || encoded[2] != compactProtocolMagic1 {
		t.Fatalf("unexpected magic: %x", encoded[:min(len(encoded), 3)])
	}
	got, err := protocol.Read(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != want.Kind || got.RequestID != want.RequestID || got.Flags != want.Flags || !bytes.Equal(got.Command, want.Command) || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}

	var buffer bytes.Buffer
	if err := protocol.Write(&buffer, want); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes(), encoded) {
		t.Fatal("Write and Marshal produced different frames")
	}
}

func TestCompactProtocolRejectsMalformedAndOversizedFrames(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{MaxFrameBytes: 64, MaxCommandBytes: 8, MaxPayloadBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	valid := CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("GET"), Payload: []byte("x")}
	encoded, err := protocol.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		data []byte
		want error
	}{
		{name: "truncated", data: encoded[:len(encoded)-1], want: ErrCompactProtocolTruncated},
		{name: "magic", data: replaceByte(encoded, 1, 'x'), want: ErrCompactProtocolInvalidFrame},
		{name: "version", data: replaceByte(encoded, 3, 2), want: ErrCompactProtocolVersionUnsupported},
		{name: "kind", data: replaceByte(encoded, 4, 99), want: ErrCompactProtocolInvalidFrame},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := protocol.Read(bytes.NewReader(test.data)); !errors.Is(err, test.want) {
				t.Fatalf("Read() error = %v, want %v", err, test.want)
			}
		})
	}
	if _, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 0, Command: []byte("GET")}); !errors.Is(err, ErrCompactProtocolRequestIDInvalid) {
		t.Fatalf("zero request ID error = %v", err)
	}
	if _, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("TOO-LONG!")}); !errors.Is(err, ErrCompactProtocolCommandTooLarge) {
		t.Fatalf("command limit error = %v", err)
	}
	if _, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("GET"), Payload: bytes.Repeat([]byte{'x'}, 17)}); !errors.Is(err, ErrCompactProtocolPayloadTooLarge) {
		t.Fatalf("payload limit error = %v", err)
	}
	frameLimitedProtocol, err := NewCompactProtocol(CompactProtocolOptions{MaxFrameBytes: 32, MaxCommandBytes: 8, MaxPayloadBytes: 24})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := frameLimitedProtocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 1, Command: []byte("GET"), Payload: bytes.Repeat([]byte{'x'}, 24)}); !errors.Is(err, ErrCompactProtocolFrameTooLarge) {
		t.Fatalf("frame limit error = %v", err)
	}
}

func TestCompactProtocolOptionsAndFrameValidation(t *testing.T) {
	invalid := []CompactProtocolOptions{
		{MaxFrameBytes: -1},
		{MaxCommandBytes: -1},
		{MaxPayloadBytes: -1},
		{MaxFrameBytes: 8, MaxCommandBytes: 9},
		{MaxFrameBytes: 8, MaxPayloadBytes: 9},
	}
	for _, options := range invalid {
		if _, err := NewCompactProtocol(options); !errors.Is(err, ErrCompactProtocolOptionsInvalid) {
			t.Fatalf("options %+v error = %v", options, err)
		}
	}
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range []CompactFrame{
		{Kind: CompactRequest, RequestID: 1},
		{Kind: CompactRequest, RequestID: 1, Command: []byte("GET"), Flags: 128},
		{Kind: CompactFrameKind(99), RequestID: 1, Command: []byte("GET")},
	} {
		if _, err := protocol.Marshal(frame); !errors.Is(err, ErrCompactProtocolInvalidFrame) {
			t.Fatalf("frame %+v error = %v", frame, err)
		}
	}
}

func TestCompactMultiplexerRoutesOutOfOrderResponses(t *testing.T) {
	multiplexer := NewCompactMultiplexer()
	first, firstPending, err := multiplexer.Request([]byte("GET"), []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	second, secondPending, err := multiplexer.Request([]byte("GET"), []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if first.RequestID == 0 || first.RequestID == second.RequestID {
		t.Fatalf("request IDs = %d/%d", first.RequestID, second.RequestID)
	}
	if secondPending.ID() != second.RequestID {
		t.Fatalf("pending ID = %d, want %d", secondPending.ID(), second.RequestID)
	}
	if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: second.RequestID, Payload: []byte("two-result")}); err != nil {
		t.Fatal(err)
	}
	if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: first.RequestID, Payload: []byte("one-result")}); err != nil {
		t.Fatal(err)
	}
	gotSecond, err := secondPending.Wait(context.Background())
	if err != nil || string(gotSecond.Payload) != "two-result" {
		t.Fatalf("second response = %+v/%v", gotSecond, err)
	}
	gotFirst, err := firstPending.Wait(context.Background())
	if err != nil || string(gotFirst.Payload) != "one-result" {
		t.Fatalf("first response = %+v/%v", gotFirst, err)
	}
	if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: first.RequestID}); !errors.Is(err, ErrCompactMultiplexerUnknownRequest) {
		t.Fatalf("unknown response error = %v", err)
	}
}

func TestCompactMultiplexerCancellationAndClose(t *testing.T) {
	multiplexer := NewCompactMultiplexer()
	_, pending, err := multiplexer.Request([]byte("GET"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pending.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error = %v", err)
	}
	if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: pending.ID()}); !errors.Is(err, ErrCompactMultiplexerUnknownRequest) {
		t.Fatalf("canceled response error = %v", err)
	}

	_, pending, err = multiplexer.Request([]byte("GET"), nil)
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("connection closed")
	multiplexer.Close(closeErr)
	if _, err := pending.Wait(context.Background()); !errors.Is(err, closeErr) {
		t.Fatalf("close wait error = %v", err)
	}
	if _, _, err := multiplexer.Request([]byte("GET"), nil); !errors.Is(err, ErrCompactMultiplexerClosed) {
		t.Fatalf("request after close error = %v", err)
	}
	multiplexer.Close(nil)
}

func TestCompactMultiplexerPendingBoundAndOptions(t *testing.T) {
	if _, err := NewCompactMultiplexerWithOptions(CompactMultiplexerOptions{MaxPending: -1}); !errors.Is(err, ErrCompactMultiplexerOptionsInvalid) {
		t.Fatalf("negative pending limit error = %v", err)
	}
	if _, err := NewCompactMultiplexerWithOptions(CompactMultiplexerOptions{MaxPending: maxCompactMultiplexerPending + 1}); !errors.Is(err, ErrCompactMultiplexerOptionsInvalid) {
		t.Fatalf("oversized pending limit error = %v", err)
	}
	multiplexer, err := NewCompactMultiplexerWithOptions(CompactMultiplexerOptions{MaxPending: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, pending, err := multiplexer.Request([]byte("GET"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := multiplexer.Request([]byte("GET"), nil); !errors.Is(err, ErrCompactMultiplexerPendingLimit) {
		t.Fatalf("pending bound error = %v", err)
	}
	if !multiplexer.Cancel(pending.ID()) {
		t.Fatal("pending request did not cancel")
	}
	if _, _, err := multiplexer.Request([]byte("GET"), nil); err != nil {
		t.Fatalf("request after cancellation error = %v", err)
	}
}

func TestCompactMultiplexerConcurrentRequests(t *testing.T) {
	multiplexer := NewCompactMultiplexer()
	const count = 64
	requests := make([]struct {
		frame   CompactFrame
		pending *CompactPendingResponse
	}, count)
	var group sync.WaitGroup
	for i := range requests {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			frame, pending, err := multiplexer.Request([]byte("GET"), []byte{byte(i)})
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			requests[i].frame = frame
			requests[i].pending = pending
		}(i)
	}
	group.Wait()
	seen := make(map[uint64]bool, count)
	for _, request := range requests {
		if request.pending == nil || seen[request.frame.RequestID] {
			t.Fatalf("duplicate or missing request: %+v", request)
		}
		seen[request.frame.RequestID] = true
		if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: request.frame.RequestID, Payload: request.frame.Payload}); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range requests {
		if _, err := request.pending.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactMultiplexerNilReceiver(t *testing.T) {
	var multiplexer *CompactMultiplexer
	if _, _, err := multiplexer.Request([]byte("GET"), nil); !errors.Is(err, ErrCompactMultiplexerClosed) {
		t.Fatalf("nil request error = %v", err)
	}
	if err := multiplexer.Resolve(CompactFrame{}); !errors.Is(err, ErrCompactMultiplexerClosed) {
		t.Fatalf("nil resolve error = %v", err)
	}
}

func replaceByte(input []byte, index int, value byte) []byte {
	output := append([]byte(nil), input...)
	output[index] = value
	return output
}
