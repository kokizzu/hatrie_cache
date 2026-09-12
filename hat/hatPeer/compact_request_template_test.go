package hatPeer

import (
	"bytes"
	"errors"
	"testing"
)

func TestCompactRequestTemplateMarshalIntoMatchesMarshal(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	command := []byte("SETSTR")
	template, err := NewCompactRequestTemplate(command)
	if err != nil {
		t.Fatalf("NewCompactRequestTemplate() error = %v", err)
	}
	command[0] = 'X'
	payload := []byte("key=value")
	want, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: 7, Command: []byte("SETSTR"), Payload: payload})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	buffer := make([]byte, len(want), len(want)+16)
	got, err := template.MarshalInto(protocol, 7, payload, buffer[:0])
	if err != nil {
		t.Fatalf("MarshalInto() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MarshalInto() = %x, want %x", got, want)
	}
	decoded, err := protocol.Read(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(decoded.Command) != "SETSTR" || string(decoded.Payload) != string(payload) {
		t.Fatalf("decoded frame = %#v, want immutable command and payload", decoded)
	}
}

func TestCompactRequestTemplateIntegratesWithMultiplexer(t *testing.T) {
	template, err := NewCompactRequestTemplate([]byte("PING"))
	if err != nil {
		t.Fatalf("NewCompactRequestTemplate() error = %v", err)
	}
	multiplexer := NewCompactMultiplexer()
	frame, pending, err := multiplexer.RequestTemplate(template, []byte("payload"))
	if err != nil {
		t.Fatalf("RequestTemplate() error = %v", err)
	}
	if frame.Kind != CompactRequest || string(frame.Command) != "PING" || pending == nil {
		t.Fatalf("RequestTemplate() = %#v, pending=%#v", frame, pending)
	}
	if err := multiplexer.Resolve(CompactFrame{Kind: CompactResponse, RequestID: frame.RequestID, Payload: []byte("ok")}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	response, err := pending.Wait(nil)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if string(response.Payload) != "ok" {
		t.Fatalf("response payload = %q, want ok", response.Payload)
	}
}

func TestCompactRequestTemplateRejectsInvalidCommands(t *testing.T) {
	if _, err := NewCompactRequestTemplate(nil); !errors.Is(err, ErrCompactRequestTemplateInvalid) {
		t.Fatalf("empty command error = %v, want %v", err, ErrCompactRequestTemplateInvalid)
	}
	tooLarge := bytes.Repeat([]byte{'x'}, maxCompactProtocolCommandBytes+1)
	if _, err := NewCompactRequestTemplate(tooLarge); !errors.Is(err, ErrCompactRequestTemplateInvalid) {
		t.Fatalf("large command error = %v, want %v", err, ErrCompactRequestTemplateInvalid)
	}
}

func TestCompactRequestTemplateHandlesVarintBoundaries(t *testing.T) {
	protocol, err := NewCompactProtocol(CompactProtocolOptions{})
	if err != nil {
		t.Fatalf("NewCompactProtocol() error = %v", err)
	}
	template, err := NewCompactRequestTemplate([]byte("SETSTR"))
	if err != nil {
		t.Fatalf("NewCompactRequestTemplate() error = %v", err)
	}
	for _, requestID := range []uint64{1, 127, 128, 16384} {
		for _, payload := range [][]byte{nil, bytes.Repeat([]byte{'x'}, 127), bytes.Repeat([]byte{'y'}, 128)} {
			want, err := protocol.Marshal(CompactFrame{Kind: CompactRequest, RequestID: requestID, Command: []byte("SETSTR"), Payload: payload})
			if err != nil {
				t.Fatalf("Marshal(%d,%d) error = %v", requestID, len(payload), err)
			}
			got, err := template.MarshalInto(protocol, requestID, payload, make([]byte, 0, len(want)))
			if err != nil {
				t.Fatalf("MarshalInto(%d,%d) error = %v", requestID, len(payload), err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("MarshalInto(%d,%d) = %x, want %x", requestID, len(payload), got, want)
			}
		}
	}
}
