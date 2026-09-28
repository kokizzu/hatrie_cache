package hatPgWire

import (
	"bytes"
	"testing"
)

func TestReusableMessageConnectionWritesParameterDescriptionIntoPacket(t *testing.T) {
	parameterTypes := []uint32{OIDInt8, OIDText, OIDFloat8}
	control := &backendMessageTestConn{}
	if err := writeParameterDescription(control, parameterTypes); err != nil {
		t.Fatalf("control description: %v", err)
	}

	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	if err := connection.writeParameterDescription(parameterTypes); err != nil {
		t.Fatalf("first description: %v", err)
	}
	firstAddress := &connection.buffer[:1][0]
	if err := connection.writeParameterDescription(parameterTypes); err != nil {
		t.Fatalf("second description: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected parameter-description buffer to be reused")
	}

	want := append(append([]byte(nil), control.Bytes()...), control.Bytes()...)
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestReusableMessageConnectionDoesNotRetainOversizedParameterDescription(t *testing.T) {
	parameterTypes := make([]uint32, backendMessageReuseMax)
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}

	if err := connection.writeParameterDescription(parameterTypes); err != nil {
		t.Fatalf("oversized description: %v", err)
	}
	if len(connection.buffer) != 0 {
		t.Fatalf("retained oversized description buffer length = %d, want 0", len(connection.buffer))
	}
}
