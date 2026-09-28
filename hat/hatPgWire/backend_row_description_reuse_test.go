package hatPgWire

import (
	"bytes"
	"strings"
	"testing"
)

func TestReusableMessageConnectionWritesRowDescriptionIntoPacket(t *testing.T) {
	fields := []Field{
		{Name: "id", DataTypeOID: OIDInt8},
		{Name: "name", DataTypeOID: OIDText},
	}
	control := &backendMessageTestConn{}
	if err := writeRowDescription(control, fields); err != nil {
		t.Fatalf("control write: %v", err)
	}

	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	if err := connection.writeRowDescription(fields); err != nil {
		t.Fatalf("first description write: %v", err)
	}
	if len(connection.buffer) == 0 {
		t.Fatal("expected a retained response buffer")
	}
	firstAddress := &connection.buffer[:1][0]
	if err := connection.writeRowDescription(fields); err != nil {
		t.Fatalf("second description write: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected the row description packet buffer to be reused")
	}

	want := append(append([]byte(nil), control.Bytes()...), control.Bytes()...)
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestReusableMessageConnectionDoesNotRetainOversizedRowDescription(t *testing.T) {
	fields := []Field{{Name: strings.Repeat("x", backendMessageReuseMax), DataTypeOID: OIDText}}
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}

	if err := connection.writeRowDescription(fields); err != nil {
		t.Fatalf("oversized description write: %v", err)
	}
	if len(connection.buffer) != 0 {
		t.Fatalf("retained oversized description buffer length = %d, want 0", len(connection.buffer))
	}
}
