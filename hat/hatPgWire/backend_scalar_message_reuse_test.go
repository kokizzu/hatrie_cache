package hatPgWire

import (
	"bytes"
	"strings"
	"testing"
)

func TestReusableMessageConnectionWritesScalarMessagesIntoPacket(t *testing.T) {
	control := &backendMessageTestConn{}
	if err := writeParameterStatus(control, "server_version", "16.0"); err != nil {
		t.Fatalf("control parameter status: %v", err)
	}
	if err := writeMessage(control, 'C', appendCString(nil, "SELECT 1")); err != nil {
		t.Fatalf("control command tag: %v", err)
	}

	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	if err := connection.writeParameterStatus("server_version", "16.0"); err != nil {
		t.Fatalf("first parameter status: %v", err)
	}
	firstAddress := &connection.buffer[:1][0]
	if err := connection.writeCStringMessage('C', "SELECT 1"); err != nil {
		t.Fatalf("first command tag: %v", err)
	}
	if err := connection.writeParameterStatus("server_version", "16.0"); err != nil {
		t.Fatalf("second parameter status: %v", err)
	}
	if err := connection.writeCStringMessage('C', "SELECT 1"); err != nil {
		t.Fatalf("second command tag: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected scalar message buffer to be reused")
	}

	want := append(append([]byte(nil), control.Bytes()...), control.Bytes()...)
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestReusableMessageConnectionDoesNotRetainOversizedCString(t *testing.T) {
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	largeValue := strings.Repeat("x", backendMessageReuseMax)

	if err := connection.writeCStringMessage('C', largeValue); err != nil {
		t.Fatalf("oversized scalar write: %v", err)
	}
	if len(connection.buffer) != 0 {
		t.Fatalf("retained oversized scalar buffer length = %d, want 0", len(connection.buffer))
	}
}
