package hatPgWire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReusableMessageConnectionWritesFixedMessagesIntoPacket(t *testing.T) {
	control := &backendMessageTestConn{}
	if err := writeAuthenticationOK(control); err != nil {
		t.Fatalf("control authentication ok: %v", err)
	}
	if err := writeAuthenticationCleartextPassword(control); err != nil {
		t.Fatalf("control cleartext authentication: %v", err)
	}
	if err := writeReadyForQuery(control); err != nil {
		t.Fatalf("control ready: %v", err)
	}

	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	if err := connection.writeAuthenticationOK(); err != nil {
		t.Fatalf("first authentication ok: %v", err)
	}
	firstAddress := &connection.buffer[:1][0]
	if err := connection.writeAuthenticationCleartextPassword(); err != nil {
		t.Fatalf("first cleartext authentication: %v", err)
	}
	if err := connection.writeReadyForQuery(); err != nil {
		t.Fatalf("first ready: %v", err)
	}
	if err := connection.writeAuthenticationOK(); err != nil {
		t.Fatalf("second authentication ok: %v", err)
	}
	if err := connection.writeAuthenticationCleartextPassword(); err != nil {
		t.Fatalf("second cleartext authentication: %v", err)
	}
	if err := connection.writeReadyForQuery(); err != nil {
		t.Fatalf("second ready: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected fixed-message buffer to be reused")
	}

	want := append(append([]byte(nil), control.Bytes()...), control.Bytes()...)
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestReusableMessageConnectionWritesBackendKeyData(t *testing.T) {
	control := &backendMessageTestConn{}
	keyData := make([]byte, 8)
	binary.BigEndian.PutUint32(keyData[:4], 11)
	binary.BigEndian.PutUint32(keyData[4:], 22)
	if err := writeMessage(control, 'K', keyData); err != nil {
		t.Fatalf("control key data: %v", err)
	}

	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	if err := connection.writeBackendKeyData(11, 22); err != nil {
		t.Fatalf("reusable key data: %v", err)
	}
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, control.Bytes()) {
		t.Fatalf("response bytes = %x, want %x", got, control.Bytes())
	}
}
