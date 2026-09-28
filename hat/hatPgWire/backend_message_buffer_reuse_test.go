package hatPgWire

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func TestWriteMessageReusesConnectionBuffer(t *testing.T) {
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	body := bytes.Repeat([]byte{'x'}, 256)

	if err := writeMessage(connection, 'D', body); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if len(connection.buffer) == 0 {
		t.Fatal("expected a retained response buffer")
	}
	firstAddress := &connection.buffer[:1][0]

	if err := writeMessage(connection, 'D', body); err != nil {
		t.Fatalf("second write: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected the response buffer to be reused")
	}

	if got, want := connection.Conn.(*backendMessageTestConn).Bytes(), append(append([]byte(nil), messagePacket('D', body)...), messagePacket('D', body)...); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestWriteMessageDoesNotRetainOversizedBuffer(t *testing.T) {
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}
	largeBody := make([]byte, backendMessageReuseMax)

	if err := writeMessage(connection, 'D', largeBody); err != nil {
		t.Fatalf("oversized write: %v", err)
	}
	if len(connection.buffer) != 0 {
		t.Fatalf("retained oversized buffer length = %d, want 0", len(connection.buffer))
	}
}

func messagePacket(messageType byte, body []byte) []byte {
	packet := make([]byte, 5+len(body))
	packet[0] = messageType
	length := 4 + len(body)
	packet[1] = byte(length >> 24)
	packet[2] = byte(length >> 16)
	packet[3] = byte(length >> 8)
	packet[4] = byte(length)
	copy(packet[5:], body)
	return packet
}

type backendMessageTestConn struct {
	bytes.Buffer
}

func (connection *backendMessageTestConn) Close() error                     { return nil }
func (connection *backendMessageTestConn) LocalAddr() net.Addr              { return backendMessageTestAddr{} }
func (connection *backendMessageTestConn) RemoteAddr() net.Addr             { return backendMessageTestAddr{} }
func (connection *backendMessageTestConn) SetDeadline(time.Time) error      { return nil }
func (connection *backendMessageTestConn) SetReadDeadline(time.Time) error  { return nil }
func (connection *backendMessageTestConn) SetWriteDeadline(time.Time) error { return nil }

type backendMessageTestAddr struct{}

func (backendMessageTestAddr) Network() string { return "test" }
func (backendMessageTestAddr) String() string  { return "test" }
