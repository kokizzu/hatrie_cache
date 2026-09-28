package hatPgWire

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestReadFrontendMessageIntoReusesMessageBuffer(t *testing.T) {
	data := appendFrontendTestMessage(nil, 'Q', []byte("SELECT 1\x00"))
	data = appendFrontendTestMessage(data, 'S', nil)
	connection := &frontendMessageTestConn{reader: bytes.NewReader(data)}

	messageType, body, buffer, err := readFrontendMessageInto(connection, 1024, nil)
	if err != nil {
		t.Fatalf("first read error = %v", err)
	}
	if messageType != 'Q' || string(body) != "SELECT 1\x00" {
		t.Fatalf("first message = %q %q", messageType, body)
	}
	if len(buffer) != len(body) || len(buffer) == 0 || &buffer[0] != &body[0] {
		t.Fatal("first message did not return its reusable buffer")
	}
	firstAddress := &buffer[0]

	messageType, body, buffer, err = readFrontendMessageInto(connection, 1024, buffer)
	if err != nil {
		t.Fatalf("second read error = %v", err)
	}
	if messageType != 'S' || len(body) != 0 {
		t.Fatalf("second message = %q %q", messageType, body)
	}
	if len(buffer) != 0 || &buffer[:1][0] != firstAddress {
		t.Fatal("second message did not preserve the reusable buffer")
	}
}

func TestReadFrontendMessageIntoDoesNotRetainOversizedBuffer(t *testing.T) {
	largeBody := bytes.Repeat([]byte{'x'}, 64<<10+1)
	data := appendFrontendTestMessage(nil, 'Q', largeBody)
	connection := &frontendMessageTestConn{reader: bytes.NewReader(data)}

	messageType, body, buffer, err := readFrontendMessageInto(connection, 1<<20, nil)
	if err != nil {
		t.Fatalf("large read error = %v", err)
	}
	if messageType != 'Q' || len(body) != len(largeBody) || len(buffer) != 0 {
		t.Fatalf("large message = type %q body %d buffer %d", messageType, len(body), len(buffer))
	}
}

func appendFrontendTestMessage(data []byte, messageType byte, body []byte) []byte {
	header := [5]byte{messageType}
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)+4))
	data = append(data, header[:]...)
	return append(data, body...)
}

type frontendMessageTestConn struct {
	reader *bytes.Reader
}

func (connection *frontendMessageTestConn) Read(data []byte) (int, error) {
	return connection.reader.Read(data)
}

func (connection *frontendMessageTestConn) Write(data []byte) (int, error) {
	return len(data), nil
}

func (connection *frontendMessageTestConn) Close() error { return nil }

func (connection *frontendMessageTestConn) LocalAddr() net.Addr { return frontendTestAddr{} }

func (connection *frontendMessageTestConn) RemoteAddr() net.Addr { return frontendTestAddr{} }

func (connection *frontendMessageTestConn) SetDeadline(time.Time) error { return nil }

func (connection *frontendMessageTestConn) SetReadDeadline(time.Time) error { return nil }

func (connection *frontendMessageTestConn) SetWriteDeadline(time.Time) error { return nil }

type frontendTestAddr struct{}

func (frontendTestAddr) Network() string { return "test" }

func (frontendTestAddr) String() string { return "test" }

var _ net.Conn = (*frontendMessageTestConn)(nil)
var _ io.Reader = (*frontendMessageTestConn)(nil)
