package hatPgWire

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestReusableMessageConnectionWritesDataRowIntoPacket(t *testing.T) {
	first := "alpha"
	second := "beta"
	row := []*string{&first, nil, &second}
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}

	if err := connection.writeDataRow(row); err != nil {
		t.Fatalf("first row write: %v", err)
	}
	if len(connection.buffer) == 0 {
		t.Fatal("expected a retained response buffer")
	}
	firstAddress := &connection.buffer[:1][0]

	if err := connection.writeDataRow(row); err != nil {
		t.Fatalf("second row write: %v", err)
	}
	if &connection.buffer[:1][0] != firstAddress {
		t.Fatal("expected the row packet buffer to be reused")
	}

	want := append(expectedDataRowPacket(row), expectedDataRowPacket(row)...)
	if got := connection.Conn.(*backendMessageTestConn).Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("response bytes = %x, want %x", got, want)
	}
}

func TestReusableMessageConnectionDoesNotRetainOversizedDataRow(t *testing.T) {
	largeValue := strings.Repeat("x", backendMessageReuseMax)
	connection := &reusableMessageConnection{Conn: &backendMessageTestConn{}}

	if err := connection.writeDataRow([]*string{&largeValue}); err != nil {
		t.Fatalf("oversized row write: %v", err)
	}
	if len(connection.buffer) != 0 {
		t.Fatalf("retained oversized row buffer length = %d, want 0", len(connection.buffer))
	}
}

func expectedDataRowPacket(row []*string) []byte {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body, uint16(len(row)))
	for _, value := range row {
		if value == nil {
			body = append(body, 0xff, 0xff, 0xff, 0xff)
			continue
		}
		body = append(body, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(body[len(body)-4:], uint32(len(*value)))
		body = append(body, (*value)...)
	}
	packet := make([]byte, 5+len(body))
	packet[0] = 'D'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	return packet
}
