package hatPeer

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func BenchmarkPerformCompactPeerHandshake(b *testing.B) {
	var response [compactPeerHandshakeResponseBytes]byte
	copy(response[:4], compactPeerHandshakeMagic)
	response[4] = compactPeerHandshakeAccepted
	response[5] = CompactPeerHandshakeVersion1
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn := compactPeerHandshakeBenchmarkConn{response: response}
		if _, err := PerformCompactPeerHandshake(context.Background(), &conn, CompactPeerHandshakeOptions{Features: 0x03}); err != nil {
			b.Fatal(err)
		}
	}
}

type compactPeerHandshakeBenchmarkConn struct {
	response [compactPeerHandshakeResponseBytes]byte
	offset   int
}

func (conn *compactPeerHandshakeBenchmarkConn) Read(bytes []byte) (int, error) {
	if conn.offset == len(conn.response) {
		return 0, io.EOF
	}
	written := copy(bytes, conn.response[conn.offset:])
	conn.offset += written
	return written, nil
}

func (conn *compactPeerHandshakeBenchmarkConn) Write(bytes []byte) (int, error) {
	if len(bytes) != compactPeerHandshakeRequestBytes || bytes[4] != CompactPeerHandshakeVersion1 {
		return 0, io.ErrUnexpectedEOF
	}
	_ = binary.BigEndian.Uint32(bytes[5:])
	return len(bytes), nil
}

func (conn *compactPeerHandshakeBenchmarkConn) Close() error { return nil }

func (conn *compactPeerHandshakeBenchmarkConn) LocalAddr() net.Addr { return nil }

func (conn *compactPeerHandshakeBenchmarkConn) RemoteAddr() net.Addr { return nil }

func (conn *compactPeerHandshakeBenchmarkConn) SetDeadline(time.Time) error { return nil }

func (conn *compactPeerHandshakeBenchmarkConn) SetReadDeadline(time.Time) error { return nil }

func (conn *compactPeerHandshakeBenchmarkConn) SetWriteDeadline(time.Time) error { return nil }
