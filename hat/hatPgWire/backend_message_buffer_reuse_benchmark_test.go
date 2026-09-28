package hatPgWire

import (
	"net"
	"testing"
	"time"
)

func BenchmarkWriteMessageAllocating(b *testing.B) {
	connection := &backendMessageBenchmarkConn{}
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeMessage(connection, 'D', body); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteMessageInto(b *testing.B) {
	connection := &reusableMessageConnection{Conn: &backendMessageBenchmarkConn{}}
	body := make([]byte, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeMessage(connection, 'D', body); err != nil {
			b.Fatal(err)
		}
	}
}

type backendMessageBenchmarkConn struct{}

func (connection *backendMessageBenchmarkConn) Read([]byte) (int, error)       { return 0, nil }
func (connection *backendMessageBenchmarkConn) Write(data []byte) (int, error) { return len(data), nil }
func (connection *backendMessageBenchmarkConn) Close() error                   { return nil }
func (connection *backendMessageBenchmarkConn) LocalAddr() net.Addr {
	return backendMessageBenchmarkAddr{}
}
func (connection *backendMessageBenchmarkConn) RemoteAddr() net.Addr {
	return backendMessageBenchmarkAddr{}
}
func (connection *backendMessageBenchmarkConn) SetDeadline(time.Time) error { return nil }
func (connection *backendMessageBenchmarkConn) SetReadDeadline(time.Time) error {
	return nil
}
func (connection *backendMessageBenchmarkConn) SetWriteDeadline(time.Time) error {
	return nil
}

type backendMessageBenchmarkAddr struct{}

func (backendMessageBenchmarkAddr) Network() string { return "benchmark" }
func (backendMessageBenchmarkAddr) String() string  { return "benchmark" }
