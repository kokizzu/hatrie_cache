package hatSql

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

type mz010SubscriptionTransportBenchmarkConn struct {
	readData []byte
	readPos  int
}

func (conn *mz010SubscriptionTransportBenchmarkConn) Read(target []byte) (int, error) {
	if len(conn.readData) == 0 {
		return 0, io.EOF
	}
	if conn.readPos >= len(conn.readData) {
		conn.readPos = 0
	}
	count := copy(target, conn.readData[conn.readPos:])
	conn.readPos += count
	return count, nil
}

func (conn *mz010SubscriptionTransportBenchmarkConn) Write(source []byte) (int, error) {
	return len(source), nil
}

func (conn *mz010SubscriptionTransportBenchmarkConn) Close() error { return nil }

func (conn *mz010SubscriptionTransportBenchmarkConn) LocalAddr() net.Addr {
	return mz010SubscriptionTransportBenchmarkAddr{}
}

func (conn *mz010SubscriptionTransportBenchmarkConn) RemoteAddr() net.Addr {
	return mz010SubscriptionTransportBenchmarkAddr{}
}

func (conn *mz010SubscriptionTransportBenchmarkConn) SetDeadline(time.Time) error { return nil }

func (conn *mz010SubscriptionTransportBenchmarkConn) SetReadDeadline(time.Time) error { return nil }

func (conn *mz010SubscriptionTransportBenchmarkConn) SetWriteDeadline(time.Time) error { return nil }

type mz010SubscriptionTransportBenchmarkAddr struct{}

func (mz010SubscriptionTransportBenchmarkAddr) Network() string { return "benchmark" }

func (mz010SubscriptionTransportBenchmarkAddr) String() string { return "benchmark" }

func BenchmarkMZ010SQLSubscriptionWireTransportSend(b *testing.B) {
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key"))
	if err != nil {
		b.Fatal(err)
	}
	conn := &mz010SubscriptionTransportBenchmarkConn{}
	transport, err := NewSQLSubscriptionWireTransport(conn, ring)
	if err != nil {
		b.Fatal(err)
	}
	envelope := SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		envelope.Sequence = uint64(index)
		if err := transport.Send(context.Background(), envelope); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(len(envelope.Payload)))
}

func BenchmarkMZ010SQLSubscriptionWireTransportReceive(b *testing.B) {
	ring, err := NewSQLSubscriptionWireKeyring([]byte("subscription-key"))
	if err != nil {
		b.Fatal(err)
	}
	wire, err := ring.Seal(SQLSubscriptionWireEnvelope{
		Mode:           SQLSubscriptionModeDifferential,
		SubscriptionID: "people-live",
		Sequence:       42,
		Payload:        []byte(`{"id":7,"name":"Ada","active":true,"score":123.5}`),
	})
	if err != nil {
		b.Fatal(err)
	}
	frame := bytes.NewBuffer(make([]byte, SQLSubscriptionWireTransportFrameHeaderBytes, SQLSubscriptionWireTransportFrameHeaderBytes+len(wire)))
	frameBytes := frame.Bytes()
	frameBytes[0] = byte(len(wire) >> 24)
	frameBytes[1] = byte(len(wire) >> 16)
	frameBytes[2] = byte(len(wire) >> 8)
	frameBytes[3] = byte(len(wire))
	frame.Write(wire)
	conn := &mz010SubscriptionTransportBenchmarkConn{readData: frame.Bytes()}
	transport, err := NewSQLSubscriptionWireTransport(conn, ring)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(wire)))
	b.ResetTimer()
	for range b.N {
		if _, err := transport.Receive(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
