package hatSql

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	// SQLSubscriptionWireTransportFrameHeaderBytes is the fixed big-endian
	// length prefix before one authenticated HSE1 envelope.
	SQLSubscriptionWireTransportFrameHeaderBytes = 4
	// MaxSQLSubscriptionWireTransportFrameBytes bounds one received envelope
	// before its payload is allocated.
	MaxSQLSubscriptionWireTransportFrameBytes = MaxSQLSubscriptionWireEnvelopeBytes
)

var (
	// ErrSQLSubscriptionWireTransportInvalid indicates an unusable connection,
	// codec, or transport state.
	ErrSQLSubscriptionWireTransportInvalid = errors.New("hatSql: invalid SQL subscription wire transport")
	// ErrSQLSubscriptionWireTransportFrameTooLarge indicates that a frame length
	// exceeded the transport allocation bound.
	ErrSQLSubscriptionWireTransportFrameTooLarge = errors.New("hatSql: SQL subscription wire transport frame is too large")
)

// SQLSubscriptionWireCodec authenticates subscription envelopes for a
// transport. SQLSubscriptionWireKeyring satisfies this interface and permits
// bounded key rotation without changing the HSE1 envelope format.
type SQLSubscriptionWireCodec interface {
	Seal(SQLSubscriptionWireEnvelope) ([]byte, error)
	Open([]byte) (SQLSubscriptionWireEnvelope, error)
}

// SQLSubscriptionWireTransport carries authenticated envelopes over a
// net.Conn. It is opt-in and owns neither the connection nor the codec.
// Concurrent sends and receives are serialized independently so one writer
// cannot interleave frame bytes while a read proceeds in the other direction.
type SQLSubscriptionWireTransport struct {
	conn  net.Conn
	codec SQLSubscriptionWireCodec
	read  sync.Mutex
	write sync.Mutex
}

// NewSQLSubscriptionWireTransport creates a length-framed transport. The
// caller remains responsible for closing conn.
func NewSQLSubscriptionWireTransport(conn net.Conn, codec SQLSubscriptionWireCodec) (*SQLSubscriptionWireTransport, error) {
	if conn == nil || codec == nil {
		return nil, ErrSQLSubscriptionWireTransportInvalid
	}
	return &SQLSubscriptionWireTransport{conn: conn, codec: codec}, nil
}

// Send seals one envelope and writes exactly one length-prefixed frame. A
// context deadline or cancellation interrupts a blocked connection operation.
func (transport *SQLSubscriptionWireTransport) Send(ctx context.Context, envelope SQLSubscriptionWireEnvelope) error {
	if transport == nil || transport.conn == nil || transport.codec == nil {
		return ErrSQLSubscriptionWireTransportInvalid
	}
	if err := sqlSubscriptionWireTransportContextErr(ctx); err != nil {
		return err
	}
	wire, err := transport.codec.Seal(envelope)
	if err != nil {
		return err
	}
	if len(wire) == 0 || len(wire) > MaxSQLSubscriptionWireTransportFrameBytes {
		return ErrSQLSubscriptionWireTransportFrameTooLarge
	}
	var prefix [SQLSubscriptionWireTransportFrameHeaderBytes]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(wire)))
	transport.write.Lock()
	defer transport.write.Unlock()
	return sqlSubscriptionWireTransportWithContext(ctx, transport.conn.SetWriteDeadline, func() error {
		if err := sqlSubscriptionWireTransportWriteFull(transport.conn, prefix[:]); err != nil {
			return err
		}
		return sqlSubscriptionWireTransportWriteFull(transport.conn, wire)
	})
}

// Receive reads and authenticates one complete length-prefixed frame. The
// length is checked before allocating the envelope buffer.
func (transport *SQLSubscriptionWireTransport) Receive(ctx context.Context) (SQLSubscriptionWireEnvelope, error) {
	if transport == nil || transport.conn == nil || transport.codec == nil {
		return SQLSubscriptionWireEnvelope{}, ErrSQLSubscriptionWireTransportInvalid
	}
	transport.read.Lock()
	defer transport.read.Unlock()
	var envelope SQLSubscriptionWireEnvelope
	err := sqlSubscriptionWireTransportWithContext(ctx, transport.conn.SetReadDeadline, func() error {
		var prefix [SQLSubscriptionWireTransportFrameHeaderBytes]byte
		if _, err := io.ReadFull(transport.conn, prefix[:]); err != nil {
			return err
		}
		frameLength := binary.BigEndian.Uint32(prefix[:])
		if frameLength == 0 || uint64(frameLength) > uint64(MaxSQLSubscriptionWireTransportFrameBytes) {
			return ErrSQLSubscriptionWireTransportFrameTooLarge
		}
		wire := make([]byte, int(frameLength))
		if _, err := io.ReadFull(transport.conn, wire); err != nil {
			return err
		}
		var openErr error
		envelope, openErr = transport.codec.Open(wire)
		return openErr
	})
	if err != nil {
		return SQLSubscriptionWireEnvelope{}, err
	}
	return envelope, nil
}

func sqlSubscriptionWireTransportWriteFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if written < 0 || written > len(data) {
			return ErrSQLSubscriptionWireTransportInvalid
		}
		data = data[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func sqlSubscriptionWireTransportWithContext(ctx context.Context, setDeadline func(time.Time) error, operation func() error) error {
	if err := sqlSubscriptionWireTransportContextErr(ctx); err != nil {
		return err
	}
	if ctx.Done() == nil {
		return operation()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := setDeadline(deadline); err != nil {
			return err
		}
	}
	stop := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = setDeadline(time.Now())
		case <-stop:
		}
	}()
	err := operation()
	close(stop)
	<-watcherDone
	_ = setDeadline(time.Time{})
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
			if deadline, hasDeadline := ctx.Deadline(); hasDeadline && !time.Now().Before(deadline) {
				return context.DeadlineExceeded
			}
		}
	}
	return err
}

func sqlSubscriptionWireTransportContextErr(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
