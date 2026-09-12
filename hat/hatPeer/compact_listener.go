package hatPeer

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCompactPeerListenerRequired       = errors.New("hatPeer: compact peer listener is required")
	ErrCompactPeerAuthorizationRequired  = errors.New("hatPeer: compact peer authorization policy is required")
	ErrCompactPeerListenerOptionsInvalid = errors.New("hatPeer: compact peer listener options are invalid")
	ErrCompactPeerListenerClosed         = errors.New("hatPeer: compact peer listener is closed")
	ErrCompactPeerListenerServeStarted   = errors.New("hatPeer: compact peer listener serve already started")
	ErrCompactPeerHandshakeInvalid       = errors.New("hatPeer: compact peer handshake is invalid")
	ErrCompactPeerHandshakeRejected      = errors.New("hatPeer: compact peer handshake was rejected")
	ErrCompactPeerTLSRequired            = errors.New("hatPeer: compact peer TLS transport is required")
)

const (
	// CompactPeerHandshakeVersion1 is the first listener handshake version.
	CompactPeerHandshakeVersion1 uint8 = 1

	DefaultCompactPeerHandshakeTimeout       = 5 * time.Second
	DefaultCompactPeerListenerMaxConnections = 256
	maxCompactPeerListenerMaxConnections     = 1 << 16
	compactPeerHandshakeMagic                = "HCP1"
	compactPeerHandshakeRequestBytes         = 9
	compactPeerHandshakeResponseBytes        = 10
	compactPeerHandshakeAccepted             = 1
)

// CompactPeerHandshake is the version and feature set negotiated before a
// CompactPeerSession starts. Features are an application-owned bit set.
type CompactPeerHandshake struct {
	Version  uint8
	Features uint32
}

// CompactPeerHandshakeOptions configures one handshake endpoint. Version zero
// selects CompactPeerHandshakeVersion1; timeout zero selects the bounded
// default.
type CompactPeerHandshakeOptions struct {
	Version  uint8
	Features uint32
	Timeout  time.Duration
}

// CompactPeerAuthorizeFunc authenticates a connection after its handshake
// header has been validated. For network security, callers should use a TLS
// listener and verify the peer certificate in this callback. The callback must
// honor ctx and must not retain conn.
type CompactPeerAuthorizeFunc func(ctx context.Context, conn net.Conn, handshake CompactPeerHandshake) error

// CompactPeerListenerOptions configures a bounded authenticated compact-peer
// listener. Authorize is mandatory, and RequireTLS additionally requires the
// accepted connection to expose a crypto/tls ConnectionState method.
type CompactPeerListenerOptions struct {
	Handshake      CompactPeerHandshakeOptions
	MaxConnections int
	RequireTLS     bool
	Authorize      CompactPeerAuthorizeFunc
	Session        CompactPeerSessionOptions
}

// CompactPeerListenerStats is a point-in-time listener snapshot.
type CompactPeerListenerStats struct {
	Active            int64
	Accepted          uint64
	Rejected          uint64
	HandshakeFailures uint64
	AuthFailures      uint64
	Negotiated        uint64
}

// CompactPeerListener accepts authenticated, version-negotiated compact peer
// sessions. It does not create a listener itself; callers provide a net.Listener
// so they can choose TCP, TLS, Unix sockets, or another transport policy.
type CompactPeerListener struct {
	listener net.Listener
	options  CompactPeerListenerOptions
	context  context.Context
	cancel   context.CancelFunc
	slots    chan struct{}
	done     chan struct{}

	closeOnce         sync.Once
	closeErr          error
	serveMu           sync.Mutex
	serving           bool
	closed            atomic.Bool
	connWG            sync.WaitGroup
	active            atomic.Int64
	accepted          atomic.Uint64
	rejected          atomic.Uint64
	handshakeFailures atomic.Uint64
	authFailures      atomic.Uint64
	negotiated        atomic.Uint64
}

// NewCompactPeerListener validates options without starting the supplied
// listener. Serve must be called explicitly by the owner.
func NewCompactPeerListener(listener net.Listener, options CompactPeerListenerOptions) (*CompactPeerListener, error) {
	if listener == nil {
		return nil, ErrCompactPeerListenerRequired
	}
	if options.Authorize == nil {
		return nil, ErrCompactPeerAuthorizationRequired
	}
	if options.MaxConnections < 0 || options.MaxConnections > maxCompactPeerListenerMaxConnections {
		return nil, ErrCompactPeerListenerOptionsInvalid
	}
	if options.MaxConnections == 0 {
		options.MaxConnections = DefaultCompactPeerListenerMaxConnections
	}
	handshake, err := normalizeCompactPeerHandshakeOptions(options.Handshake)
	if err != nil {
		return nil, err
	}
	if handshake.Version != CompactPeerHandshakeVersion1 {
		return nil, ErrCompactPeerListenerOptionsInvalid
	}
	options.Handshake = handshake
	parent := options.Session.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &CompactPeerListener{
		listener: listener,
		options:  options,
		context:  ctx,
		cancel:   cancel,
		slots:    make(chan struct{}, options.MaxConnections),
		done:     make(chan struct{}),
	}, nil
}

// PerformCompactPeerHandshake negotiates one client-side handshake. A
// successful return clears the temporary deadline so the session owns normal
// connection lifetime behavior.
func PerformCompactPeerHandshake(ctx context.Context, conn net.Conn, options CompactPeerHandshakeOptions) (CompactPeerHandshake, error) {
	if conn == nil {
		return CompactPeerHandshake{}, ErrCompactPeerConnectionRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	handshake, err := normalizeCompactPeerHandshakeOptions(options)
	if err != nil {
		return CompactPeerHandshake{}, err
	}
	clearDeadline, err := beginCompactPeerHandshakeDeadline(ctx, conn, handshake.Timeout)
	if err != nil {
		return CompactPeerHandshake{}, err
	}
	defer clearDeadline()

	var request [compactPeerHandshakeRequestBytes]byte
	copy(request[:4], compactPeerHandshakeMagic)
	request[4] = handshake.Version
	binary.BigEndian.PutUint32(request[5:], handshake.Features)
	if err := writeCompactPeerBytes(conn, request[:]); err != nil {
		return CompactPeerHandshake{}, err
	}
	var response [compactPeerHandshakeResponseBytes]byte
	if _, err := io.ReadFull(conn, response[:]); err != nil {
		return CompactPeerHandshake{}, err
	}
	if string(response[:4]) != compactPeerHandshakeMagic {
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeInvalid
	}
	if response[4] != compactPeerHandshakeAccepted {
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeRejected
	}
	if response[5] != handshake.Version {
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeInvalid
	}
	return CompactPeerHandshake{
		Version:  response[5],
		Features: binary.BigEndian.Uint32(response[6:]),
	}, nil
}

// Serve accepts connections until Close, the supplied context, or the
// underlying listener stops it. Close and context cancellation return
// ErrCompactPeerListenerClosed.
func (server *CompactPeerListener) Serve(ctx context.Context) error {
	if server == nil {
		return ErrCompactPeerListenerRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	server.serveMu.Lock()
	if server.serving {
		server.serveMu.Unlock()
		return ErrCompactPeerListenerServeStarted
	}
	server.serving = true
	server.serveMu.Unlock()
	stopOnContext := context.AfterFunc(ctx, func() {
		server.cancel()
		_ = server.listener.Close()
	})
	defer stopOnContext()
	defer func() {
		server.cancel()
		server.connWG.Wait()
		close(server.done)
	}()

	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if server.closed.Load() || ctx.Err() != nil || server.context.Err() != nil {
				return ErrCompactPeerListenerClosed
			}
			if temporary, ok := err.(net.Error); ok && temporary.Temporary() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			return err
		}
		if server.closed.Load() {
			_ = conn.Close()
			return ErrCompactPeerListenerClosed
		}
		select {
		case server.slots <- struct{}{}:
			server.accepted.Add(1)
			server.connWG.Add(1)
			go func() {
				defer server.connWG.Done()
				server.serveConnection(conn)
			}()
		default:
			server.rejected.Add(1)
			_ = conn.Close()
		}
	}
}

// Close stops accepting connections and cancels all sessions created by the
// listener. It is safe to call more than once.
func (server *CompactPeerListener) Close() error {
	if server == nil {
		return ErrCompactPeerListenerRequired
	}
	server.closeOnce.Do(func() {
		server.closed.Store(true)
		server.cancel()
		server.closeErr = server.listener.Close()
	})
	return server.closeErr
}

// Done returns a channel closed after Serve has stopped and all accepted
// connection handlers have exited.
func (server *CompactPeerListener) Done() <-chan struct{} {
	if server == nil {
		return nil
	}
	return server.done
}

// Stats returns a point-in-time listener snapshot.
func (server *CompactPeerListener) Stats() CompactPeerListenerStats {
	if server == nil {
		return CompactPeerListenerStats{}
	}
	return CompactPeerListenerStats{
		Active:            server.active.Load(),
		Accepted:          server.accepted.Load(),
		Rejected:          server.rejected.Load(),
		HandshakeFailures: server.handshakeFailures.Load(),
		AuthFailures:      server.authFailures.Load(),
		Negotiated:        server.negotiated.Load(),
	}
}

func (server *CompactPeerListener) serveConnection(conn net.Conn) {
	defer func() {
		<-server.slots
		_ = conn.Close()
	}()
	if _, err := server.negotiate(conn); err != nil {
		return
	}
	sessionOptions := server.options.Session
	sessionOptions.Context = server.context
	session, err := NewCompactPeerSession(conn, sessionOptions)
	if err != nil {
		return
	}
	server.active.Add(1)
	defer server.active.Add(-1)
	<-session.Done()
}

func (server *CompactPeerListener) negotiate(conn net.Conn) (CompactPeerHandshake, error) {
	if server.options.RequireTLS {
		if _, ok := conn.(interface{ ConnectionState() tls.ConnectionState }); !ok {
			server.handshakeFailures.Add(1)
			return CompactPeerHandshake{}, ErrCompactPeerTLSRequired
		}
	}
	clearDeadline, err := beginCompactPeerHandshakeDeadline(server.context, conn, server.options.Handshake.Timeout)
	if err != nil {
		server.handshakeFailures.Add(1)
		return CompactPeerHandshake{}, err
	}
	defer clearDeadline()

	var request [compactPeerHandshakeRequestBytes]byte
	if _, err := io.ReadFull(conn, request[:]); err != nil {
		server.handshakeFailures.Add(1)
		return CompactPeerHandshake{}, err
	}
	if string(request[:4]) != compactPeerHandshakeMagic {
		server.handshakeFailures.Add(1)
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeInvalid
	}
	handshake := CompactPeerHandshake{
		Version:  request[4],
		Features: binary.BigEndian.Uint32(request[5:]),
	}
	if handshake.Version != server.options.Handshake.Version {
		server.handshakeFailures.Add(1)
		_ = writeCompactPeerHandshakeResponse(conn, 0, 0, 0)
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeRejected
	}
	authorizeContext, cancel := context.WithTimeout(server.context, server.options.Handshake.Timeout)
	authorizeErr := server.options.Authorize(authorizeContext, conn, handshake)
	cancel()
	if authorizeErr != nil {
		server.authFailures.Add(1)
		server.handshakeFailures.Add(1)
		_ = writeCompactPeerHandshakeResponse(conn, 0, 0, 0)
		return CompactPeerHandshake{}, ErrCompactPeerHandshakeRejected
	}
	selected := CompactPeerHandshake{
		Version:  handshake.Version,
		Features: handshake.Features & server.options.Handshake.Features,
	}
	if err := writeCompactPeerHandshakeResponse(conn, compactPeerHandshakeAccepted, selected.Version, selected.Features); err != nil {
		server.handshakeFailures.Add(1)
		return CompactPeerHandshake{}, err
	}
	server.negotiated.Add(1)
	return selected, nil
}

func normalizeCompactPeerHandshakeOptions(options CompactPeerHandshakeOptions) (CompactPeerHandshakeOptions, error) {
	if options.Version == 0 {
		options.Version = CompactPeerHandshakeVersion1
	}
	if options.Timeout < 0 {
		return CompactPeerHandshakeOptions{}, ErrCompactPeerListenerOptionsInvalid
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultCompactPeerHandshakeTimeout
	}
	return options, nil
}

func beginCompactPeerHandshakeDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) (func(), error) {
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	return func() {
		stop()
		_ = conn.SetDeadline(time.Time{})
	}, nil
}

func writeCompactPeerHandshakeResponse(conn net.Conn, status, version uint8, features uint32) error {
	var response [compactPeerHandshakeResponseBytes]byte
	copy(response[:4], compactPeerHandshakeMagic)
	response[4] = status
	response[5] = version
	binary.BigEndian.PutUint32(response[6:], features)
	return writeCompactPeerBytes(conn, response[:])
}

func writeCompactPeerBytes(writer io.Writer, bytes []byte) error {
	for len(bytes) > 0 {
		written, err := writer.Write(bytes)
		if written > 0 {
			bytes = bytes[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
