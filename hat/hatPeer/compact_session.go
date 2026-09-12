package hatPeer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

var (
	// ErrCompactPeerConnectionRequired indicates that a session needs a socket.
	ErrCompactPeerConnectionRequired = errors.New("hatPeer: compact peer connection is required")
	// ErrCompactPeerOptionsInvalid indicates an invalid session option.
	ErrCompactPeerOptionsInvalid = errors.New("hatPeer: compact peer session options are invalid")
	// ErrCompactPeerClosed indicates that a session was closed locally.
	ErrCompactPeerClosed = errors.New("hatPeer: compact peer session is closed")
	// ErrCompactPeerRemote wraps an error returned by a remote request handler.
	ErrCompactPeerRemote = errors.New("hatPeer: remote compact peer request failed")
	// ErrCompactPeerHandlerRequired indicates that a request arrived without a
	// server handler.
	ErrCompactPeerHandlerRequired = errors.New("hatPeer: compact peer request handler is required")
	// ErrCompactPeerCancellationInvalid indicates a malformed reserved
	// cancellation request.
	ErrCompactPeerCancellationInvalid = errors.New("hatPeer: compact peer cancellation request is invalid")
	// ErrCompactPeerInFlightLimit indicates that a peer sent too many requests
	// while all handler slots were busy.
	ErrCompactPeerInFlightLimit = errors.New("hatPeer: compact peer in-flight limit reached")
)

const (
	// DefaultCompactPeerMaxInFlight bounds both outgoing pending calls and
	// concurrently executing inbound handlers.
	DefaultCompactPeerMaxInFlight = DefaultCompactMultiplexerMaxPending
	maxCompactPeerMaxInFlight     = 1 << 20
)

var compactPeerCancellationCommand = []byte("_hat.peer.cancel.v1")

// CompactPeerHandler handles one inbound compact request. The returned frame
// is normalized to a response carrying the request's ID and command. Handler
// calls may run concurrently up to MaxInFlight.
type CompactPeerHandler func(context.Context, CompactFrame) (CompactFrame, error)

// CompactPeerSessionOptions configures one opt-in compact peer connection.
// Protocol bounds frame decoding before allocation; MaxInFlight bounds
// pending calls and server handler concurrency. A nil Handler is valid for a
// client-only session and receives an explicit error for inbound requests.
type CompactPeerSessionOptions struct {
	Protocol    CompactProtocolOptions
	MaxInFlight int
	Context     context.Context
	Handler     CompactPeerHandler
	Lifecycle   *PeerLifecycleRegistry
	PeerID      string
	// EnableRequestCancellation sends a best-effort reserved request when a
	// caller context cancels. The remote session cancels the matching handler
	// context. It is disabled by default to preserve existing wire behavior.
	EnableRequestCancellation bool
}

// CompactPeerSession adapts CompactProtocol and CompactMultiplexer to a
// bidirectional net.Conn. It supports concurrent out-of-order calls and
// inbound requests on the same connection. It is deliberately an embedded,
// opt-in adapter; it does not open listeners, select authentication, or
// replace the existing HTTP/gRPC/replication servers.
type CompactPeerSession struct {
	conn         net.Conn
	protocol     CompactProtocol
	multiplex    *CompactMultiplexer
	handler      CompactPeerHandler
	inflight     chan struct{}
	context      context.Context
	cancel       context.CancelFunc
	lifecycle    *PeerLifecycleRegistry
	peerID       string
	done         chan struct{}
	readDone     chan struct{}
	closeOnce    sync.Once
	writeMu      sync.Mutex
	stateMu      sync.Mutex
	cancellation *compactPeerCancellationState
	closed       bool
	closeError   error
}

type compactPeerCancellationState struct {
	mu     sync.Mutex
	active map[uint64]context.CancelFunc
}

// NewCompactPeerSession starts a bounded reader loop on conn. Both peers may
// issue calls, so the handler is optional and response frames are accepted on
// every session.
func NewCompactPeerSession(conn net.Conn, options CompactPeerSessionOptions) (*CompactPeerSession, error) {
	if conn == nil {
		return nil, ErrCompactPeerConnectionRequired
	}
	maxInFlight := options.MaxInFlight
	if maxInFlight == 0 {
		maxInFlight = DefaultCompactPeerMaxInFlight
	}
	if maxInFlight < 1 || maxInFlight > maxCompactPeerMaxInFlight {
		return nil, ErrCompactPeerOptionsInvalid
	}
	protocol, err := NewCompactProtocol(options.Protocol)
	if err != nil {
		return nil, fmt.Errorf("%w: protocol: %v", ErrCompactPeerOptionsInvalid, err)
	}
	parent := options.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	multiplexer, err := NewCompactMultiplexerWithOptions(CompactMultiplexerOptions{MaxPending: maxInFlight})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%w: multiplexer: %v", ErrCompactPeerOptionsInvalid, err)
	}
	var cancellation *compactPeerCancellationState
	if options.EnableRequestCancellation {
		cancellation = &compactPeerCancellationState{active: make(map[uint64]context.CancelFunc)}
	}
	session := &CompactPeerSession{
		conn:         conn,
		protocol:     protocol,
		multiplex:    multiplexer,
		handler:      options.Handler,
		inflight:     make(chan struct{}, maxInFlight),
		context:      ctx,
		cancel:       cancel,
		lifecycle:    options.Lifecycle,
		peerID:       options.PeerID,
		done:         make(chan struct{}),
		readDone:     make(chan struct{}),
		cancellation: cancellation,
	}
	go session.readLoop()
	go session.watchContext()
	session.emitLifecycle(PeerLifecycleEvent{Kind: PeerLifecycleConnected})
	return session, nil
}

// Call sends one request and waits for its correlated response. A canceled
// call removes its local pending entry; the eventual remote response is
// ignored if it races with cancellation.
func (session *CompactPeerSession) Call(ctx context.Context, command, payload []byte) (CompactFrame, error) {
	if session == nil {
		return CompactFrame{}, ErrCompactPeerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return CompactFrame{}, err
	}
	if err := session.Err(); err != nil {
		return CompactFrame{}, err
	}
	request, pending, err := session.multiplex.Request(command, payload)
	if err != nil {
		return CompactFrame{}, err
	}
	if err := session.write(request); err != nil {
		session.fail(err)
		return CompactFrame{}, err
	}
	response, err := pending.Wait(ctx)
	if err != nil {
		if ctx.Err() != nil {
			session.multiplex.Cancel(request.RequestID)
			session.sendRequestCancellation(request.RequestID)
			return CompactFrame{}, ctx.Err()
		}
		return CompactFrame{}, err
	}
	if response.Kind == CompactError {
		return CompactFrame{}, fmt.Errorf("%w: %s", ErrCompactPeerRemote, response.Payload)
	}
	return response, nil
}

// Close terminates the connection, cancels handlers, and wakes all pending
// calls. It is safe to call more than once.
func (session *CompactPeerSession) Close() error {
	if session == nil {
		return ErrCompactPeerClosed
	}
	session.terminate(ErrCompactPeerClosed, true)
	<-session.readDone
	return nil
}

// Done returns a channel closed when the session terminates.
func (session *CompactPeerSession) Done() <-chan struct{} {
	if session == nil {
		return nil
	}
	return session.done
}

// Err returns the terminal session error, or nil while it is active.
func (session *CompactPeerSession) Err() error {
	if session == nil {
		return ErrCompactPeerClosed
	}
	session.stateMu.Lock()
	err := session.closeError
	session.stateMu.Unlock()
	return err
}

func (session *CompactPeerSession) readLoop() {
	defer close(session.readDone)
	for {
		frame, err := session.protocol.Read(session.conn)
		if err != nil {
			session.fail(err)
			return
		}
		switch frame.Kind {
		case CompactResponse, CompactError:
			if err := session.multiplex.Resolve(frame); err != nil && !errors.Is(err, ErrCompactMultiplexerUnknownRequest) {
				session.fail(err)
				return
			}
		case CompactRequest:
			if bytes.Equal(frame.Command, compactPeerCancellationCommand) {
				if len(frame.Payload) != 0 {
					session.writeError(frame, ErrCompactPeerCancellationInvalid)
					continue
				}
				session.cancelInbound(frame.RequestID)
				continue
			}
			session.dispatch(frame)
		}
	}
}

func (session *CompactPeerSession) dispatch(request CompactFrame) {
	if session.handler == nil {
		session.writeError(request, ErrCompactPeerHandlerRequired)
		return
	}
	select {
	case session.inflight <- struct{}{}:
		handlerContext := session.context
		var cancel context.CancelFunc
		if session.cancellation != nil {
			handlerContext, cancel = context.WithCancel(session.context)
			session.registerInbound(request.RequestID, cancel)
		}
		go session.handle(handlerContext, request, cancel)
	default:
		session.writeError(request, ErrCompactPeerInFlightLimit)
	}
}

func (session *CompactPeerSession) handle(ctx context.Context, request CompactFrame, cancel context.CancelFunc) {
	defer func() { <-session.inflight }()
	if cancel != nil {
		defer func() {
			session.unregisterInbound(request.RequestID)
			cancel()
		}()
	}
	response, err := session.handler(ctx, request)
	if err != nil {
		response = CompactFrame{Kind: CompactError, Command: request.Command, Payload: compactPeerErrorPayload(session.protocol, err)}
	} else {
		response.Kind = CompactResponse
		if len(response.Command) == 0 {
			response.Command = request.Command
		}
	}
	response.RequestID = request.RequestID
	if err := session.write(response); err != nil {
		session.fail(err)
	}
}

func (session *CompactPeerSession) registerInbound(requestID uint64, cancel context.CancelFunc) {
	state := session.cancellation
	if state == nil {
		return
	}
	state.mu.Lock()
	state.active[requestID] = cancel
	state.mu.Unlock()
}

func (session *CompactPeerSession) unregisterInbound(requestID uint64) {
	state := session.cancellation
	if state == nil {
		return
	}
	state.mu.Lock()
	delete(state.active, requestID)
	state.mu.Unlock()
}

func (session *CompactPeerSession) cancelInbound(requestID uint64) {
	if requestID == 0 {
		return
	}
	state := session.cancellation
	if state == nil {
		return
	}
	state.mu.Lock()
	cancel := state.active[requestID]
	state.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (session *CompactPeerSession) sendRequestCancellation(requestID uint64) {
	if session == nil || session.cancellation == nil || requestID == 0 {
		return
	}
	_ = session.write(CompactFrame{
		Kind:      CompactRequest,
		RequestID: requestID,
		Command:   compactPeerCancellationCommand,
	})
}

func (session *CompactPeerSession) writeError(request CompactFrame, err error) {
	response := CompactFrame{
		Kind:      CompactError,
		RequestID: request.RequestID,
		Command:   request.Command,
		Payload:   compactPeerErrorPayload(session.protocol, err),
	}
	if writeErr := session.write(response); writeErr != nil {
		session.fail(writeErr)
	}
}

func (session *CompactPeerSession) write(frame CompactFrame) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	if err := session.Err(); err != nil {
		return err
	}
	return session.protocol.Write(session.conn, frame)
}

func (session *CompactPeerSession) watchContext() {
	select {
	case <-session.context.Done():
		session.fail(session.context.Err())
	case <-session.done:
	}
}

func (session *CompactPeerSession) fail(err error) {
	session.terminate(err, false)
}

func (session *CompactPeerSession) terminate(err error, shutdown bool) {
	if err == nil {
		err = ErrCompactPeerClosed
	}
	session.closeOnce.Do(func() {
		session.stateMu.Lock()
		session.closed = true
		session.closeError = err
		session.stateMu.Unlock()
		session.cancel()
		session.multiplex.Close(err)
		_ = session.conn.Close()
		close(session.done)
		if session.lifecycle != nil {
			go session.emitTerminalLifecycle(err, shutdown)
		}
	})
}

func (session *CompactPeerSession) emitLifecycle(event PeerLifecycleEvent) {
	if session.lifecycle == nil {
		return
	}
	event.PeerID = session.peerID
	_ = session.lifecycle.Emit(event)
}

func (session *CompactPeerSession) emitTerminalLifecycle(err error, shutdown bool) {
	event := PeerLifecycleEvent{Kind: PeerLifecycleDisconnected}
	if err != nil {
		event.Error = err.Error()
	}
	session.emitLifecycle(event)
	if shutdown {
		session.emitLifecycle(PeerLifecycleEvent{Kind: PeerLifecycleShutdown, Error: event.Error})
	}
}

func compactPeerErrorPayload(protocol CompactProtocol, err error) []byte {
	if err == nil {
		return nil
	}
	payload := []byte(err.Error())
	if len(payload) > protocol.maxPayloadBytes {
		payload = payload[:protocol.maxPayloadBytes]
	}
	return payload
}
