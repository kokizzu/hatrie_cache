package hatPeer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
)

var (
	// ErrCompactPeerStreamOptionsInvalid indicates an invalid stream bound.
	ErrCompactPeerStreamOptionsInvalid = errors.New("hatPeer: compact peer stream options are invalid")
	// ErrCompactPeerStreamHandlerRequired indicates that a stream request has
	// no server-side handler.
	ErrCompactPeerStreamHandlerRequired = errors.New("hatPeer: compact peer stream handler is required")
	// ErrCompactPeerStreamClosed indicates that a stream is no longer open.
	ErrCompactPeerStreamClosed = errors.New("hatPeer: compact peer stream is closed")
	// ErrCompactPeerStreamLimit indicates that the local stream bound is full.
	ErrCompactPeerStreamLimit = errors.New("hatPeer: compact peer stream limit reached")
	// ErrCompactPeerStreamState indicates an invalid remote stream transition.
	ErrCompactPeerStreamState = errors.New("hatPeer: compact peer stream state is invalid")
	// ErrCompactPeerStreamProtocol indicates an invalid stream envelope.
	ErrCompactPeerStreamProtocol = errors.New("hatPeer: compact peer stream protocol is invalid")
	// ErrCompactPeerStreamPayloadTooLarge indicates that the stream envelope
	// cannot fit within the compact session payload bound.
	ErrCompactPeerStreamPayloadTooLarge = errors.New("hatPeer: compact peer stream payload is too large")
)

const (
	// DefaultCompactPeerMaxStreams bounds active streams in each direction.
	DefaultCompactPeerMaxStreams = 64
	maxCompactPeerMaxStreams     = 1 << 16
	compactPeerStreamVersion     = 1
)

var compactPeerStreamCommand = []byte("_hat.peer.stream.v1")

// CompactPeerStreamOperation identifies one stream transaction operation.
type CompactPeerStreamOperation byte

const (
	CompactPeerStreamBegin CompactPeerStreamOperation = iota + 1
	CompactPeerStreamCall
	CompactPeerStreamCommit
	CompactPeerStreamRollback
)

func (operation CompactPeerStreamOperation) valid() bool {
	return operation >= CompactPeerStreamBegin && operation <= CompactPeerStreamRollback
}

// String returns the stable name of a stream operation.
func (operation CompactPeerStreamOperation) String() string {
	switch operation {
	case CompactPeerStreamBegin:
		return "begin"
	case CompactPeerStreamCall:
		return "call"
	case CompactPeerStreamCommit:
		return "commit"
	case CompactPeerStreamRollback:
		return "rollback"
	default:
		return "unknown"
	}
}

// CompactPeerStreamRequest is delivered to the server-side stream handler.
// Command and Payload are borrowed from the received compact frame and must
// not be retained or modified by the handler.
type CompactPeerStreamRequest struct {
	Operation CompactPeerStreamOperation
	StreamID  uint64
	Command   []byte
	Payload   []byte
}

// CompactPeerStreamHandler handles one ordered operation in a remote stream.
// The handler owns the actual storage transaction associated with StreamID.
type CompactPeerStreamHandler func(context.Context, CompactPeerStreamRequest) (CompactFrame, error)

// CompactPeerStreamOptions configures a bidirectional stream endpoint. The
// embedded session options still control protocol limits, peer lifecycle
// events, and ordinary non-stream requests.
type CompactPeerStreamOptions struct {
	Session    CompactPeerSessionOptions
	MaxStreams int
	Handler    CompactPeerStreamHandler
}

// CompactPeerStreamEndpoint adds bounded begin/call/commit/rollback streams to
// a CompactPeerSession. Ordinary compact requests continue to use the
// embedded session handler.
type CompactPeerStreamEndpoint struct {
	session       *CompactPeerSession
	maxStreams    int
	streamHandler CompactPeerStreamHandler
	ordinary      CompactPeerHandler

	mu       sync.Mutex
	nextID   uint64
	incoming map[uint64]*compactPeerIncomingStream
	outgoing map[uint64]*CompactPeerStream
}

type compactPeerIncomingStream struct {
	mu     sync.Mutex
	closed bool
}

// CompactPeerStream is one serialized logical transaction over an endpoint.
// Different streams may use the underlying session concurrently, while calls
// within one stream remain ordered.
type CompactPeerStream struct {
	endpoint *CompactPeerStreamEndpoint
	streamID uint64
	mu       sync.Mutex
	closed   bool
}

// NewCompactPeerStreamEndpoint starts a stream-capable compact peer endpoint.
// A nil Handler is valid for a client-only endpoint; a remote begin request
// then receives ErrCompactPeerStreamHandlerRequired.
func NewCompactPeerStreamEndpoint(conn net.Conn, options CompactPeerStreamOptions) (*CompactPeerStreamEndpoint, error) {
	maxStreams := options.MaxStreams
	if maxStreams == 0 {
		maxStreams = DefaultCompactPeerMaxStreams
	}
	if maxStreams < 1 || maxStreams > maxCompactPeerMaxStreams {
		return nil, ErrCompactPeerStreamOptionsInvalid
	}
	endpoint := &CompactPeerStreamEndpoint{
		maxStreams:    maxStreams,
		streamHandler: options.Handler,
		ordinary:      options.Session.Handler,
		incoming:      make(map[uint64]*compactPeerIncomingStream),
		outgoing:      make(map[uint64]*CompactPeerStream),
	}
	sessionOptions := options.Session
	sessionOptions.Handler = endpoint.handleRequest
	session, err := NewCompactPeerSession(conn, sessionOptions)
	if err != nil {
		return nil, err
	}
	endpoint.session = session
	return endpoint, nil
}

// OpenStream starts a remote stream transaction.
func (endpoint *CompactPeerStreamEndpoint) OpenStream(ctx context.Context) (*CompactPeerStream, error) {
	if endpoint == nil || endpoint.session == nil {
		return nil, ErrCompactPeerStreamClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stream := &CompactPeerStream{endpoint: endpoint}
	endpoint.mu.Lock()
	if len(endpoint.outgoing) >= endpoint.maxStreams {
		endpoint.mu.Unlock()
		return nil, ErrCompactPeerStreamLimit
	}
	stream.streamID = endpoint.nextStreamIDLocked()
	endpoint.outgoing[stream.streamID] = stream
	endpoint.mu.Unlock()
	if _, err := endpoint.callStream(ctx, CompactPeerStreamBegin, stream.streamID, nil, nil); err != nil {
		endpoint.removeOutgoing(stream)
		stream.mu.Lock()
		stream.closed = true
		stream.mu.Unlock()
		return nil, err
	}
	return stream, nil
}

// Close terminates the underlying compact session.
func (endpoint *CompactPeerStreamEndpoint) Close() error {
	if endpoint == nil || endpoint.session == nil {
		return ErrCompactPeerStreamClosed
	}
	return endpoint.session.Close()
}

// Done returns a channel closed when the underlying session terminates.
func (endpoint *CompactPeerStreamEndpoint) Done() <-chan struct{} {
	if endpoint == nil || endpoint.session == nil {
		return nil
	}
	return endpoint.session.Done()
}

// Err returns the underlying terminal session error.
func (endpoint *CompactPeerStreamEndpoint) Err() error {
	if endpoint == nil || endpoint.session == nil {
		return ErrCompactPeerStreamClosed
	}
	return endpoint.session.Err()
}

// Session returns the underlying compact peer session for non-stream calls.
func (endpoint *CompactPeerStreamEndpoint) Session() *CompactPeerSession {
	if endpoint == nil {
		return nil
	}
	return endpoint.session
}

// ID returns the stable stream identifier used by the remote handler.
func (stream *CompactPeerStream) ID() uint64 {
	if stream == nil {
		return 0
	}
	return stream.streamID
}

// Call performs one ordered operation in the stream.
func (stream *CompactPeerStream) Call(ctx context.Context, command, payload []byte) (CompactFrame, error) {
	if stream == nil || stream.endpoint == nil {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	return stream.endpoint.callStream(ctx, CompactPeerStreamCall, stream.streamID, command, payload)
}

// Commit commits the remote transaction and closes this stream after a
// successful response. A failed or canceled commit leaves the stream open so
// the caller can retry or roll it back.
func (stream *CompactPeerStream) Commit(ctx context.Context) (CompactFrame, error) {
	return stream.finish(ctx, CompactPeerStreamCommit)
}

// Rollback rolls back the remote transaction and closes this stream after a
// successful response. A failed or canceled rollback leaves the stream open.
func (stream *CompactPeerStream) Rollback(ctx context.Context) (CompactFrame, error) {
	return stream.finish(ctx, CompactPeerStreamRollback)
}

// Close rolls back an open stream. It is idempotent after a successful commit,
// rollback, or earlier Close call.
func (stream *CompactPeerStream) Close(ctx context.Context) error {
	if stream == nil {
		return ErrCompactPeerStreamClosed
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := stream.endpoint.callStream(ctx, CompactPeerStreamRollback, stream.streamID, nil, nil); err != nil {
		return err
	}
	stream.closed = true
	stream.endpoint.removeOutgoing(stream)
	return nil
}

func (stream *CompactPeerStream) finish(ctx context.Context, operation CompactPeerStreamOperation) (CompactFrame, error) {
	if stream == nil || stream.endpoint == nil {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	response, err := stream.endpoint.callStream(ctx, operation, stream.streamID, nil, nil)
	if err != nil {
		return CompactFrame{}, err
	}
	stream.closed = true
	stream.endpoint.removeOutgoing(stream)
	return response, nil
}

func (endpoint *CompactPeerStreamEndpoint) handleRequest(ctx context.Context, frame CompactFrame) (CompactFrame, error) {
	if !bytes.Equal(frame.Command, compactPeerStreamCommand) {
		if endpoint.ordinary == nil {
			return CompactFrame{}, ErrCompactPeerHandlerRequired
		}
		return endpoint.ordinary(ctx, frame)
	}
	request, err := decodeCompactPeerStreamRequest(frame.Payload)
	if err != nil {
		return CompactFrame{}, err
	}
	if endpoint.streamHandler == nil {
		return CompactFrame{}, ErrCompactPeerStreamHandlerRequired
	}
	return endpoint.handleStreamRequest(ctx, request)
}

func (endpoint *CompactPeerStreamEndpoint) handleStreamRequest(ctx context.Context, request CompactPeerStreamRequest) (CompactFrame, error) {
	if request.Operation == CompactPeerStreamBegin {
		state := &compactPeerIncomingStream{}
		endpoint.mu.Lock()
		if len(endpoint.incoming) >= endpoint.maxStreams {
			endpoint.mu.Unlock()
			return CompactFrame{}, ErrCompactPeerStreamLimit
		}
		if _, exists := endpoint.incoming[request.StreamID]; exists {
			endpoint.mu.Unlock()
			return CompactFrame{}, ErrCompactPeerStreamState
		}
		endpoint.incoming[request.StreamID] = state
		endpoint.mu.Unlock()

		state.mu.Lock()
		response, err := endpoint.streamHandler(ctx, request)
		if err != nil {
			state.closed = true
			endpoint.removeIncoming(request.StreamID, state)
		}
		state.mu.Unlock()
		return response, err
	}

	endpoint.mu.Lock()
	state := endpoint.incoming[request.StreamID]
	endpoint.mu.Unlock()
	if state == nil {
		return CompactFrame{}, ErrCompactPeerStreamState
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	response, err := endpoint.streamHandler(ctx, request)
	if err == nil && (request.Operation == CompactPeerStreamCommit || request.Operation == CompactPeerStreamRollback) {
		state.closed = true
		endpoint.removeIncoming(request.StreamID, state)
	}
	return response, err
}

func (endpoint *CompactPeerStreamEndpoint) callStream(ctx context.Context, operation CompactPeerStreamOperation, streamID uint64, command, payload []byte) (CompactFrame, error) {
	if endpoint == nil || endpoint.session == nil {
		return CompactFrame{}, ErrCompactPeerStreamClosed
	}
	envelope, err := encodeCompactPeerStreamRequest(operation, streamID, command, payload, endpoint.session.protocol.maxPayloadBytes)
	if err != nil {
		return CompactFrame{}, err
	}
	return endpoint.session.Call(ctx, compactPeerStreamCommand, envelope)
}

func (endpoint *CompactPeerStreamEndpoint) nextStreamIDLocked() uint64 {
	for {
		endpoint.nextID++
		if endpoint.nextID == 0 {
			endpoint.nextID++
		}
		if _, exists := endpoint.outgoing[endpoint.nextID]; !exists {
			return endpoint.nextID
		}
	}
}

func (endpoint *CompactPeerStreamEndpoint) removeOutgoing(stream *CompactPeerStream) {
	endpoint.mu.Lock()
	if endpoint.outgoing[stream.streamID] == stream {
		delete(endpoint.outgoing, stream.streamID)
	}
	endpoint.mu.Unlock()
}

func (endpoint *CompactPeerStreamEndpoint) removeIncoming(streamID uint64, state *compactPeerIncomingStream) {
	endpoint.mu.Lock()
	if endpoint.incoming[streamID] == state {
		delete(endpoint.incoming, streamID)
	}
	endpoint.mu.Unlock()
}

func encodeCompactPeerStreamRequest(operation CompactPeerStreamOperation, streamID uint64, command, payload []byte, maxPayload int) ([]byte, error) {
	if !operation.valid() || streamID == 0 {
		return nil, ErrCompactPeerStreamProtocol
	}
	if operation == CompactPeerStreamCall {
		if len(command) == 0 {
			return nil, ErrCompactPeerStreamProtocol
		}
	} else if len(command) != 0 || len(payload) != 0 {
		return nil, ErrCompactPeerStreamProtocol
	}
	if maxPayload > 0 && (len(command) > maxPayload || len(payload) > maxPayload) {
		return nil, ErrCompactPeerStreamPayloadTooLarge
	}
	var encoded [binary.MaxVarintLen64]byte
	streamIDSize := binary.PutUvarint(encoded[:], streamID)
	commandSize := binary.PutUvarint(encoded[:], uint64(len(command)))
	payloadSize := binary.PutUvarint(encoded[:], uint64(len(payload)))
	capacity := 2 + streamIDSize + commandSize + len(command) + payloadSize + len(payload)
	if maxPayload > 0 && capacity > maxPayload {
		return nil, ErrCompactPeerStreamPayloadTooLarge
	}
	envelope := make([]byte, 0, capacity)
	envelope = append(envelope, compactPeerStreamVersion, byte(operation))
	n := binary.PutUvarint(encoded[:], streamID)
	envelope = append(envelope, encoded[:n]...)
	n = binary.PutUvarint(encoded[:], uint64(len(command)))
	envelope = append(envelope, encoded[:n]...)
	envelope = append(envelope, command...)
	n = binary.PutUvarint(encoded[:], uint64(len(payload)))
	envelope = append(envelope, encoded[:n]...)
	envelope = append(envelope, payload...)
	return envelope, nil
}

func decodeCompactPeerStreamRequest(payload []byte) (CompactPeerStreamRequest, error) {
	if len(payload) < 2 || payload[0] != compactPeerStreamVersion {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	operation := CompactPeerStreamOperation(payload[1])
	if !operation.valid() {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	offset := 2
	streamID, ok := readCompactPeerStreamUvarint(payload, &offset)
	if !ok || streamID == 0 {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	commandLength, ok := readCompactPeerStreamUvarint(payload, &offset)
	if !ok || commandLength > uint64(len(payload)-offset) {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	commandEnd := offset + int(commandLength)
	command := payload[offset:commandEnd]
	offset = commandEnd
	payloadLength, ok := readCompactPeerStreamUvarint(payload, &offset)
	if !ok || payloadLength > uint64(len(payload)-offset) {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	payloadEnd := offset + int(payloadLength)
	if payloadEnd != len(payload) {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	requestPayload := payload[offset:payloadEnd]
	if operation == CompactPeerStreamCall {
		if len(command) == 0 {
			return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
		}
	} else if len(command) != 0 || len(requestPayload) != 0 {
		return CompactPeerStreamRequest{}, ErrCompactPeerStreamProtocol
	}
	return CompactPeerStreamRequest{Operation: operation, StreamID: streamID, Command: command, Payload: requestPayload}, nil
}

func readCompactPeerStreamUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}

func (request CompactPeerStreamRequest) String() string {
	return fmt.Sprintf("%s stream=%d", request.Operation, request.StreamID)
}
