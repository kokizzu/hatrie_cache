package hatPeer

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	compactProtocolMagic0  byte = 'H'
	compactProtocolMagic1  byte = 'P'
	compactProtocolVersion byte = 1
	compactProtocolHeader  int  = 5
	compactProtocolFlags   byte = 3

	// DefaultCompactProtocolMaxFrameBytes bounds one encoded frame body.
	DefaultCompactProtocolMaxFrameBytes = 4 << 20
	// DefaultCompactProtocolMaxCommandBytes bounds the command name.
	DefaultCompactProtocolMaxCommandBytes = 256
	// DefaultCompactProtocolMaxPayloadBytes bounds one command payload.
	DefaultCompactProtocolMaxPayloadBytes = DefaultCompactProtocolMaxFrameBytes - 32
	maxCompactProtocolFrameBytes          = 64 << 20
	maxCompactProtocolCommandBytes        = 4096
	maxCompactProtocolPayloadBytes        = maxCompactProtocolFrameBytes - 32
	minCompactProtocolFrameBytes          = 8
	DefaultCompactMultiplexerMaxPending   = 1024
	maxCompactMultiplexerPending          = 1 << 20
)

var (
	ErrCompactProtocolOptionsInvalid     = errors.New("hatPeer: compact protocol options are invalid")
	ErrCompactProtocolInvalidFrame       = errors.New("hatPeer: compact protocol frame is invalid")
	ErrCompactProtocolVersionUnsupported = errors.New("hatPeer: compact protocol version is unsupported")
	ErrCompactProtocolTruncated          = errors.New("hatPeer: compact protocol frame is truncated")
	ErrCompactProtocolFrameTooLarge      = errors.New("hatPeer: compact protocol frame is too large")
	ErrCompactProtocolCommandTooLarge    = errors.New("hatPeer: compact protocol command is too large")
	ErrCompactProtocolPayloadTooLarge    = errors.New("hatPeer: compact protocol payload is too large")
	ErrCompactProtocolRequestIDInvalid   = errors.New("hatPeer: compact protocol request ID is invalid")
	ErrCompactProtocolReaderNil          = errors.New("hatPeer: compact protocol reader is nil")
	ErrCompactProtocolWriterNil          = errors.New("hatPeer: compact protocol writer is nil")
	ErrCompactMultiplexerClosed          = errors.New("hatPeer: compact multiplexer is closed")
	ErrCompactMultiplexerUnknownRequest  = errors.New("hatPeer: compact multiplexer request is unknown")
	ErrCompactMultiplexerCanceled        = errors.New("hatPeer: compact multiplexer request is canceled")
	ErrCompactMultiplexerOptionsInvalid  = errors.New("hatPeer: compact multiplexer options are invalid")
	ErrCompactMultiplexerPendingLimit    = errors.New("hatPeer: compact multiplexer pending limit reached")
)

// CompactFrameKind identifies the direction and result of one frame.
type CompactFrameKind byte

const (
	CompactRequest  CompactFrameKind = 1
	CompactResponse CompactFrameKind = 2
	CompactError    CompactFrameKind = 3
)

// CompactFrame is the compact command envelope exchanged by a peer adapter.
// RequestID correlates responses with requests; it must be non-zero.
type CompactFrame struct {
	Kind      CompactFrameKind
	RequestID uint64
	Flags     byte
	Command   []byte
	Payload   []byte
}

// CompactProtocolOptions bounds decoding before memory is allocated. Zero
// values select the documented defaults.
type CompactProtocolOptions struct {
	MaxFrameBytes   int
	MaxCommandBytes int
	MaxPayloadBytes int
}

// CompactProtocol encodes and decodes length-prefixed compact frames.
type CompactProtocol struct {
	maxFrameBytes   int
	maxCommandBytes int
	maxPayloadBytes int
}

// NewCompactProtocol creates a bounded compact protocol codec.
func NewCompactProtocol(options CompactProtocolOptions) (CompactProtocol, error) {
	maxFrameBytes := options.MaxFrameBytes
	if maxFrameBytes == 0 {
		maxFrameBytes = DefaultCompactProtocolMaxFrameBytes
	}
	maxCommandBytes := options.MaxCommandBytes
	if maxCommandBytes == 0 {
		maxCommandBytes = DefaultCompactProtocolMaxCommandBytes
	}
	maxPayloadBytes := options.MaxPayloadBytes
	if maxPayloadBytes == 0 {
		maxPayloadBytes = DefaultCompactProtocolMaxPayloadBytes
	}
	if maxFrameBytes < minCompactProtocolFrameBytes || maxFrameBytes > maxCompactProtocolFrameBytes ||
		maxCommandBytes < 1 || maxCommandBytes > maxCompactProtocolCommandBytes ||
		maxPayloadBytes < 0 || maxPayloadBytes > maxCompactProtocolPayloadBytes ||
		maxCommandBytes > maxFrameBytes-8 || maxPayloadBytes > maxFrameBytes-8 {
		return CompactProtocol{}, ErrCompactProtocolOptionsInvalid
	}
	return CompactProtocol{
		maxFrameBytes:   maxFrameBytes,
		maxCommandBytes: maxCommandBytes,
		maxPayloadBytes: maxPayloadBytes,
	}, nil
}

// Marshal returns one length-prefixed compact frame.
func (protocol CompactProtocol) Marshal(frame CompactFrame) ([]byte, error) {
	if err := protocol.validateFrame(frame); err != nil {
		return nil, err
	}
	bodyBytes := compactProtocolHeader + compactUvarintSize(frame.RequestID) + compactUvarintSize(uint64(len(frame.Command))) + len(frame.Command) + compactUvarintSize(uint64(len(frame.Payload))) + len(frame.Payload)
	if bodyBytes > protocol.maxFrameBytes {
		return nil, ErrCompactProtocolFrameTooLarge
	}
	prefixBytes := compactUvarintSize(uint64(bodyBytes))
	encoded := make([]byte, prefixBytes+bodyBytes)
	binary.PutUvarint(encoded, uint64(bodyBytes))
	offset := prefixBytes
	encoded[offset] = compactProtocolMagic0
	offset++
	encoded[offset] = compactProtocolMagic1
	offset++
	encoded[offset] = compactProtocolVersion
	offset++
	encoded[offset] = byte(frame.Kind)
	offset++
	encoded[offset] = frame.Flags
	offset++
	offset += binary.PutUvarint(encoded[offset:], frame.RequestID)
	offset += binary.PutUvarint(encoded[offset:], uint64(len(frame.Command)))
	offset += copy(encoded[offset:], frame.Command)
	offset += binary.PutUvarint(encoded[offset:], uint64(len(frame.Payload)))
	copy(encoded[offset:], frame.Payload)
	return encoded, nil
}

// Write marshals and writes one complete frame, handling short writes.
func (protocol CompactProtocol) Write(writer io.Writer, frame CompactFrame) error {
	if writer == nil {
		return ErrCompactProtocolWriterNil
	}
	encoded, err := protocol.Marshal(frame)
	if err != nil {
		return err
	}
	for len(encoded) > 0 {
		written, writeErr := writer.Write(encoded)
		if written < 0 || written > len(encoded) {
			return io.ErrShortWrite
		}
		if written > 0 {
			encoded = encoded[written:]
		}
		if writeErr != nil {
			return writeErr
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// Read reads and validates one length-prefixed compact frame.
func (protocol CompactProtocol) Read(reader io.Reader) (CompactFrame, error) {
	if reader == nil {
		return CompactFrame{}, ErrCompactProtocolReaderNil
	}
	bodyBytes, err := readCompactUvarint(reader)
	if err != nil {
		return CompactFrame{}, err
	}
	if bodyBytes < uint64(minCompactProtocolFrameBytes) {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	if bodyBytes > uint64(protocol.maxFrameBytes) {
		return CompactFrame{}, ErrCompactProtocolFrameTooLarge
	}
	body := make([]byte, int(bodyBytes))
	if _, err := io.ReadFull(reader, body); err != nil {
		return CompactFrame{}, ErrCompactProtocolTruncated
	}
	return protocol.decodeBody(body)
}

func (protocol CompactProtocol) validateFrame(frame CompactFrame) error {
	if frame.RequestID == 0 {
		return ErrCompactProtocolRequestIDInvalid
	}
	if frame.Flags&^compactProtocolFlags != 0 || !validCompactFrameKind(frame.Kind) {
		return ErrCompactProtocolInvalidFrame
	}
	if frame.Kind == CompactRequest && len(frame.Command) == 0 {
		return ErrCompactProtocolInvalidFrame
	}
	if len(frame.Command) > protocol.maxCommandBytes {
		return ErrCompactProtocolCommandTooLarge
	}
	if len(frame.Payload) > protocol.maxPayloadBytes {
		return ErrCompactProtocolPayloadTooLarge
	}
	return nil
}

func (protocol CompactProtocol) decodeBody(body []byte) (CompactFrame, error) {
	if len(body) < compactProtocolHeader {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	if body[0] != compactProtocolMagic0 || body[1] != compactProtocolMagic1 {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	if body[2] != compactProtocolVersion {
		return CompactFrame{}, ErrCompactProtocolVersionUnsupported
	}
	kind := CompactFrameKind(body[3])
	if !validCompactFrameKind(kind) {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	flags := body[4]
	if flags&^compactProtocolFlags != 0 {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	offset := compactProtocolHeader
	requestID, err := readCompactBodyUvarint(body, &offset)
	if err != nil || requestID == 0 {
		return CompactFrame{}, ErrCompactProtocolRequestIDInvalid
	}
	commandBytes, err := readCompactBodyUvarint(body, &offset)
	if err != nil {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	if commandBytes > uint64(protocol.maxCommandBytes) {
		return CompactFrame{}, ErrCompactProtocolCommandTooLarge
	}
	commandEnd := offset + int(commandBytes)
	if commandEnd < offset || commandEnd > len(body) {
		return CompactFrame{}, ErrCompactProtocolTruncated
	}
	command := body[offset:commandEnd]
	offset = commandEnd
	payloadBytes, err := readCompactBodyUvarint(body, &offset)
	if err != nil {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	if payloadBytes > uint64(protocol.maxPayloadBytes) {
		return CompactFrame{}, ErrCompactProtocolPayloadTooLarge
	}
	payloadEnd := offset + int(payloadBytes)
	if payloadEnd < offset || payloadEnd > len(body) {
		return CompactFrame{}, ErrCompactProtocolTruncated
	}
	if payloadEnd != len(body) || kind == CompactRequest && len(command) == 0 {
		return CompactFrame{}, ErrCompactProtocolInvalidFrame
	}
	return CompactFrame{
		Kind:      kind,
		RequestID: requestID,
		Flags:     flags,
		Command:   command,
		Payload:   body[offset:payloadEnd],
	}, nil
}

func validCompactFrameKind(kind CompactFrameKind) bool {
	return kind == CompactRequest || kind == CompactResponse || kind == CompactError
}

func compactUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func readCompactUvarint(reader io.Reader) (uint64, error) {
	var encoded [binary.MaxVarintLen64]byte
	for i := range encoded {
		if _, err := io.ReadFull(reader, encoded[i:i+1]); err != nil {
			return 0, ErrCompactProtocolTruncated
		}
		if encoded[i]&0x80 == 0 {
			value, used := binary.Uvarint(encoded[:i+1])
			if used != i+1 || compactUvarintSize(value) != used {
				return 0, ErrCompactProtocolInvalidFrame
			}
			return value, nil
		}
	}
	return 0, ErrCompactProtocolInvalidFrame
}

func readCompactBodyUvarint(body []byte, offset *int) (uint64, error) {
	if *offset >= len(body) {
		return 0, ErrCompactProtocolTruncated
	}
	value, used := binary.Uvarint(body[*offset:])
	if used <= 0 || compactUvarintSize(value) != used {
		return 0, ErrCompactProtocolInvalidFrame
	}
	*offset += used
	return value, nil
}

// CompactMultiplexer allocates correlation IDs and routes responses that may
// arrive in a different order from their requests. It does not own a socket;
// callers write returned frames through CompactProtocol and pass decoded
// response frames to Resolve.
type CompactMultiplexer struct {
	mu         sync.Mutex
	nextID     uint64
	closed     bool
	maxPending int
	pending    map[uint64]*CompactPendingResponse
}

// CompactMultiplexerOptions bounds the number of outstanding requests kept in
// memory for one peer connection. Zero selects the default of 1024.
type CompactMultiplexerOptions struct {
	MaxPending int
}

type compactPendingResult struct {
	frame CompactFrame
	err   error
}

// CompactPendingResponse represents one request waiting for a response.
type CompactPendingResponse struct {
	id          uint64
	done        chan struct{}
	complete    sync.Once
	result      compactPendingResult
	multiplexer *CompactMultiplexer
}

// NewCompactMultiplexer creates an empty response dispatcher with the default
// pending-request bound.
func NewCompactMultiplexer() *CompactMultiplexer {
	multiplexer, _ := NewCompactMultiplexerWithOptions(CompactMultiplexerOptions{})
	return multiplexer
}

// NewCompactMultiplexerWithOptions creates an empty bounded response
// dispatcher.
func NewCompactMultiplexerWithOptions(options CompactMultiplexerOptions) (*CompactMultiplexer, error) {
	maxPending := options.MaxPending
	if maxPending == 0 {
		maxPending = DefaultCompactMultiplexerMaxPending
	}
	if maxPending < 1 || maxPending > maxCompactMultiplexerPending {
		return nil, ErrCompactMultiplexerOptionsInvalid
	}
	return &CompactMultiplexer{
		nextID:     1,
		maxPending: maxPending,
		pending:    make(map[uint64]*CompactPendingResponse),
	}, nil
}

// Request creates a request frame and registers its response waiter.
func (multiplexer *CompactMultiplexer) Request(command, payload []byte) (CompactFrame, *CompactPendingResponse, error) {
	if multiplexer == nil {
		return CompactFrame{}, nil, ErrCompactMultiplexerClosed
	}
	if len(command) == 0 {
		return CompactFrame{}, nil, ErrCompactProtocolInvalidFrame
	}
	multiplexer.mu.Lock()
	defer multiplexer.mu.Unlock()
	if multiplexer.closed {
		return CompactFrame{}, nil, ErrCompactMultiplexerClosed
	}
	if len(multiplexer.pending) >= multiplexer.maxPending {
		return CompactFrame{}, nil, ErrCompactMultiplexerPendingLimit
	}
	requestID := multiplexer.nextRequestIDLocked()
	pending := &CompactPendingResponse{
		id:          requestID,
		done:        make(chan struct{}),
		multiplexer: multiplexer,
	}
	multiplexer.pending[requestID] = pending
	return CompactFrame{Kind: CompactRequest, RequestID: requestID, Command: command, Payload: payload}, pending, nil
}

// Resolve delivers one response or error frame to the matching request.
func (multiplexer *CompactMultiplexer) Resolve(frame CompactFrame) error {
	if multiplexer == nil {
		return ErrCompactMultiplexerClosed
	}
	if frame.RequestID == 0 || (frame.Kind != CompactResponse && frame.Kind != CompactError) {
		return ErrCompactProtocolInvalidFrame
	}
	multiplexer.mu.Lock()
	pending := multiplexer.pending[frame.RequestID]
	if pending != nil {
		delete(multiplexer.pending, frame.RequestID)
	}
	multiplexer.mu.Unlock()
	if pending == nil {
		return ErrCompactMultiplexerUnknownRequest
	}
	pending.finish(frame, nil)
	return nil
}

// Cancel removes one pending request. It returns false when the request was
// already resolved, canceled, or never registered.
func (multiplexer *CompactMultiplexer) Cancel(requestID uint64) bool {
	if multiplexer == nil || requestID == 0 {
		return false
	}
	multiplexer.mu.Lock()
	pending := multiplexer.pending[requestID]
	if pending != nil {
		delete(multiplexer.pending, requestID)
	}
	multiplexer.mu.Unlock()
	if pending == nil {
		return false
	}
	pending.finish(CompactFrame{}, ErrCompactMultiplexerCanceled)
	return true
}

// Close completes all pending requests with err and rejects new requests.
// Passing nil uses ErrCompactMultiplexerClosed.
func (multiplexer *CompactMultiplexer) Close(err error) {
	if multiplexer == nil {
		return
	}
	if err == nil {
		err = ErrCompactMultiplexerClosed
	}
	multiplexer.mu.Lock()
	if multiplexer.closed {
		multiplexer.mu.Unlock()
		return
	}
	multiplexer.closed = true
	pending := make([]*CompactPendingResponse, 0, len(multiplexer.pending))
	for requestID, request := range multiplexer.pending {
		delete(multiplexer.pending, requestID)
		pending = append(pending, request)
	}
	multiplexer.mu.Unlock()
	for _, request := range pending {
		request.finish(CompactFrame{}, err)
	}
}

// ID returns the correlation ID associated with the pending response.
func (pending *CompactPendingResponse) ID() uint64 {
	if pending == nil {
		return 0
	}
	return pending.id
}

// Wait waits for the response or cancels the request when ctx expires.
func (pending *CompactPendingResponse) Wait(ctx context.Context) (CompactFrame, error) {
	if pending == nil {
		return CompactFrame{}, ErrCompactMultiplexerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-pending.done:
		return pending.result.frame, pending.result.err
	case <-ctx.Done():
		pending.multiplexer.Cancel(pending.id)
		return CompactFrame{}, ctx.Err()
	}
}

func (pending *CompactPendingResponse) finish(frame CompactFrame, err error) {
	pending.complete.Do(func() {
		pending.result = compactPendingResult{frame: frame, err: err}
		close(pending.done)
	})
}

func (multiplexer *CompactMultiplexer) nextRequestIDLocked() uint64 {
	for {
		requestID := multiplexer.nextID
		multiplexer.nextID++
		if requestID == 0 {
			continue
		}
		if _, exists := multiplexer.pending[requestID]; !exists {
			return requestID
		}
	}
}
