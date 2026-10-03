package hatPeer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	compactPeerConfigWatchVersion byte = 1

	compactPeerConfigWatchOpenCommand  = "_hat.peer.config.watch.open.v1"
	compactPeerConfigWatchPollCommand  = "_hat.peer.config.watch.poll.v1"
	compactPeerConfigWatchCloseCommand = "_hat.peer.config.watch.close.v1"

	// DefaultCompactPeerConfigWatchMaxHistory bounds retained replay events.
	DefaultCompactPeerConfigWatchMaxHistory = 256
	// DefaultCompactPeerConfigWatchMaxEvents bounds one poll response.
	DefaultCompactPeerConfigWatchMaxEvents = 32
	// DefaultCompactPeerConfigWatchMaxPrefixBytes bounds one watched prefix.
	DefaultCompactPeerConfigWatchMaxPrefixBytes = 256
	// DefaultCompactPeerConfigWatchMaxPathBytes bounds one event path.
	DefaultCompactPeerConfigWatchMaxPathBytes = 1024
	// DefaultCompactPeerConfigWatchMaxValueBytes bounds one event value.
	DefaultCompactPeerConfigWatchMaxValueBytes = 64 << 10

	maxCompactPeerConfigWatchHistory     = 1 << 20
	maxCompactPeerConfigWatchEvents      = 1024
	maxCompactPeerConfigWatchPrefixBytes = 4096
	maxCompactPeerConfigWatchPathBytes   = 64 << 10
	maxCompactPeerConfigWatchValueBytes  = DefaultCompactProtocolMaxPayloadBytes - 128
)

var (
	// ErrCompactPeerConfigWatchOptionsInvalid indicates invalid watch bounds.
	ErrCompactPeerConfigWatchOptionsInvalid = errors.New("hatPeer: compact peer config watch options are invalid")
	// ErrCompactPeerConfigWatchPrefixTooLarge indicates an oversized prefix.
	ErrCompactPeerConfigWatchPrefixTooLarge = errors.New("hatPeer: compact peer config watch prefix is too large")
	// ErrCompactPeerConfigWatchPathInvalid indicates an empty event path.
	ErrCompactPeerConfigWatchPathInvalid = errors.New("hatPeer: compact peer config watch path is invalid")
	// ErrCompactPeerConfigWatchPathTooLarge indicates an oversized event path.
	ErrCompactPeerConfigWatchPathTooLarge = errors.New("hatPeer: compact peer config watch path is too large")
	// ErrCompactPeerConfigWatchValueTooLarge indicates an oversized event value.
	ErrCompactPeerConfigWatchValueTooLarge = errors.New("hatPeer: compact peer config watch value is too large")
	// ErrCompactPeerConfigWatchHistoryGap indicates that replay has been evicted.
	ErrCompactPeerConfigWatchHistoryGap = errors.New("hatPeer: compact peer config watch history gap")
	// ErrCompactPeerConfigWatchRevisionInvalid indicates an invalid replay cursor.
	ErrCompactPeerConfigWatchRevisionInvalid = errors.New("hatPeer: compact peer config watch revision is invalid")
	// ErrCompactPeerConfigWatchPayloadInvalid indicates malformed watch payload.
	ErrCompactPeerConfigWatchPayloadInvalid = errors.New("hatPeer: compact peer config watch payload is invalid")
	// ErrCompactPeerConfigWatchPayloadTooLarge indicates a response beyond the wire bound.
	ErrCompactPeerConfigWatchPayloadTooLarge = errors.New("hatPeer: compact peer config watch payload is too large")
	// ErrCompactPeerConfigWatchCommand indicates an unsupported watch command.
	ErrCompactPeerConfigWatchCommand = errors.New("hatPeer: compact peer config watch command is unsupported")
	// ErrCompactPeerConfigWatchSessionRequired indicates a missing client session.
	ErrCompactPeerConfigWatchSessionRequired = errors.New("hatPeer: compact peer config watch session is required")
	// ErrCompactPeerConfigWatchNotOpen indicates that polling started before Open.
	ErrCompactPeerConfigWatchNotOpen = errors.New("hatPeer: compact peer config watch is not open")
	// ErrCompactPeerConfigWatchClosed indicates that the client was closed.
	ErrCompactPeerConfigWatchClosed = errors.New("hatPeer: compact peer config watch client is closed")
)

// CompactPeerConfigWatchEvent is one revisioned remote configuration change.
type CompactPeerConfigWatchEvent struct {
	Revision uint64
	Path     string
	Value    []byte
}

// CompactPeerConfigWatchSnapshot describes the server cursor and retained
// replay window at open time.
type CompactPeerConfigWatchSnapshot struct {
	Revision       uint64
	OldestRevision uint64
}

// CompactPeerConfigWatchOptions bounds a server watch registry. Zero values
// select bounded defaults.
type CompactPeerConfigWatchOptions struct {
	MaxHistory     int
	MaxEvents      int
	MaxPrefixBytes int
	MaxPathBytes   int
	MaxValueBytes  int
}

// CompactPeerConfigWatchClientOptions configures one polling watch client.
// The client repeats the same revision cursor after reconnecting, so retained
// history can be replayed without keeping connection-local server state.
type CompactPeerConfigWatchClientOptions struct {
	Prefix         string
	MaxEvents      int
	MaxPrefixBytes int
	MaxPathBytes   int
	MaxValueBytes  int
}

type compactPeerConfigWatchLimits struct {
	maxHistory     int
	maxEvents      int
	maxPrefixBytes int
	maxPathBytes   int
	maxValueBytes  int
}

// CompactPeerConfigWatchServer retains a bounded revision history and serves
// open, poll, and close commands through a CompactPeerSession handler. It is
// deliberately stateless per connection; the client revision is the durable
// reconnect cursor.
type CompactPeerConfigWatchServer struct {
	mu       sync.Mutex
	limits   compactPeerConfigWatchLimits
	revision uint64
	history  []CompactPeerConfigWatchEvent
}

// CompactPeerConfigWatchClient polls a remote watch server and keeps the last
// consumed revision locally for reconnect and replay.
type CompactPeerConfigWatchClient struct {
	mu        sync.Mutex
	limits    compactPeerConfigWatchLimits
	prefix    string
	maxEvents int
	session   *CompactPeerSession
	revision  uint64
	opened    bool
	closed    bool
}

// NewCompactPeerConfigWatchServer creates a bounded remote watch registry.
func NewCompactPeerConfigWatchServer(options CompactPeerConfigWatchOptions) (*CompactPeerConfigWatchServer, error) {
	limits, err := normalizeCompactPeerConfigWatchOptions(options)
	if err != nil {
		return nil, err
	}
	return &CompactPeerConfigWatchServer{limits: limits, history: make([]CompactPeerConfigWatchEvent, 0, minInt(limits.maxHistory, 64))}, nil
}

// NewCompactPeerConfigWatchClient creates a client for one path prefix.
func NewCompactPeerConfigWatchClient(options CompactPeerConfigWatchClientOptions) (*CompactPeerConfigWatchClient, error) {
	limits, err := normalizeCompactPeerConfigWatchOptions(CompactPeerConfigWatchOptions{
		MaxEvents:      options.MaxEvents,
		MaxPrefixBytes: options.MaxPrefixBytes,
		MaxPathBytes:   options.MaxPathBytes,
		MaxValueBytes:  options.MaxValueBytes,
	})
	if err != nil {
		return nil, err
	}
	if err := validateCompactPeerConfigWatchPrefix(options.Prefix, limits); err != nil {
		return nil, err
	}
	return &CompactPeerConfigWatchClient{
		limits:    limits,
		prefix:    options.Prefix,
		maxEvents: limits.maxEvents,
	}, nil
}

// Publish appends one configuration change and returns its revision. Values
// are copied before the method returns, so callers may reuse their buffers.
func (server *CompactPeerConfigWatchServer) Publish(path string, value []byte) (uint64, error) {
	if server == nil {
		return 0, ErrCompactPeerConfigWatchOptionsInvalid
	}
	if err := validateCompactPeerConfigWatchPath(path, server.limits); err != nil {
		return 0, err
	}
	if len(value) > server.limits.maxValueBytes {
		return 0, ErrCompactPeerConfigWatchValueTooLarge
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.revision == ^uint64(0) {
		return 0, ErrCompactPeerConfigWatchRevisionInvalid
	}
	server.revision++
	server.history = append(server.history, CompactPeerConfigWatchEvent{
		Revision: server.revision,
		Path:     path,
		Value:    append([]byte(nil), value...),
	})
	if excess := len(server.history) - server.limits.maxHistory; excess > 0 {
		remaining := len(server.history) - excess
		copy(server.history, server.history[excess:])
		for index := remaining; index < len(server.history); index++ {
			server.history[index] = CompactPeerConfigWatchEvent{}
		}
		server.history = server.history[:remaining]
	}
	return server.revision, nil
}

// Snapshot returns the current bounded replay window.
func (server *CompactPeerConfigWatchServer) Snapshot() CompactPeerConfigWatchSnapshot {
	if server == nil {
		return CompactPeerConfigWatchSnapshot{}
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	return server.snapshotLocked()
}

// Handler returns a compact-session handler for the watch commands. Unknown
// commands are delegated to next when it is non-nil.
func (server *CompactPeerConfigWatchServer) Handler(next CompactPeerHandler) CompactPeerHandler {
	return func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
		if err := ctx.Err(); err != nil {
			return CompactFrame{}, err
		}
		if server == nil {
			return CompactFrame{}, ErrCompactPeerConfigWatchOptionsInvalid
		}
		switch string(request.Command) {
		case compactPeerConfigWatchOpenCommand:
			return server.handleOpen(request.Payload)
		case compactPeerConfigWatchPollCommand:
			return server.handlePoll(request.Payload)
		case compactPeerConfigWatchCloseCommand:
			return handleCompactPeerConfigWatchClose(request.Payload)
		default:
			if next != nil {
				return next(ctx, request)
			}
			return CompactFrame{}, fmt.Errorf("%w: %s", ErrCompactPeerConfigWatchCommand, request.Command)
		}
	}
}

// Open binds the client to a session and returns the server replay window.
// Existing events are not consumed by Open; Poll starts at the client's last
// revision, including revision zero for a new client.
func (client *CompactPeerConfigWatchClient) Open(ctx context.Context, session *CompactPeerSession) (CompactPeerConfigWatchSnapshot, error) {
	if client == nil {
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchClosed
	}
	if session == nil {
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchSessionRequired
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchClosed
	}
	payload, err := marshalCompactPeerConfigWatchRequest(client.prefix, client.revision, client.maxEvents, client.limits)
	if err != nil {
		return CompactPeerConfigWatchSnapshot{}, err
	}
	response, err := session.Call(ctx, []byte(compactPeerConfigWatchOpenCommand), payload)
	if err != nil {
		return CompactPeerConfigWatchSnapshot{}, err
	}
	state, err := unmarshalCompactPeerConfigWatchSnapshot(response.Payload)
	if err != nil {
		return CompactPeerConfigWatchSnapshot{}, err
	}
	if state.Revision < client.revision {
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchRevisionInvalid
	}
	client.session = session
	client.opened = true
	return state, nil
}

// Reconnect opens the same watch on a new compact peer session.
func (client *CompactPeerConfigWatchClient) Reconnect(ctx context.Context, session *CompactPeerSession) (CompactPeerConfigWatchSnapshot, error) {
	return client.Open(ctx, session)
}

// Poll returns up to the configured number of events after the last consumed
// revision. Non-matching revisions still advance the cursor when the server
// has fully scanned the retained history, avoiding repeated prefix scans.
func (client *CompactPeerConfigWatchClient) Poll(ctx context.Context) ([]CompactPeerConfigWatchEvent, error) {
	if client == nil {
		return nil, ErrCompactPeerConfigWatchClosed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil, ErrCompactPeerConfigWatchClosed
	}
	if !client.opened || client.session == nil {
		return nil, ErrCompactPeerConfigWatchNotOpen
	}
	payload, err := marshalCompactPeerConfigWatchRequest(client.prefix, client.revision, client.maxEvents, client.limits)
	if err != nil {
		return nil, err
	}
	response, err := client.session.Call(ctx, []byte(compactPeerConfigWatchPollCommand), payload)
	if err != nil {
		return nil, err
	}
	latest, events, err := unmarshalCompactPeerConfigWatchEvents(response.Payload, CompactPeerConfigWatchOptions{
		MaxEvents:      client.limits.maxEvents,
		MaxPrefixBytes: client.limits.maxPrefixBytes,
		MaxPathBytes:   client.limits.maxPathBytes,
		MaxValueBytes:  client.limits.maxValueBytes,
	})
	if err != nil {
		return nil, err
	}
	if latest < client.revision {
		return nil, ErrCompactPeerConfigWatchRevisionInvalid
	}
	client.revision = latest
	return events, nil
}

// Close stops the client and sends a best-effort stateless close command.
func (client *CompactPeerConfigWatchClient) Close(ctx context.Context) error {
	if client == nil {
		return nil
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil
	}
	session := client.session
	client.closed = true
	client.opened = false
	client.session = nil
	client.mu.Unlock()
	if session == nil {
		return nil
	}
	_, err := session.Call(ctx, []byte(compactPeerConfigWatchCloseCommand), []byte{compactPeerConfigWatchVersion})
	if errors.Is(err, ErrCompactPeerClosed) || errors.Is(err, ErrCompactMultiplexerClosed) {
		return nil
	}
	return err
}

// Revision returns the last cursor consumed by Poll.
func (client *CompactPeerConfigWatchClient) Revision() uint64 {
	if client == nil {
		return 0
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.revision
}

func (server *CompactPeerConfigWatchServer) handleOpen(payload []byte) (CompactFrame, error) {
	request, err := unmarshalCompactPeerConfigWatchRequest(payload, server.limits)
	if err != nil {
		return CompactFrame{}, err
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if err := server.validateRevisionLocked(request.afterRevision); err != nil {
		return CompactFrame{}, err
	}
	encoded, err := marshalCompactPeerConfigWatchSnapshot(server.snapshotLocked())
	if err != nil {
		return CompactFrame{}, err
	}
	return CompactFrame{Payload: encoded}, nil
}

func (server *CompactPeerConfigWatchServer) handlePoll(payload []byte) (CompactFrame, error) {
	request, err := unmarshalCompactPeerConfigWatchRequest(payload, server.limits)
	if err != nil {
		return CompactFrame{}, err
	}
	server.mu.Lock()
	if err := server.validateRevisionLocked(request.afterRevision); err != nil {
		server.mu.Unlock()
		return CompactFrame{}, err
	}
	cursor, events := server.eventsAfterLocked(request.prefix, request.afterRevision, request.maxEvents)
	server.mu.Unlock()
	encoded, err := marshalCompactPeerConfigWatchEvents(cursor, events, server.limits)
	if err != nil {
		return CompactFrame{}, err
	}
	return CompactFrame{Payload: encoded}, nil
}

func handleCompactPeerConfigWatchClose(payload []byte) (CompactFrame, error) {
	if len(payload) != 1 || payload[0] != compactPeerConfigWatchVersion {
		return CompactFrame{}, ErrCompactPeerConfigWatchPayloadInvalid
	}
	return CompactFrame{Payload: []byte{compactPeerConfigWatchVersion}}, nil
}

func (server *CompactPeerConfigWatchServer) snapshotLocked() CompactPeerConfigWatchSnapshot {
	state := CompactPeerConfigWatchSnapshot{Revision: server.revision}
	if len(server.history) > 0 {
		state.OldestRevision = server.history[0].Revision
	}
	return state
}

func (server *CompactPeerConfigWatchServer) validateRevisionLocked(after uint64) error {
	if after > server.revision {
		return ErrCompactPeerConfigWatchRevisionInvalid
	}
	if len(server.history) > 0 && server.history[0].Revision > 0 && after < server.history[0].Revision-1 {
		return fmt.Errorf("%w: after=%d oldest=%d", ErrCompactPeerConfigWatchHistoryGap, after, server.history[0].Revision)
	}
	return nil
}

func (server *CompactPeerConfigWatchServer) eventsAfterLocked(prefix string, after uint64, maxEvents int) (uint64, []CompactPeerConfigWatchEvent) {
	cursor := after
	events := make([]CompactPeerConfigWatchEvent, 0, minInt(maxEvents, 8))
	for _, event := range server.history {
		if event.Revision <= after {
			continue
		}
		if len(events) >= maxEvents {
			break
		}
		cursor = event.Revision
		if !strings.HasPrefix(event.Path, prefix) {
			continue
		}
		events = append(events, CompactPeerConfigWatchEvent{
			Revision: event.Revision,
			Path:     event.Path,
			Value:    append([]byte(nil), event.Value...),
		})
	}
	if len(events) < maxEvents {
		cursor = server.revision
	}
	return cursor, events
}

func normalizeCompactPeerConfigWatchOptions(options CompactPeerConfigWatchOptions) (compactPeerConfigWatchLimits, error) {
	limits := compactPeerConfigWatchLimits{
		maxHistory:     options.MaxHistory,
		maxEvents:      options.MaxEvents,
		maxPrefixBytes: options.MaxPrefixBytes,
		maxPathBytes:   options.MaxPathBytes,
		maxValueBytes:  options.MaxValueBytes,
	}
	if limits.maxHistory == 0 {
		limits.maxHistory = DefaultCompactPeerConfigWatchMaxHistory
	}
	if limits.maxEvents == 0 {
		limits.maxEvents = DefaultCompactPeerConfigWatchMaxEvents
	}
	if limits.maxPrefixBytes == 0 {
		limits.maxPrefixBytes = DefaultCompactPeerConfigWatchMaxPrefixBytes
	}
	if limits.maxPathBytes == 0 {
		limits.maxPathBytes = DefaultCompactPeerConfigWatchMaxPathBytes
	}
	if limits.maxValueBytes == 0 {
		limits.maxValueBytes = DefaultCompactPeerConfigWatchMaxValueBytes
	}
	if limits.maxHistory < 1 || limits.maxHistory > maxCompactPeerConfigWatchHistory ||
		limits.maxEvents < 1 || limits.maxEvents > maxCompactPeerConfigWatchEvents ||
		limits.maxPrefixBytes < 0 || limits.maxPrefixBytes > maxCompactPeerConfigWatchPrefixBytes ||
		limits.maxPathBytes < 1 || limits.maxPathBytes > maxCompactPeerConfigWatchPathBytes ||
		limits.maxValueBytes < 0 || limits.maxValueBytes > maxCompactPeerConfigWatchValueBytes {
		return compactPeerConfigWatchLimits{}, ErrCompactPeerConfigWatchOptionsInvalid
	}
	return limits, nil
}

func validateCompactPeerConfigWatchPrefix(prefix string, limits compactPeerConfigWatchLimits) error {
	if len(prefix) > limits.maxPrefixBytes {
		return ErrCompactPeerConfigWatchPrefixTooLarge
	}
	return nil
}

func validateCompactPeerConfigWatchPath(path string, limits compactPeerConfigWatchLimits) error {
	if path == "" {
		return ErrCompactPeerConfigWatchPathInvalid
	}
	if len(path) > limits.maxPathBytes {
		return ErrCompactPeerConfigWatchPathTooLarge
	}
	return nil
}

type compactPeerConfigWatchRequest struct {
	prefix        string
	afterRevision uint64
	maxEvents     int
}

func marshalCompactPeerConfigWatchRequest(prefix string, afterRevision uint64, maxEvents int, limits compactPeerConfigWatchLimits) ([]byte, error) {
	if err := validateCompactPeerConfigWatchPrefix(prefix, limits); err != nil {
		return nil, err
	}
	if maxEvents < 1 || maxEvents > limits.maxEvents {
		return nil, ErrCompactPeerConfigWatchOptionsInvalid
	}
	payload := make([]byte, 0, 1+len(prefix)+30)
	payload = append(payload, compactPeerConfigWatchVersion)
	payload = appendCompactPeerConfigWatchUvarint(payload, uint64(len(prefix)))
	payload = append(payload, prefix...)
	payload = appendCompactPeerConfigWatchUvarint(payload, afterRevision)
	payload = appendCompactPeerConfigWatchUvarint(payload, uint64(maxEvents))
	return payload, nil
}

func unmarshalCompactPeerConfigWatchRequest(payload []byte, limits compactPeerConfigWatchLimits) (compactPeerConfigWatchRequest, error) {
	if len(payload) < 1 || payload[0] != compactPeerConfigWatchVersion {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchPayloadInvalid
	}
	offset := 1
	prefixBytes, next, err := consumeCompactPeerConfigWatchUvarint(payload, offset)
	if err != nil {
		return compactPeerConfigWatchRequest{}, err
	}
	offset = next
	prefix, offset, err := consumeCompactPeerConfigWatchBytes(payload, offset, prefixBytes, limits.maxPrefixBytes, ErrCompactPeerConfigWatchPrefixTooLarge)
	if err != nil {
		return compactPeerConfigWatchRequest{}, err
	}
	afterRevision, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, offset)
	if err != nil {
		return compactPeerConfigWatchRequest{}, err
	}
	maxEvents, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, offset)
	if err != nil || maxEvents < 1 || offset != len(payload) {
		if err != nil {
			return compactPeerConfigWatchRequest{}, err
		}
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchPayloadInvalid
	}
	if maxEvents > uint64(limits.maxEvents) {
		maxEvents = uint64(limits.maxEvents)
	}
	return compactPeerConfigWatchRequest{prefix: string(prefix), afterRevision: afterRevision, maxEvents: int(maxEvents)}, nil
}

func marshalCompactPeerConfigWatchSnapshot(snapshot CompactPeerConfigWatchSnapshot) ([]byte, error) {
	if snapshot.OldestRevision > snapshot.Revision && snapshot.OldestRevision != 0 {
		return nil, ErrCompactPeerConfigWatchRevisionInvalid
	}
	payload := make([]byte, 0, 21)
	payload = append(payload, compactPeerConfigWatchVersion)
	payload = appendCompactPeerConfigWatchUvarint(payload, snapshot.Revision)
	payload = appendCompactPeerConfigWatchUvarint(payload, snapshot.OldestRevision)
	return payload, nil
}

func unmarshalCompactPeerConfigWatchSnapshot(payload []byte) (CompactPeerConfigWatchSnapshot, error) {
	if len(payload) < 1 || payload[0] != compactPeerConfigWatchVersion {
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchPayloadInvalid
	}
	revision, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, 1)
	if err != nil {
		return CompactPeerConfigWatchSnapshot{}, err
	}
	oldest, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, offset)
	if err != nil || offset != len(payload) || oldest > revision && oldest != 0 {
		if err != nil {
			return CompactPeerConfigWatchSnapshot{}, err
		}
		return CompactPeerConfigWatchSnapshot{}, ErrCompactPeerConfigWatchPayloadInvalid
	}
	return CompactPeerConfigWatchSnapshot{Revision: revision, OldestRevision: oldest}, nil
}

func marshalCompactPeerConfigWatchEvents(latestRevision uint64, events []CompactPeerConfigWatchEvent, options ...compactPeerConfigWatchLimits) ([]byte, error) {
	limits, err := compactPeerConfigWatchLimitsFromOptional(options)
	if err != nil {
		return nil, err
	}
	if len(events) > limits.maxEvents {
		return nil, ErrCompactPeerConfigWatchPayloadTooLarge
	}
	payloadBytes := 1 + compactUvarintSize(latestRevision) + compactUvarintSize(uint64(len(events)))
	for _, event := range events {
		if event.Revision == 0 || event.Revision > latestRevision {
			return nil, ErrCompactPeerConfigWatchRevisionInvalid
		}
		if err := validateCompactPeerConfigWatchPath(event.Path, limits); err != nil {
			return nil, err
		}
		if len(event.Value) > limits.maxValueBytes {
			return nil, ErrCompactPeerConfigWatchValueTooLarge
		}
		eventBytes := compactUvarintSize(event.Revision) + compactUvarintSize(uint64(len(event.Path))) + len(event.Path) + compactUvarintSize(uint64(len(event.Value))) + len(event.Value)
		if eventBytes > DefaultCompactProtocolMaxPayloadBytes-payloadBytes {
			return nil, ErrCompactPeerConfigWatchPayloadTooLarge
		}
		payloadBytes += eventBytes
	}
	payload := make([]byte, 0, payloadBytes)
	payload = append(payload, compactPeerConfigWatchVersion)
	payload = appendCompactPeerConfigWatchUvarint(payload, latestRevision)
	payload = appendCompactPeerConfigWatchUvarint(payload, uint64(len(events)))
	for _, event := range events {
		payload = appendCompactPeerConfigWatchUvarint(payload, event.Revision)
		payload = appendCompactPeerConfigWatchUvarint(payload, uint64(len(event.Path)))
		payload = append(payload, event.Path...)
		payload = appendCompactPeerConfigWatchUvarint(payload, uint64(len(event.Value)))
		payload = append(payload, event.Value...)
	}
	return payload, nil
}

func unmarshalCompactPeerConfigWatchEvents(payload []byte, options CompactPeerConfigWatchOptions) (uint64, []CompactPeerConfigWatchEvent, error) {
	limits, err := normalizeCompactPeerConfigWatchOptions(options)
	if err != nil {
		return 0, nil, err
	}
	if len(payload) < 1 || payload[0] != compactPeerConfigWatchVersion {
		return 0, nil, ErrCompactPeerConfigWatchPayloadInvalid
	}
	latest, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, 1)
	if err != nil {
		return 0, nil, err
	}
	count, offset, err := consumeCompactPeerConfigWatchUvarintAt(payload, offset)
	if err != nil || count > uint64(limits.maxEvents) {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, ErrCompactPeerConfigWatchPayloadTooLarge
	}
	events := make([]CompactPeerConfigWatchEvent, 0, int(count))
	for index := uint64(0); index < count; index++ {
		revision, next, err := consumeCompactPeerConfigWatchUvarint(payload, offset)
		if err != nil {
			return 0, nil, err
		}
		offset = next
		pathBytes, next, err := consumeCompactPeerConfigWatchUvarint(payload, offset)
		if err != nil {
			return 0, nil, err
		}
		offset = next
		path, nextOffset, err := consumeCompactPeerConfigWatchBytes(payload, offset, pathBytes, limits.maxPathBytes, ErrCompactPeerConfigWatchPathTooLarge)
		if err != nil {
			return 0, nil, err
		}
		offset = nextOffset
		valueBytes, next, err := consumeCompactPeerConfigWatchUvarint(payload, offset)
		if err != nil {
			return 0, nil, err
		}
		offset = next
		value, nextOffset, err := consumeCompactPeerConfigWatchBytes(payload, offset, valueBytes, limits.maxValueBytes, ErrCompactPeerConfigWatchValueTooLarge)
		if err != nil {
			return 0, nil, err
		}
		offset = nextOffset
		if revision == 0 || revision > latest {
			return 0, nil, ErrCompactPeerConfigWatchRevisionInvalid
		}
		events = append(events, CompactPeerConfigWatchEvent{Revision: revision, Path: string(path), Value: append([]byte(nil), value...)})
	}
	if offset != len(payload) {
		return 0, nil, fmt.Errorf("%w: trailing bytes offset=%d length=%d", ErrCompactPeerConfigWatchPayloadInvalid, offset, len(payload))
	}
	return latest, events, nil
}

func compactPeerConfigWatchLimitsFromOptional(options []compactPeerConfigWatchLimits) (compactPeerConfigWatchLimits, error) {
	if len(options) > 1 {
		return compactPeerConfigWatchLimits{}, ErrCompactPeerConfigWatchOptionsInvalid
	}
	if len(options) == 1 {
		return options[0], nil
	}
	return normalizeCompactPeerConfigWatchOptions(CompactPeerConfigWatchOptions{})
}

func appendCompactPeerConfigWatchUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:count]...)
}

func consumeCompactPeerConfigWatchUvarint(payload []byte, offset int) (uint64, int, error) {
	if offset < 0 || offset > len(payload) {
		return 0, 0, ErrCompactPeerConfigWatchPayloadInvalid
	}
	value, count := binary.Uvarint(payload[offset:])
	if count == 0 {
		return 0, 0, ErrCompactPeerConfigWatchPayloadInvalid
	}
	if count < 0 {
		return 0, 0, ErrCompactPeerConfigWatchPayloadInvalid
	}
	return value, offset + count, nil
}

func consumeCompactPeerConfigWatchUvarintAt(payload []byte, offset int) (uint64, int, error) {
	return consumeCompactPeerConfigWatchUvarint(payload, offset)
}

func consumeCompactPeerConfigWatchBytes(payload []byte, offset int, size uint64, maxSize int, tooLarge error) ([]byte, int, error) {
	if offset < 0 || offset > len(payload) {
		return nil, 0, ErrCompactPeerConfigWatchPayloadInvalid
	}
	if size > uint64(maxSize) {
		return nil, 0, tooLarge
	}
	if size > uint64(len(payload)-offset) {
		return nil, 0, ErrCompactPeerConfigWatchPayloadInvalid
	}
	end := offset + int(size)
	return payload[offset:end], end, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
