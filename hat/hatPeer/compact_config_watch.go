package hatPeer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	"hatrie_cache/hat/hatTopology"
)

const (
	// CompactPeerConfigWatchCommand identifies the versioned peer watch RPC.
	CompactPeerConfigWatchCommand   = "_hat.peer.config.watch.v1"
	compactPeerConfigWatchVersion   = 1
	compactPeerConfigWatchGap       = 1
	compactPeerConfigWatchSuccess   = 0
	compactPeerConfigWatchMaxEvents = hatTopology.MaxConfigWatchReadLimit
	compactPeerConfigWatchMaxString = 1 << 20
)

var (
	ErrCompactPeerConfigWatchSourceRequired   = errors.New("hatPeer: config watch source is required")
	ErrCompactPeerConfigWatchRequestInvalid   = errors.New("hatPeer: config watch request is invalid")
	ErrCompactPeerConfigWatchResponseInvalid  = errors.New("hatPeer: config watch response is invalid")
	ErrCompactPeerConfigWatchPrincipalInvalid = errors.New("hatPeer: config watch principal is invalid")
	ErrCompactPeerConfigWatchReconnect        = errors.New("hatPeer: config watch reconnect failed")
)

// CompactPeerConfigWatchSource is implemented by hatTopology.ConfigWatchLog
// and allows a peer handler to serve either replay or wait requests.
type CompactPeerConfigWatchSource interface {
	Read(context.Context, hatTopology.ConfigWatchRequest) ([]hatTopology.ConfigWatchEvent, uint64, error)
	Wait(context.Context, hatTopology.ConfigWatchRequest) ([]hatTopology.ConfigWatchEvent, uint64, error)
}

// NewCompactPeerConfigWatchHandler returns a bounded compact-protocol handler
// for a config watch source. The request cursor is idempotent, so clients can
// safely resend it after reconnecting.
func NewCompactPeerConfigWatchHandler(source CompactPeerConfigWatchSource) CompactPeerHandler {
	return func(ctx context.Context, request CompactFrame) (CompactFrame, error) {
		if !bytes.Equal(request.Command, []byte(CompactPeerConfigWatchCommand)) {
			return CompactFrame{}, ErrCompactPeerConfigWatchRequestInvalid
		}
		if source == nil {
			return CompactFrame{}, ErrCompactPeerConfigWatchSourceRequired
		}
		watchRequest, err := decodeCompactPeerConfigWatchRequest(request.Payload)
		if err != nil {
			return CompactFrame{}, err
		}
		requestForSource := hatTopology.ConfigWatchRequest{
			Principal:    watchRequest.principal,
			AfterVersion: watchRequest.afterVersion,
			Limit:        watchRequest.limit,
			KeyPrefix:    watchRequest.keyPrefix,
		}
		var events []hatTopology.ConfigWatchEvent
		var next uint64
		if watchRequest.wait {
			events, next, err = source.Wait(ctx, requestForSource)
		} else {
			events, next, err = source.Read(ctx, requestForSource)
		}
		if err != nil {
			var gap *hatTopology.ConfigWatchGapError
			if errors.As(err, &gap) {
				payload, encodeErr := encodeCompactPeerConfigWatchGap(gap)
				if encodeErr != nil {
					return CompactFrame{}, encodeErr
				}
				return CompactFrame{Command: []byte(CompactPeerConfigWatchCommand), Payload: payload}, nil
			}
			return CompactFrame{}, err
		}
		payload, err := encodeCompactPeerConfigWatchResponse(events, next)
		if err != nil {
			return CompactFrame{}, err
		}
		return CompactFrame{Command: []byte(CompactPeerConfigWatchCommand), Payload: payload}, nil
	}
}

// CompactPeerConfigWatchClientOptions configures a cursor-based peer watch.
// Reconnect is optional; when set, one failed idempotent read is retried on a
// fresh session before the error is returned.
type CompactPeerConfigWatchClientOptions struct {
	Session   *CompactPeerSession
	Reconnect func(context.Context) (*CompactPeerSession, error)
	Principal string
	KeyPrefix string
	Limit     int
}

// CompactPeerConfigWatchClient is a reconnectable remote config cursor.
type CompactPeerConfigWatchClient struct {
	mu        sync.RWMutex
	callMu    sync.Mutex
	session   *CompactPeerSession
	reconnect func(context.Context) (*CompactPeerSession, error)
	principal string
	keyPrefix string
	limit     int
	cursor    uint64
	lastError error
}

// NewCompactPeerConfigWatchClient validates and creates a remote watch
// cursor. It does not start a goroutine; callers choose Read or Wait.
func NewCompactPeerConfigWatchClient(options CompactPeerConfigWatchClientOptions) (*CompactPeerConfigWatchClient, error) {
	if options.Session == nil {
		return nil, ErrCompactPeerClosed
	}
	principal := strings.TrimSpace(options.Principal)
	if principal == "" || len(principal) > 128 {
		return nil, ErrCompactPeerConfigWatchPrincipalInvalid
	}
	if len(options.KeyPrefix) > hatTopology.MaxConfigWatchKeyBytes {
		return nil, hatTopology.ErrConfigWatchKeyInvalid
	}
	limit := options.Limit
	if limit == 0 {
		limit = hatTopology.DefaultConfigWatchReadLimit
	}
	if limit < 1 || limit > hatTopology.MaxConfigWatchReadLimit {
		return nil, hatTopology.ErrConfigWatchReadLimitInvalid
	}
	return &CompactPeerConfigWatchClient{
		session:   options.Session,
		reconnect: options.Reconnect,
		principal: principal,
		keyPrefix: options.KeyPrefix,
		limit:     limit,
	}, nil
}

// Read returns currently retained events and advances the cursor to the last
// scanned version, including unrelated keys skipped by KeyPrefix.
func (client *CompactPeerConfigWatchClient) Read(ctx context.Context) ([]hatTopology.ConfigWatchEvent, error) {
	return client.roundTrip(ctx, false)
}

// Wait blocks until matching events are available or ctx is canceled.
func (client *CompactPeerConfigWatchClient) Wait(ctx context.Context) ([]hatTopology.ConfigWatchEvent, error) {
	return client.roundTrip(ctx, true)
}

// Cursor returns the last server version acknowledged by a successful read.
func (client *CompactPeerConfigWatchClient) Cursor() uint64 {
	if client == nil {
		return 0
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.cursor
}

// LastError returns the last transport or typed history-gap error.
func (client *CompactPeerConfigWatchClient) LastError() error {
	if client == nil {
		return ErrCompactPeerClosed
	}
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.lastError
}

// ResetCursor sets the cursor after the caller has installed a fresh snapshot
// following a history gap.
func (client *CompactPeerConfigWatchClient) ResetCursor(version uint64) error {
	if client == nil {
		return ErrCompactPeerClosed
	}
	client.mu.Lock()
	client.cursor = version
	client.lastError = nil
	client.mu.Unlock()
	return nil
}

func (client *CompactPeerConfigWatchClient) roundTrip(ctx context.Context, wait bool) ([]hatTopology.ConfigWatchEvent, error) {
	if client == nil {
		return nil, ErrCompactPeerClosed
	}
	client.callMu.Lock()
	defer client.callMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	client.mu.RLock()
	session := client.session
	request := compactPeerConfigWatchRequest{
		principal:    client.principal,
		keyPrefix:    client.keyPrefix,
		afterVersion: client.cursor,
		limit:        client.limit,
		wait:         wait,
	}
	client.mu.RUnlock()
	payload, err := encodeCompactPeerConfigWatchRequest(request)
	if err != nil {
		return nil, client.recordError(err)
	}
	frame, err := session.Call(ctx, []byte(CompactPeerConfigWatchCommand), payload)
	if err != nil && ctx.Err() == nil {
		client.mu.RLock()
		reconnect := client.reconnect
		client.mu.RUnlock()
		if reconnect != nil {
			next, reconnectErr := reconnect(ctx)
			if reconnectErr != nil {
				return nil, client.recordError(fmt.Errorf("%w: %v", ErrCompactPeerConfigWatchReconnect, reconnectErr))
			}
			if next == nil {
				return nil, client.recordError(ErrCompactPeerConfigWatchReconnect)
			}
			client.mu.Lock()
			client.session = next
			client.mu.Unlock()
			frame, err = next.Call(ctx, []byte(CompactPeerConfigWatchCommand), payload)
		}
	}
	if err != nil {
		return nil, client.recordError(err)
	}
	if frame.Kind == CompactError {
		return nil, client.recordError(fmt.Errorf("%w: %s", ErrCompactPeerRemote, string(frame.Payload)))
	}
	if !bytes.Equal(frame.Command, []byte(CompactPeerConfigWatchCommand)) {
		return nil, client.recordError(ErrCompactPeerConfigWatchResponseInvalid)
	}
	events, next, gap, err := decodeCompactPeerConfigWatchResponse(frame.Payload)
	if err != nil {
		return nil, client.recordError(err)
	}
	if gap != nil {
		if gap.AfterVersion != request.afterVersion {
			return nil, client.recordError(ErrCompactPeerConfigWatchResponseInvalid)
		}
		return nil, client.recordError(gap)
	}
	if next < request.afterVersion {
		return nil, client.recordError(ErrCompactPeerConfigWatchResponseInvalid)
	}
	for index := 1; index < len(events); index++ {
		if events[index].Version <= events[index-1].Version {
			return nil, client.recordError(ErrCompactPeerConfigWatchResponseInvalid)
		}
	}
	if len(events) > 0 && events[len(events)-1].Version > next {
		return nil, client.recordError(ErrCompactPeerConfigWatchResponseInvalid)
	}
	client.mu.Lock()
	client.cursor = next
	client.lastError = nil
	client.mu.Unlock()
	return events, nil
}

func (client *CompactPeerConfigWatchClient) recordError(err error) error {
	client.mu.Lock()
	client.lastError = err
	client.mu.Unlock()
	return err
}

type compactPeerConfigWatchRequest struct {
	principal    string
	keyPrefix    string
	afterVersion uint64
	limit        int
	wait         bool
}

func encodeCompactPeerConfigWatchRequest(request compactPeerConfigWatchRequest) ([]byte, error) {
	if request.principal == "" || len(request.principal) > 128 || len(request.keyPrefix) > hatTopology.MaxConfigWatchKeyBytes || request.limit < 1 || request.limit > hatTopology.MaxConfigWatchReadLimit {
		return nil, ErrCompactPeerConfigWatchRequestInvalid
	}
	payload := make([]byte, 0, 32+len(request.principal)+len(request.keyPrefix))
	payload = append(payload, compactPeerConfigWatchVersion)
	if request.wait {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	payload = binary.AppendUvarint(payload, request.afterVersion)
	payload = binary.AppendUvarint(payload, uint64(request.limit))
	var err error
	payload, err = appendCompactPeerConfigWatchString(payload, request.principal, 128)
	if err != nil {
		return nil, err
	}
	return appendCompactPeerConfigWatchString(payload, request.keyPrefix, hatTopology.MaxConfigWatchKeyBytes)
}

func decodeCompactPeerConfigWatchRequest(payload []byte) (compactPeerConfigWatchRequest, error) {
	if len(payload) < 2 || payload[0] != compactPeerConfigWatchVersion || payload[1] > 1 {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchRequestInvalid
	}
	offset := 2
	after, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
	if !ok {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchRequestInvalid
	}
	limitValue, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
	if !ok || limitValue < 1 || limitValue > hatTopology.MaxConfigWatchReadLimit {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchRequestInvalid
	}
	principal, ok := readCompactPeerConfigWatchString(payload, &offset, 128)
	if !ok || principal == "" {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchRequestInvalid
	}
	keyPrefix, ok := readCompactPeerConfigWatchString(payload, &offset, hatTopology.MaxConfigWatchKeyBytes)
	if !ok || offset != len(payload) {
		return compactPeerConfigWatchRequest{}, ErrCompactPeerConfigWatchRequestInvalid
	}
	return compactPeerConfigWatchRequest{principal: principal, keyPrefix: keyPrefix, afterVersion: after, limit: int(limitValue), wait: payload[1] == 1}, nil
}

func encodeCompactPeerConfigWatchResponse(events []hatTopology.ConfigWatchEvent, next uint64) ([]byte, error) {
	if len(events) > compactPeerConfigWatchMaxEvents {
		return nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	payload := []byte{compactPeerConfigWatchVersion, compactPeerConfigWatchSuccess}
	payload = binary.AppendUvarint(payload, next)
	payload = binary.AppendUvarint(payload, uint64(len(events)))
	for _, event := range events {
		if event.Version == 0 || event.Source == "" || event.Key == "" || (event.Deleted && len(event.Value) != 0) {
			return nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		var err error
		payload = binary.AppendUvarint(payload, event.Version)
		flags := byte(0)
		if event.Deleted {
			flags = 1
		}
		payload = append(payload, flags)
		payload, err = appendCompactPeerConfigWatchString(payload, event.Source, compactPeerConfigWatchMaxString)
		if err != nil {
			return nil, err
		}
		payload, err = appendCompactPeerConfigWatchString(payload, event.Key, hatTopology.MaxConfigWatchKeyBytes)
		if err != nil {
			return nil, err
		}
		payload, err = appendCompactPeerConfigWatchBytes(payload, event.Value, hatTopology.MaxConfigWatchValueBytes)
		if err != nil {
			return nil, err
		}
	}
	return payload, nil
}

func encodeCompactPeerConfigWatchGap(gap *hatTopology.ConfigWatchGapError) ([]byte, error) {
	if gap == nil {
		return nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	payload := []byte{compactPeerConfigWatchVersion, compactPeerConfigWatchGap}
	payload = binary.AppendUvarint(payload, gap.CurrentVersion)
	payload = binary.AppendUvarint(payload, gap.AfterVersion)
	payload = binary.AppendUvarint(payload, gap.EarliestVersion)
	return payload, nil
}

func decodeCompactPeerConfigWatchResponse(payload []byte) ([]hatTopology.ConfigWatchEvent, uint64, *hatTopology.ConfigWatchGapError, error) {
	if len(payload) < 2 || payload[0] != compactPeerConfigWatchVersion {
		return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	offset := 2
	if payload[1] == compactPeerConfigWatchGap {
		current, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
		if !ok {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		after, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
		if !ok {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		earliest, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
		if !ok || offset != len(payload) {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		return nil, after, &hatTopology.ConfigWatchGapError{AfterVersion: after, EarliestVersion: earliest, CurrentVersion: current}, nil
	}
	if payload[1] != compactPeerConfigWatchSuccess {
		return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	next, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
	if !ok {
		return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	count, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
	if !ok || count > compactPeerConfigWatchMaxEvents {
		return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	events := make([]hatTopology.ConfigWatchEvent, 0, count)
	for index := uint64(0); index < count; index++ {
		version, ok := readCompactPeerConfigWatchUvarint(payload, &offset)
		if !ok || offset >= len(payload) {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		flags := payload[offset]
		offset++
		if flags > 1 {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		source, ok := readCompactPeerConfigWatchString(payload, &offset, compactPeerConfigWatchMaxString)
		if !ok || source == "" {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		key, ok := readCompactPeerConfigWatchString(payload, &offset, hatTopology.MaxConfigWatchKeyBytes)
		if !ok || key == "" {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		value, ok := readCompactPeerConfigWatchBytes(payload, &offset, hatTopology.MaxConfigWatchValueBytes)
		if !ok || (flags == 1 && len(value) != 0) {
			return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
		}
		events = append(events, hatTopology.ConfigWatchEvent{Version: version, Source: source, Key: key, Value: value, Deleted: flags == 1})
	}
	if offset != len(payload) {
		return nil, 0, nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	return events, next, nil, nil
}

func appendCompactPeerConfigWatchString(dst []byte, value string, max int) ([]byte, error) {
	if len(value) > max {
		return nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...), nil
}

func appendCompactPeerConfigWatchBytes(dst, value []byte, max int) ([]byte, error) {
	if len(value) > max {
		return nil, ErrCompactPeerConfigWatchResponseInvalid
	}
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	return append(dst, value...), nil
}

func readCompactPeerConfigWatchString(payload []byte, offset *int, max int) (string, bool) {
	value, ok := readCompactPeerConfigWatchBytes(payload, offset, max)
	return string(value), ok
}

func readCompactPeerConfigWatchBytes(payload []byte, offset *int, max int) ([]byte, bool) {
	if offset == nil || *offset < 0 || *offset > len(payload) {
		return nil, false
	}
	length, ok := readCompactPeerConfigWatchUvarint(payload, offset)
	if !ok || *offset > len(payload) || length > uint64(max) || length > uint64(len(payload)-*offset) {
		return nil, false
	}
	start := *offset
	*offset += int(length)
	return append([]byte(nil), payload[start:*offset]...), true
}

func readCompactPeerConfigWatchUvarint(payload []byte, offset *int) (uint64, bool) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, false
	}
	value, width := binary.Uvarint(payload[*offset:])
	if width <= 0 {
		return 0, false
	}
	*offset += width
	return value, true
}
