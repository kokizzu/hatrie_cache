package hatTopology

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

const (
	// ConfigWatchPeerCommand is the compact-peer command used by the bounded
	// configuration replay protocol. The command is intentionally stable so a
	// caller can route it through hatPeer.CompactPeerHandler without importing
	// a transport package here.
	ConfigWatchPeerCommand = "_hat.topology.config_watch.v1"

	// ConfigWatchPeerProtocolVersion identifies the binary request/response
	// payload format.
	ConfigWatchPeerProtocolVersion byte = 1

	// DefaultConfigWatchPeerMaxResponseBytes bounds one remote replay. It keeps
	// the default response below the compact peer frame budget even when a log
	// has a larger local value limit.
	DefaultConfigWatchPeerMaxResponseBytes = 8 << 20
	// MaxConfigWatchPeerResponseBytes prevents a caller from opting into an
	// unbounded response allocation.
	MaxConfigWatchPeerResponseBytes = 64 << 20
	maxConfigWatchPeerRequestBytes  = 4 << 10

	configWatchPeerOperationRead byte = iota + 1
	configWatchPeerOperationWait

	configWatchPeerStatusOK byte = iota
	configWatchPeerStatusGap
	configWatchPeerStatusError

	configWatchPeerErrorUnknown byte = iota + 1
	configWatchPeerErrorCanceled
	configWatchPeerErrorDeadlineExceeded
	configWatchPeerErrorInvalid
	configWatchPeerErrorAuthorization
)

var (
	// ErrConfigWatchPeerClientNil indicates a method call on a nil client.
	ErrConfigWatchPeerClientNil = errors.New("hatTopology: config watch peer client is nil")
	// ErrConfigWatchPeerCallRequired indicates that no transport callback was
	// configured for a peer client.
	ErrConfigWatchPeerCallRequired = errors.New("hatTopology: config watch peer call is required")
	// ErrConfigWatchPeerContextRequired indicates that a nil context was used.
	ErrConfigWatchPeerContextRequired = errors.New("hatTopology: config watch peer context is required")
	// ErrConfigWatchPeerPrincipalInvalid indicates that the client identity is
	// empty or exceeds the same bound as the local watch authorizer.
	ErrConfigWatchPeerPrincipalInvalid = errors.New("hatTopology: config watch peer principal is invalid")
	// ErrConfigWatchPeerRequestInvalid indicates a malformed binary request.
	ErrConfigWatchPeerRequestInvalid = errors.New("hatTopology: config watch peer request is invalid")
	// ErrConfigWatchPeerResponseInvalid indicates a malformed binary response.
	ErrConfigWatchPeerResponseInvalid = errors.New("hatTopology: config watch peer response is invalid")
	// ErrConfigWatchPeerRemote indicates a request rejected by the remote log.
	ErrConfigWatchPeerRemote = errors.New("hatTopology: config watch peer request was rejected")
	// ErrConfigWatchPeerPrincipalMismatch indicates that the wire identity did
	// not match the identity bound by the authenticated transport.
	ErrConfigWatchPeerPrincipalMismatch = errors.New("hatTopology: config watch peer principal mismatch")
	// ErrConfigWatchPeerResponseTooLarge indicates that a response exceeded the
	// configured client or server bound.
	ErrConfigWatchPeerResponseTooLarge = errors.New("hatTopology: config watch peer response is too large")
)

// ConfigWatchPeerCall sends one config-watch command to an authenticated peer.
// The adapter normally calls CompactPeerSession.Call and returns the response
// frame payload. Authentication and binding the principal to the peer identity
// remain responsibilities of the transport adapter.
type ConfigWatchPeerCall func(context.Context, []byte, []byte) ([]byte, error)

// ConfigWatchPeerClientOptions configures a reconnect-safe remote watcher.
// The client has no connection state: callers can replace Call after a
// reconnect and resume from the returned cursor.
type ConfigWatchPeerClientOptions struct {
	Call             ConfigWatchPeerCall
	Principal        string
	Prefix           string
	MaxResponseBytes int
}

// ConfigWatchPeerClient issues bounded replay and long-poll requests to a
// remote ConfigWatchLog. It does not retain events or spawn goroutines.
type ConfigWatchPeerClient struct {
	call             ConfigWatchPeerCall
	principal        string
	prefix           string
	maxResponseBytes int
}

// ConfigWatchPeerRemoteError is a non-sensitive error returned by a peer when
// the local log rejected a request. Detailed authorization errors stay on the
// server and are not sent over the wire.
type ConfigWatchPeerRemoteError struct {
	Code byte
}

func (err *ConfigWatchPeerRemoteError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: code=%d", ErrConfigWatchPeerRemote, err.Code)
}

// NewConfigWatchPeerClient validates a stateless remote watcher.
func NewConfigWatchPeerClient(options ConfigWatchPeerClientOptions) (*ConfigWatchPeerClient, error) {
	if options.Call == nil {
		return nil, ErrConfigWatchPeerCallRequired
	}
	if options.MaxResponseBytes == 0 {
		options.MaxResponseBytes = DefaultConfigWatchPeerMaxResponseBytes
	}
	if options.MaxResponseBytes < 1 || options.MaxResponseBytes > MaxConfigWatchPeerResponseBytes {
		return nil, ErrConfigWatchPeerResponseTooLarge
	}
	principal, err := normalizeConfigWatchPeerPrincipal(options.Principal)
	if err != nil {
		return nil, err
	}
	prefix, err := normalizeConfigWatchPeerPrefix(options.Prefix)
	if err != nil {
		return nil, err
	}
	return &ConfigWatchPeerClient{
		call:             options.Call,
		principal:        principal,
		prefix:           prefix,
		maxResponseBytes: options.MaxResponseBytes,
	}, nil
}

// Read replays retained events after afterVersion. A history gap is returned
// as *ConfigWatchGapError so a caller can fetch a fresh snapshot before
// resuming from the supplied current version.
func (client *ConfigWatchPeerClient) Read(ctx context.Context, afterVersion uint64, limit int) ([]ConfigWatchEvent, uint64, error) {
	return client.request(ctx, configWatchPeerOperationRead, afterVersion, limit)
}

// Wait replays immediately available events or waits for a newer event. The
// caller's context is forwarded to the transport and can cancel a compact
// peer request when request cancellation is enabled on that session.
func (client *ConfigWatchPeerClient) Wait(ctx context.Context, afterVersion uint64, limit int) ([]ConfigWatchEvent, uint64, error) {
	return client.request(ctx, configWatchPeerOperationWait, afterVersion, limit)
}

// HandleConfigWatchPeerRequest decodes one bounded peer request and executes
// it against the log. Malformed payloads return an error to the transport;
// authenticated application errors are encoded in a small response so the
// client can preserve gap and cancellation semantics without leaking details.
func (log *ConfigWatchLog) HandleConfigWatchPeerRequest(ctx context.Context, payload []byte) ([]byte, error) {
	request, err := decodeConfigWatchPeerRequest(payload)
	if err != nil {
		return nil, err
	}
	return log.handleConfigWatchPeerRequest(ctx, request)
}

// HandleConfigWatchPeerRequestForPrincipal is the safer transport adapter
// entry point. It binds the request to an identity established by the
// authenticated connection instead of trusting only the wire principal.
func (log *ConfigWatchLog) HandleConfigWatchPeerRequestForPrincipal(ctx context.Context, principal string, payload []byte) ([]byte, error) {
	boundPrincipal, err := normalizeConfigWatchPeerPrincipal(principal)
	if err != nil {
		return nil, err
	}
	request, err := decodeConfigWatchPeerRequest(payload)
	if err != nil {
		return nil, err
	}
	if request.principal != boundPrincipal {
		return nil, ErrConfigWatchPeerPrincipalMismatch
	}
	return log.handleConfigWatchPeerRequest(ctx, request)
}

func (log *ConfigWatchLog) handleConfigWatchPeerRequest(ctx context.Context, request configWatchPeerRequest) ([]byte, error) {
	var (
		events []ConfigWatchEvent
		cursor uint64
		err    error
	)
	switch request.operation {
	case configWatchPeerOperationRead:
		events, cursor, err = log.Read(ctx, ConfigWatchRequest{
			Principal:    request.principal,
			Prefix:       request.prefix,
			AfterVersion: request.afterVersion,
			Limit:        request.limit,
		})
	case configWatchPeerOperationWait:
		events, cursor, err = log.Wait(ctx, ConfigWatchRequest{
			Principal:    request.principal,
			Prefix:       request.prefix,
			AfterVersion: request.afterVersion,
			Limit:        request.limit,
		})
	default:
		return nil, ErrConfigWatchPeerRequestInvalid
	}
	if err != nil {
		return encodeConfigWatchPeerError(err)
	}
	return encodeConfigWatchPeerEvents(events, cursor, DefaultConfigWatchPeerMaxResponseBytes)
}

type configWatchPeerRequest struct {
	operation    byte
	principal    string
	prefix       string
	afterVersion uint64
	limit        int
}

func (client *ConfigWatchPeerClient) request(ctx context.Context, operation byte, afterVersion uint64, limit int) ([]ConfigWatchEvent, uint64, error) {
	if client == nil {
		return nil, afterVersion, ErrConfigWatchPeerClientNil
	}
	if ctx == nil {
		return nil, afterVersion, ErrConfigWatchPeerContextRequired
	}
	payload, err := encodeConfigWatchPeerRequest(configWatchPeerRequest{
		operation:    operation,
		principal:    client.principal,
		prefix:       client.prefix,
		afterVersion: afterVersion,
		limit:        limit,
	})
	if err != nil {
		return nil, afterVersion, err
	}
	encoded, err := client.call(ctx, []byte(ConfigWatchPeerCommand), payload)
	if err != nil {
		return nil, afterVersion, err
	}
	if len(encoded) > client.maxResponseBytes {
		return nil, afterVersion, ErrConfigWatchPeerResponseTooLarge
	}
	return decodeConfigWatchPeerResponse(encoded, afterVersion)
}

func encodeConfigWatchPeerRequest(request configWatchPeerRequest) ([]byte, error) {
	if request.operation != configWatchPeerOperationRead && request.operation != configWatchPeerOperationWait {
		return nil, ErrConfigWatchPeerRequestInvalid
	}
	principal, err := normalizeConfigWatchPeerPrincipal(request.principal)
	if err != nil {
		return nil, err
	}
	prefix, err := normalizeConfigWatchPeerPrefix(request.prefix)
	if err != nil {
		return nil, err
	}
	if request.limit < 0 || request.limit > MaxConfigWatchReadLimit {
		return nil, ErrConfigWatchReadLimitInvalid
	}
	encoded := make([]byte, 0, 32+len(principal))
	encoded = append(encoded, ConfigWatchPeerProtocolVersion, request.operation)
	encoded = appendConfigWatchPeerUvarint(encoded, uint64(len(principal)))
	encoded = append(encoded, principal...)
	encoded = appendConfigWatchPeerUvarint(encoded, uint64(len(prefix)))
	encoded = append(encoded, prefix...)
	encoded = appendConfigWatchPeerUvarint(encoded, request.afterVersion)
	encoded = appendConfigWatchPeerUvarint(encoded, uint64(request.limit))
	if len(encoded) > maxConfigWatchPeerRequestBytes {
		return nil, ErrConfigWatchPeerRequestInvalid
	}
	return encoded, nil
}

func decodeConfigWatchPeerRequest(payload []byte) (configWatchPeerRequest, error) {
	if len(payload) < 2 || len(payload) > maxConfigWatchPeerRequestBytes || payload[0] != ConfigWatchPeerProtocolVersion {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	operation := payload[1]
	if operation != configWatchPeerOperationRead && operation != configWatchPeerOperationWait {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	offset := 2
	principalBytes, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || principalBytes == 0 || principalBytes > maxConfigWatchPrincipalBytes || principalBytes > uint64(len(payload)-offset) {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	principal := string(payload[offset : offset+int(principalBytes)])
	offset += int(principalBytes)
	prefixBytes, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || prefixBytes > MaxConfigWatchKeyBytes || prefixBytes > uint64(len(payload)-offset) {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	prefix := string(payload[offset : offset+int(prefixBytes)])
	offset += int(prefixBytes)
	afterVersion, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	limitValue, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || limitValue > MaxConfigWatchReadLimit || offset != len(payload) {
		return configWatchPeerRequest{}, ErrConfigWatchPeerRequestInvalid
	}
	return configWatchPeerRequest{
		operation:    operation,
		principal:    principal,
		prefix:       prefix,
		afterVersion: afterVersion,
		limit:        int(limitValue),
	}, nil
}

func encodeConfigWatchPeerEvents(events []ConfigWatchEvent, cursor uint64, maxBytes int) ([]byte, error) {
	encoded := make([]byte, 0, minConfigWatchPeerResponseCapacity(events))
	encoded = append(encoded, ConfigWatchPeerProtocolVersion, configWatchPeerStatusOK)
	encoded = appendConfigWatchPeerUvarint(encoded, cursor)
	encoded = appendConfigWatchPeerUvarint(encoded, uint64(len(events)))
	for _, event := range events {
		if event.Source == "" || len(event.Source) > maxConfigWatchSourceBytes || event.Key == "" || len(event.Key) > MaxConfigWatchKeyBytes || len(event.Value) > MaxConfigWatchValueBytes || event.Deleted && len(event.Value) != 0 {
			return nil, ErrConfigWatchPeerResponseInvalid
		}
		encoded = appendConfigWatchPeerUvarint(encoded, event.Version)
		var ok bool
		if encoded, ok = appendConfigWatchPeerBytesBounded(encoded, []byte(event.Source), maxBytes); !ok {
			return nil, ErrConfigWatchPeerResponseTooLarge
		}
		if encoded, ok = appendConfigWatchPeerBytesBounded(encoded, []byte(event.Key), maxBytes); !ok {
			return nil, ErrConfigWatchPeerResponseTooLarge
		}
		var flags byte
		if event.Deleted {
			flags = 1
		}
		if len(encoded) >= maxBytes {
			return nil, ErrConfigWatchPeerResponseTooLarge
		}
		encoded = append(encoded, flags)
		if encoded, ok = appendConfigWatchPeerBytesBounded(encoded, event.Value, maxBytes); !ok {
			return nil, ErrConfigWatchPeerResponseTooLarge
		}
	}
	return encoded, nil
}

func encodeConfigWatchPeerError(err error) ([]byte, error) {
	var gapErr *ConfigWatchGapError
	if errors.As(err, &gapErr) {
		encoded := make([]byte, 0, 32)
		encoded = append(encoded, ConfigWatchPeerProtocolVersion, configWatchPeerStatusGap)
		encoded = appendConfigWatchPeerUvarint(encoded, gapErr.AfterVersion)
		encoded = appendConfigWatchPeerUvarint(encoded, 0)
		encoded = appendConfigWatchPeerUvarint(encoded, gapErr.AfterVersion)
		encoded = appendConfigWatchPeerUvarint(encoded, gapErr.EarliestVersion)
		encoded = appendConfigWatchPeerUvarint(encoded, gapErr.CurrentVersion)
		return encoded, nil
	}
	code := configWatchPeerErrorUnknown
	switch {
	case errors.Is(err, context.Canceled):
		code = configWatchPeerErrorCanceled
	case errors.Is(err, context.DeadlineExceeded):
		code = configWatchPeerErrorDeadlineExceeded
	case errors.Is(err, ErrConfigWatchReadLimitInvalid), errors.Is(err, ErrConfigWatchPrincipalInvalid), errors.Is(err, ErrConfigWatchPrefixInvalid), errors.Is(err, ErrConfigWatchContextRequired):
		code = configWatchPeerErrorInvalid
	default:
		code = configWatchPeerErrorAuthorization
	}
	return []byte{ConfigWatchPeerProtocolVersion, configWatchPeerStatusError, 0, 0, code}, nil
}

func decodeConfigWatchPeerResponse(payload []byte, requestedAfter uint64) ([]ConfigWatchEvent, uint64, error) {
	if len(payload) < 4 || payload[0] != ConfigWatchPeerProtocolVersion {
		return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
	}
	offset := 2
	cursor, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok {
		return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
	}
	eventCount, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || eventCount > MaxConfigWatchReadLimit {
		return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
	}
	switch payload[1] {
	case configWatchPeerStatusOK:
		events := make([]ConfigWatchEvent, 0, int(eventCount))
		lastVersion := requestedAfter
		for index := uint64(0); index < eventCount; index++ {
			event, next, eventOK := decodeConfigWatchPeerEvent(payload, offset)
			if !eventOK {
				return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
			}
			if event.Version <= lastVersion {
				return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
			}
			offset = next
			events = append(events, event)
			lastVersion = event.Version
		}
		if offset != len(payload) {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		if (len(events) == 0 && cursor != requestedAfter) || (len(events) > 0 && cursor != lastVersion) {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		return events, cursor, nil
	case configWatchPeerStatusGap:
		if eventCount != 0 {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		afterVersion, afterOK := readConfigWatchPeerUvarint(payload, &offset)
		earliestVersion, earliestOK := readConfigWatchPeerUvarint(payload, &offset)
		currentVersion, currentOK := readConfigWatchPeerUvarint(payload, &offset)
		if !afterOK || !earliestOK || !currentOK || cursor != afterVersion || afterVersion != requestedAfter || offset != len(payload) {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		return nil, afterVersion, &ConfigWatchGapError{
			AfterVersion:    afterVersion,
			EarliestVersion: earliestVersion,
			CurrentVersion:  currentVersion,
		}
	case configWatchPeerStatusError:
		if eventCount != 0 || cursor != 0 || offset >= len(payload) {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		code := payload[offset]
		offset++
		if offset != len(payload) {
			return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
		}
		switch code {
		case configWatchPeerErrorCanceled:
			return nil, requestedAfter, context.Canceled
		case configWatchPeerErrorDeadlineExceeded:
			return nil, requestedAfter, context.DeadlineExceeded
		default:
			return nil, requestedAfter, &ConfigWatchPeerRemoteError{Code: code}
		}
	default:
		return nil, requestedAfter, ErrConfigWatchPeerResponseInvalid
	}
}

func decodeConfigWatchPeerEvent(payload []byte, offset int) (ConfigWatchEvent, int, bool) {
	version, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || version == 0 {
		return ConfigWatchEvent{}, offset, false
	}
	source, next, ok := readConfigWatchPeerBytes(payload, offset, maxConfigWatchSourceBytes)
	if !ok || len(source) == 0 {
		return ConfigWatchEvent{}, offset, false
	}
	key, next, ok := readConfigWatchPeerBytes(payload, next, MaxConfigWatchKeyBytes)
	if !ok || len(key) == 0 || next >= len(payload) {
		return ConfigWatchEvent{}, offset, false
	}
	flags := payload[next]
	next++
	if flags&^byte(1) != 0 {
		return ConfigWatchEvent{}, offset, false
	}
	value, next, ok := readConfigWatchPeerBytes(payload, next, MaxConfigWatchValueBytes)
	if !ok || flags&1 != 0 && len(value) != 0 {
		return ConfigWatchEvent{}, offset, false
	}
	return ConfigWatchEvent{
		Version: version,
		Source:  string(source),
		Key:     string(key),
		Value:   value,
		Deleted: flags&1 != 0,
	}, next, true
}

func normalizeConfigWatchPeerPrincipal(principal string) (string, error) {
	principal = strings.TrimSpace(principal)
	if principal == "" || len(principal) > maxConfigWatchPrincipalBytes {
		return "", ErrConfigWatchPeerPrincipalInvalid
	}
	return principal, nil
}

func normalizeConfigWatchPeerPrefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if len(prefix) > MaxConfigWatchKeyBytes {
		return "", ErrConfigWatchPrefixInvalid
	}
	return prefix, nil
}

func appendConfigWatchPeerUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:count]...)
}

func appendConfigWatchPeerBytes(dst, value []byte) []byte {
	dst = appendConfigWatchPeerUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func appendConfigWatchPeerBytesBounded(dst, value []byte, maxBytes int) ([]byte, bool) {
	lengthSize := configWatchPeerUvarintSize(uint64(len(value)))
	if maxBytes < 0 || len(dst) > maxBytes-lengthSize-len(value) {
		return dst, false
	}
	return appendConfigWatchPeerBytes(dst, value), true
}

func configWatchPeerUvarintSize(value uint64) int {
	size := 1
	for value >= 1<<7 {
		value >>= 7
		size++
	}
	return size
}

func readConfigWatchPeerUvarint(payload []byte, offset *int) (uint64, bool) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, false
	}
	value, count := binary.Uvarint(payload[*offset:])
	if count <= 0 {
		return 0, false
	}
	*offset += count
	return value, true
}

func readConfigWatchPeerBytes(payload []byte, offset, maxBytes int) ([]byte, int, bool) {
	length, ok := readConfigWatchPeerUvarint(payload, &offset)
	if !ok || length > uint64(maxBytes) || length > uint64(len(payload)-offset) {
		return nil, offset, false
	}
	end := offset + int(length)
	return append([]byte(nil), payload[offset:end]...), end, true
}

func minConfigWatchPeerResponseCapacity(events []ConfigWatchEvent) int {
	if len(events) == 0 {
		return 16
	}
	return min(16+len(events)*16, DefaultConfigWatchPeerMaxResponseBytes)
}
