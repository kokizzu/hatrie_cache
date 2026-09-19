package hatSql

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	DefaultWebhookEventDeduplicatorCapacity         = 4096
	DefaultWebhookEventDeduplicatorTTL              = 24 * time.Hour
	DefaultWebhookEventDeduplicatorMaxSourceBytes   = 128
	DefaultWebhookEventDeduplicatorMaxEventIDBytes  = 256
	DefaultWebhookEventDeduplicatorMaxPayloadBytes  = 16 << 20
	DefaultWebhookEventDeduplicatorMaxSnapshotBytes = 8 << 20

	maxWebhookEventDeduplicatorCapacity      = 1 << 20
	maxWebhookEventDeduplicatorTTL           = 365 * 24 * time.Hour
	maxWebhookEventDeduplicatorLabelBytes    = 4096
	maxWebhookEventDeduplicatorPayloadBytes  = 64 << 20
	maxWebhookEventDeduplicatorSnapshotBytes = 64 << 20
	webhookEventDeduplicatorSnapshotMagic    = "HWE1"
	webhookEventDeduplicatorSnapshotVersion  = 1
	webhookEventDeduplicatorFingerprintBytes = sha256.Size
)

var (
	ErrWebhookEventInvalid  = errors.New("invalid webhook event idempotency input")
	ErrWebhookEventConflict = errors.New("webhook event idempotency conflict")
	ErrWebhookEventCapacity = errors.New("webhook event idempotency capacity reached")
	ErrWebhookEventSnapshot = errors.New("invalid webhook event idempotency snapshot")
)

// WebhookEventDecision describes the result of admitting an event.
type WebhookEventDecision uint8

const (
	WebhookEventAccepted WebhookEventDecision = iota
	WebhookEventDuplicate
)

// WebhookEventDeduplicatorOptions bounds retained event identities and the
// snapshot format. Zero values select the documented defaults.
type WebhookEventDeduplicatorOptions struct {
	Capacity         int
	TTL              time.Duration
	MaxSourceBytes   int
	MaxEventIDBytes  int
	MaxPayloadBytes  int
	MaxSnapshotBytes int
}

type webhookEventKey struct {
	source  string
	eventID string
}

type webhookEventRecord struct {
	fingerprint [webhookEventDeduplicatorFingerprintBytes]byte
	expiresAt   int64
}

// WebhookEventDeduplicator is an opt-in, bounded event-ID ledger. It stores a
// SHA-256 payload fingerprint rather than the payload, so same-ID retries are
// accepted only when the event body is identical.
type WebhookEventDeduplicator struct {
	mu               sync.Mutex
	capacity         int
	ttl              time.Duration
	maxSourceBytes   int
	maxEventIDBytes  int
	maxPayloadBytes  int
	maxSnapshotBytes int
	entries          map[webhookEventKey]webhookEventRecord
}

// NewWebhookEventDeduplicator creates a bounded event ledger.
func NewWebhookEventDeduplicator(options WebhookEventDeduplicatorOptions) (*WebhookEventDeduplicator, error) {
	if options.Capacity < 0 || options.TTL < 0 || options.MaxSourceBytes < 0 || options.MaxEventIDBytes < 0 || options.MaxPayloadBytes < 0 || options.MaxSnapshotBytes < 0 {
		return nil, ErrWebhookEventInvalid
	}
	if options.Capacity == 0 {
		options.Capacity = DefaultWebhookEventDeduplicatorCapacity
	}
	if options.TTL == 0 {
		options.TTL = DefaultWebhookEventDeduplicatorTTL
	}
	if options.MaxSourceBytes == 0 {
		options.MaxSourceBytes = DefaultWebhookEventDeduplicatorMaxSourceBytes
	}
	if options.MaxEventIDBytes == 0 {
		options.MaxEventIDBytes = DefaultWebhookEventDeduplicatorMaxEventIDBytes
	}
	if options.MaxPayloadBytes == 0 {
		options.MaxPayloadBytes = DefaultWebhookEventDeduplicatorMaxPayloadBytes
	}
	if options.MaxSnapshotBytes == 0 {
		options.MaxSnapshotBytes = DefaultWebhookEventDeduplicatorMaxSnapshotBytes
	}
	if options.Capacity > maxWebhookEventDeduplicatorCapacity ||
		options.TTL > maxWebhookEventDeduplicatorTTL ||
		options.MaxSourceBytes > maxWebhookEventDeduplicatorLabelBytes ||
		options.MaxEventIDBytes > maxWebhookEventDeduplicatorLabelBytes ||
		options.MaxPayloadBytes > maxWebhookEventDeduplicatorPayloadBytes ||
		options.MaxSnapshotBytes > maxWebhookEventDeduplicatorSnapshotBytes {
		return nil, ErrWebhookEventInvalid
	}
	return &WebhookEventDeduplicator{
		capacity:         options.Capacity,
		ttl:              options.TTL,
		maxSourceBytes:   options.MaxSourceBytes,
		maxEventIDBytes:  options.MaxEventIDBytes,
		maxPayloadBytes:  options.MaxPayloadBytes,
		maxSnapshotBytes: options.MaxSnapshotBytes,
		entries:          make(map[webhookEventKey]webhookEventRecord, options.Capacity),
	}, nil
}

// Accept records an event or identifies an identical retry. The supplied
// timestamp is part of the deterministic contract and must not be zero.
func (deduplicator *WebhookEventDeduplicator) Accept(source, eventID string, payload []byte, now time.Time) (WebhookEventDecision, error) {
	if deduplicator == nil {
		return WebhookEventAccepted, ErrWebhookEventInvalid
	}
	if err := deduplicator.validateEvent(source, eventID, payload, now); err != nil {
		return WebhookEventAccepted, err
	}
	nowUnix := now.UnixNano()
	fingerprint := sha256.Sum256(payload)
	key := webhookEventKey{source: source, eventID: eventID}

	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	if record, exists := deduplicator.entries[key]; exists {
		if record.expiresAt > nowUnix {
			if record.fingerprint != fingerprint {
				return WebhookEventAccepted, ErrWebhookEventConflict
			}
			return WebhookEventDuplicate, nil
		}
		delete(deduplicator.entries, key)
	}
	if len(deduplicator.entries) >= deduplicator.capacity {
		deduplicator.pruneLocked(nowUnix)
		if len(deduplicator.entries) >= deduplicator.capacity {
			return WebhookEventAccepted, ErrWebhookEventCapacity
		}
	}
	expiresAt := now.Add(deduplicator.ttl).UnixNano()
	if expiresAt <= nowUnix {
		return WebhookEventAccepted, ErrWebhookEventInvalid
	}
	deduplicator.entries[key] = webhookEventRecord{fingerprint: fingerprint, expiresAt: expiresAt}
	return WebhookEventAccepted, nil
}

// Prune removes entries expired at or before now and returns the number
// removed.
func (deduplicator *WebhookEventDeduplicator) Prune(now time.Time) int {
	if deduplicator == nil || now.IsZero() {
		return 0
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	return deduplicator.pruneLocked(now.UnixNano())
}

// Len returns the number of retained live or not-yet-pruned identities.
func (deduplicator *WebhookEventDeduplicator) Len() int {
	if deduplicator == nil {
		return 0
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	return len(deduplicator.entries)
}

// Snapshot returns the deterministic HWE1 binary state. It contains event
// identities, expiry timestamps, and fingerprints, never payload bytes.
func (deduplicator *WebhookEventDeduplicator) Snapshot() ([]byte, error) {
	if deduplicator == nil {
		return nil, ErrWebhookEventInvalid
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	keys := make([]webhookEventKey, 0, len(deduplicator.entries))
	for key := range deduplicator.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].source == keys[right].source {
			return keys[left].eventID < keys[right].eventID
		}
		return keys[left].source < keys[right].source
	})
	encoded := make([]byte, 0, len(webhookEventDeduplicatorSnapshotMagic)+1+binary.MaxVarintLen64+len(keys)*64)
	encoded = append(encoded, webhookEventDeduplicatorSnapshotMagic...)
	encoded = append(encoded, webhookEventDeduplicatorSnapshotVersion)
	encoded = appendWebhookEventUvarint(encoded, uint64(len(keys)))
	for _, key := range keys {
		record := deduplicator.entries[key]
		encoded = appendWebhookEventString(encoded, key.source)
		encoded = appendWebhookEventString(encoded, key.eventID)
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], uint64(record.expiresAt))
		encoded = append(encoded, number[:]...)
		encoded = append(encoded, record.fingerprint[:]...)
		if len(encoded)+4 > deduplicator.maxSnapshotBytes {
			return nil, fmt.Errorf("%w: snapshot exceeds configured limit", ErrWebhookEventSnapshot)
		}
	}
	checksum := crc32.Checksum(encoded, crc32.MakeTable(crc32.Castagnoli))
	var checksumBytes [4]byte
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	if len(encoded) > deduplicator.maxSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds configured limit", ErrWebhookEventSnapshot)
	}
	return encoded, nil
}

// Restore atomically replaces the retained ledger with a validated HWE1
// snapshot. Expired records are discarded relative to now.
func (deduplicator *WebhookEventDeduplicator) Restore(snapshot []byte, now time.Time) error {
	if deduplicator == nil || now.IsZero() || len(snapshot) > deduplicator.maxSnapshotBytes || len(snapshot) < len(webhookEventDeduplicatorSnapshotMagic)+1+1+4 {
		return ErrWebhookEventSnapshot
	}
	body := snapshot[:len(snapshot)-4]
	expectedChecksum := binary.BigEndian.Uint32(snapshot[len(snapshot)-4:])
	actualChecksum := crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli))
	if expectedChecksum != actualChecksum || string(body[:len(webhookEventDeduplicatorSnapshotMagic)]) != webhookEventDeduplicatorSnapshotMagic || body[len(webhookEventDeduplicatorSnapshotMagic)] != webhookEventDeduplicatorSnapshotVersion {
		return ErrWebhookEventSnapshot
	}
	offset := len(webhookEventDeduplicatorSnapshotMagic) + 1
	count, ok := readWebhookEventUvarint(body, &offset)
	if !ok || count > uint64(deduplicator.capacity) {
		return ErrWebhookEventSnapshot
	}
	entries := make(map[webhookEventKey]webhookEventRecord, int(count))
	seen := make(map[webhookEventKey]struct{}, int(count))
	nowUnix := now.UnixNano()
	for index := uint64(0); index < count; index++ {
		source, ok := readWebhookEventString(body, &offset, deduplicator.maxSourceBytes)
		if !ok {
			return ErrWebhookEventSnapshot
		}
		eventID, ok := readWebhookEventString(body, &offset, deduplicator.maxEventIDBytes)
		if !ok {
			return ErrWebhookEventSnapshot
		}
		key := webhookEventKey{source: source, eventID: eventID}
		if _, duplicate := seen[key]; duplicate {
			return ErrWebhookEventSnapshot
		}
		seen[key] = struct{}{}
		if offset+8+webhookEventDeduplicatorFingerprintBytes > len(body) {
			return ErrWebhookEventSnapshot
		}
		expiresAt := int64(binary.BigEndian.Uint64(body[offset : offset+8]))
		offset += 8
		var fingerprint [webhookEventDeduplicatorFingerprintBytes]byte
		copy(fingerprint[:], body[offset:offset+webhookEventDeduplicatorFingerprintBytes])
		offset += webhookEventDeduplicatorFingerprintBytes
		if expiresAt > nowUnix {
			entries[key] = webhookEventRecord{fingerprint: fingerprint, expiresAt: expiresAt}
		}
	}
	if offset != len(body) {
		return ErrWebhookEventSnapshot
	}
	deduplicator.mu.Lock()
	deduplicator.entries = entries
	deduplicator.mu.Unlock()
	return nil
}

func (deduplicator *WebhookEventDeduplicator) validateEvent(source, eventID string, payload []byte, now time.Time) error {
	if now.IsZero() || source == "" || eventID == "" || len(source) > deduplicator.maxSourceBytes || len(eventID) > deduplicator.maxEventIDBytes || len(payload) > deduplicator.maxPayloadBytes || !utf8.ValidString(source) || !utf8.ValidString(eventID) {
		return ErrWebhookEventInvalid
	}
	return nil
}

func (deduplicator *WebhookEventDeduplicator) pruneLocked(nowUnix int64) int {
	removed := 0
	for key, record := range deduplicator.entries {
		if record.expiresAt <= nowUnix {
			delete(deduplicator.entries, key)
			removed++
		}
	}
	return removed
}

func appendWebhookEventUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:count]...)
}

func appendWebhookEventString(destination []byte, value string) []byte {
	destination = appendWebhookEventUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func readWebhookEventUvarint(source []byte, offset *int) (uint64, bool) {
	if *offset >= len(source) {
		return 0, false
	}
	value, count := binary.Uvarint(source[*offset:])
	if count <= 0 {
		return 0, false
	}
	*offset += count
	return value, true
}

func readWebhookEventString(source []byte, offset *int, maxBytes int) (string, bool) {
	length, ok := readWebhookEventUvarint(source, offset)
	if !ok || length == 0 || length > uint64(maxBytes) || length > uint64(len(source)-*offset) {
		return "", false
	}
	end := *offset + int(length)
	value := source[*offset:end]
	*offset = end
	if !utf8.Valid(value) {
		return "", false
	}
	return string(value), true
}
