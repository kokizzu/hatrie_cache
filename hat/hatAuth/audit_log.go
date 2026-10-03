package hatAuth

import (
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultAuditLogCapacity is used when NewAuditLog receives zero.
	DefaultAuditLogCapacity = 1024
	// MaxAuditLogCapacity prevents accidental unbounded in-process audit state.
	MaxAuditLogCapacity = 1 << 20
	// MaxAuditMetadataFields bounds per-event metadata retained by the log.
	MaxAuditMetadataFields = 8
	// MaxAuditFieldBytes bounds command and identity metadata fields.
	MaxAuditFieldBytes = 256
	// MaxAuditReasonCodeBytes bounds stable caller-owned reason codes.
	MaxAuditReasonCodeBytes = 64
	// AuditRedactedValue is stored for known credential-bearing metadata keys.
	AuditRedactedValue = "<redacted>"
)

var (
	// ErrAuditLogCapacityInvalid indicates an unsupported bounded log size.
	ErrAuditLogCapacityInvalid = errors.New("hatauth: audit log capacity is invalid")
	// ErrAuditCommandRequired indicates that an event has no command name.
	ErrAuditCommandRequired = errors.New("hatauth: audit command is required")
	// ErrAuditOutcomeInvalid indicates an unsupported event outcome.
	ErrAuditOutcomeInvalid = errors.New("hatauth: audit outcome is invalid")
	// ErrAuditFieldInvalid indicates invalid or control-bearing metadata.
	ErrAuditFieldInvalid = errors.New("hatauth: audit field is invalid")
	// ErrAuditMetadataLimit indicates too many metadata fields were supplied.
	ErrAuditMetadataLimit = errors.New("hatauth: audit metadata limit exceeded")
	// ErrAuditSequenceExhausted indicates that the monotone event sequence cannot advance.
	ErrAuditSequenceExhausted = errors.New("hatauth: audit sequence exhausted")
)

// AuditOutcome describes the terminal result of an audited operation.
type AuditOutcome string

const (
	AuditOutcomeAllowed AuditOutcome = "allowed"
	AuditOutcomeDenied  AuditOutcome = "denied"
	AuditOutcomeError   AuditOutcome = "error"
	AuditOutcomeInvalid AuditOutcome = ""
)

// AuditMetadata is a bounded key/value field. Values for credential-bearing
// keys are replaced with AuditRedactedValue before they enter the log.
type AuditMetadata struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// AuditRecord is the caller-owned safe command metadata submitted to Append.
// It intentionally has no command payload, raw SQL, token, or error field.
type AuditRecord struct {
	Principal string          `json:"principal,omitempty"`
	Command   string          `json:"command"`
	Namespace string          `json:"namespace,omitempty"`
	Source    string          `json:"source,omitempty"`
	Resource  string          `json:"resource,omitempty"`
	Outcome   AuditOutcome    `json:"outcome"`
	Reason    string          `json:"reason,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Metadata  []AuditMetadata `json:"metadata,omitempty"`
}

// AuditEvent is the immutable normalized event retained by AuditLog.
type AuditEvent struct {
	Sequence  uint64          `json:"sequence"`
	At        time.Time       `json:"at"`
	Principal string          `json:"principal,omitempty"`
	Command   string          `json:"command"`
	Namespace string          `json:"namespace,omitempty"`
	Source    string          `json:"source,omitempty"`
	Resource  string          `json:"resource,omitempty"`
	Outcome   AuditOutcome    `json:"outcome"`
	Reason    string          `json:"reason,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Metadata  []AuditMetadata `json:"metadata,omitempty"`
}

// AuditLogOptions configures a bounded in-process audit stream.
type AuditLogOptions struct {
	Capacity int
	Now      func() time.Time
}

// AuditLogStats describes retained and overwritten event sequence state.
type AuditLogStats struct {
	Capacity      int
	Entries       int
	FirstSequence uint64
	LastSequence  uint64
	Dropped       uint64
}

// AuditLog is a concurrency-safe bounded append-only event stream. Once the
// capacity is reached, the oldest event is overwritten and Dropped increases.
// Callers that need durability should drain Since into their durable sink.
type AuditLog struct {
	mu           sync.Mutex
	capacity     int
	now          func() time.Time
	entries      []AuditEvent
	start        int
	size         int
	nextSequence uint64
	dropped      uint64
}

// NewAuditLog creates a bounded audit stream. A zero capacity selects
// DefaultAuditLogCapacity; negative or oversized capacities are rejected.
func NewAuditLog(capacity int) (*AuditLog, error) {
	return NewAuditLogWithOptions(AuditLogOptions{Capacity: capacity})
}

// NewAuditLogWithOptions creates a bounded audit stream with an injectable
// clock for deterministic callers and tests.
func NewAuditLogWithOptions(options AuditLogOptions) (*AuditLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultAuditLogCapacity
	}
	if capacity < 0 || capacity > MaxAuditLogCapacity {
		return nil, ErrAuditLogCapacityInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &AuditLog{
		capacity: capacity,
		now:      now,
		entries:  make([]AuditEvent, capacity),
	}, nil
}

// Append validates, normalizes, and retains one event. The returned event is
// independent from the log's internal metadata storage.
func (log *AuditLog) Append(record AuditRecord) (AuditEvent, error) {
	var zero AuditEvent
	normalized, err := normalizeAuditRecord(record)
	if err != nil {
		return zero, err
	}
	if log == nil {
		return zero, ErrAuditLogCapacityInvalid
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	log.initializeLocked()
	if log.nextSequence == ^uint64(0) {
		return zero, ErrAuditSequenceExhausted
	}
	sequence := log.nextSequence + 1
	event := AuditEvent{
		Sequence:  sequence,
		At:        log.now().UTC(),
		Principal: normalized.Principal,
		Command:   normalized.Command,
		Namespace: normalized.Namespace,
		Source:    normalized.Source,
		Resource:  normalized.Resource,
		Outcome:   normalized.Outcome,
		Reason:    normalized.Reason,
		RequestID: normalized.RequestID,
		Metadata:  normalized.Metadata,
	}
	index := (log.start + log.size) % log.capacity
	if log.size == log.capacity {
		index = log.start
		log.start = (log.start + 1) % log.capacity
		log.dropped++
	} else {
		log.size++
	}
	log.entries[index] = event
	log.nextSequence = sequence
	return cloneAuditEvent(event), nil
}

// Snapshot returns retained events in sequence order with independent metadata
// slices. It returns nil when the stream is empty.
func (log *AuditLog) Snapshot() []AuditEvent {
	events, _ := log.Since(0, nil)
	return events
}

// Since returns retained events newer than afterSequence. The boolean is true
// when the requested sequence predates the oldest retained event.
func (log *AuditLog) Since(afterSequence uint64, dst []AuditEvent) (events []AuditEvent, truncated bool) {
	if log == nil {
		return dst, false
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.size == 0 {
		return dst, false
	}
	first := log.entries[log.start].Sequence
	if first > 0 && afterSequence < first-1 {
		truncated = true
	}
	if cap(dst)-len(dst) < log.size {
		grown := make([]AuditEvent, len(dst), len(dst)+log.size)
		copy(grown, dst)
		dst = grown
	}
	events = dst
	for offset := 0; offset < log.size; offset++ {
		event := log.entries[(log.start+offset)%log.capacity]
		if event.Sequence > afterSequence {
			events = append(events, cloneAuditEvent(event))
		}
	}
	return events, truncated
}

// Stats returns a point-in-time retention snapshot.
func (log *AuditLog) Stats() AuditLogStats {
	if log == nil {
		return AuditLogStats{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	stats := AuditLogStats{Capacity: log.capacity, Entries: log.size, Dropped: log.dropped}
	if log.size != 0 {
		stats.FirstSequence = log.entries[log.start].Sequence
		stats.LastSequence = log.entries[(log.start+log.size-1)%log.capacity].Sequence
	}
	return stats
}

func (log *AuditLog) initializeLocked() {
	if log.capacity != 0 {
		return
	}
	log.capacity = DefaultAuditLogCapacity
	log.now = time.Now
	log.entries = make([]AuditEvent, log.capacity)
}

func normalizeAuditRecord(record AuditRecord) (AuditEvent, error) {
	command, err := normalizeAuditText("command", record.Command, MaxAuditFieldBytes, true)
	if err != nil {
		if strings.TrimSpace(record.Command) == "" {
			return AuditEvent{}, ErrAuditCommandRequired
		}
		return AuditEvent{}, err
	}
	if strings.ContainsAny(command, " \t") {
		return AuditEvent{}, ErrAuditFieldInvalid
	}
	if record.Outcome != AuditOutcomeAllowed && record.Outcome != AuditOutcomeDenied && record.Outcome != AuditOutcomeError {
		return AuditEvent{}, ErrAuditOutcomeInvalid
	}
	principal, err := normalizeAuditText("principal", record.Principal, MaxAuditFieldBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	if len(principal) >= len("Bearer ") && strings.EqualFold(principal[:len("Bearer ")], "Bearer ") {
		principal = AuditRedactedValue
	}
	namespace, err := normalizeAuditText("namespace", record.Namespace, MaxAuditFieldBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	source, err := normalizeAuditText("source", record.Source, MaxAuditFieldBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	resource, err := normalizeAuditText("resource", record.Resource, MaxAuditFieldBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	reason, err := normalizeAuditText("reason", record.Reason, MaxAuditReasonCodeBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	if strings.ContainsAny(reason, " \t") {
		return AuditEvent{}, ErrAuditFieldInvalid
	}
	requestID, err := normalizeAuditText("request_id", record.RequestID, MaxAuditFieldBytes, false)
	if err != nil {
		return AuditEvent{}, err
	}
	metadata, err := normalizeAuditMetadata(record.Metadata)
	if err != nil {
		return AuditEvent{}, err
	}
	return AuditEvent{
		Principal: principal,
		Command:   command,
		Namespace: namespace,
		Source:    source,
		Resource:  resource,
		Outcome:   record.Outcome,
		Reason:    reason,
		RequestID: requestID,
		Metadata:  metadata,
	}, nil
}

func normalizeAuditMetadata(metadata []AuditMetadata) ([]AuditMetadata, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if len(metadata) > MaxAuditMetadataFields {
		return nil, ErrAuditMetadataLimit
	}
	normalized := make([]AuditMetadata, len(metadata))
	for index, field := range metadata {
		key, err := normalizeAuditText("metadata key", field.Key, MaxAuditFieldBytes, true)
		if err != nil {
			return nil, err
		}
		key = strings.ToLower(key)
		value := AuditRedactedValue
		if !auditSensitiveKey(key) {
			value, err = normalizeAuditText("metadata value", field.Value, MaxAuditFieldBytes, false)
			if err != nil {
				return nil, err
			}
		}
		normalized[index] = AuditMetadata{Key: key, Value: value}
	}
	return normalized, nil
}

func normalizeAuditText(_ string, value string, maxBytes int, required bool) (string, error) {
	if len(value) > maxBytes || !utf8.ValidString(value) {
		return "", ErrAuditFieldInvalid
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] == 0x7f {
			return "", ErrAuditFieldInvalid
		}
	}
	value = strings.TrimSpace(value)
	if required && value == "" {
		return "", ErrAuditFieldInvalid
	}
	return value, nil
}

func auditSensitiveKey(key string) bool {
	for _, sensitive := range []string{"authorization", "password", "passwd", "secret", "token", "credential", "api_key", "api-key", "api.key", "access_key", "access-key", "access.key"} {
		if strings.Contains(key, sensitive) {
			return true
		}
	}
	return false
}

func cloneAuditEvent(event AuditEvent) AuditEvent {
	if len(event.Metadata) != 0 {
		event.Metadata = append([]AuditMetadata(nil), event.Metadata...)
	}
	return event
}
