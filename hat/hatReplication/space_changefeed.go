package hatReplication

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	DefaultSpaceChangefeedMaxEvents = 4096
	DefaultSpaceChangefeedMaxBytes  = 4 << 20
)

var (
	ErrSpaceChangefeedNil              = errors.New("hatriecache: space changefeed is nil")
	ErrSpaceChangefeedOptionsInvalid   = errors.New("hatriecache: space changefeed options are invalid")
	ErrSpaceChangefeedSpaceRequired    = errors.New("hatriecache: space changefeed space is required")
	ErrSpaceChangefeedOperationInvalid = errors.New("hatriecache: space changefeed operation is invalid")
	ErrSpaceChangefeedKeyRequired      = errors.New("hatriecache: space changefeed key is required")
	ErrSpaceChangefeedEventTooLarge    = errors.New("hatriecache: space changefeed event is too large")
	ErrSpaceChangefeedFull             = errors.New("hatriecache: space changefeed is full")
	ErrSpaceChangefeedCheckpointSource = errors.New("hatriecache: space changefeed checkpoint source mismatch")
	ErrSpaceChangefeedCheckpointFuture = errors.New("hatriecache: space changefeed checkpoint is ahead of the feed")
	ErrSpaceChangefeedCompacted        = errors.New("hatriecache: space changefeed checkpoint was compacted")
	ErrSpaceChangefeedLimitInvalid     = errors.New("hatriecache: space changefeed read limit is invalid")
)

// SpaceChangeOperation describes the mutation represented by one event.
type SpaceChangeOperation uint8

const (
	SpaceChangeInsert SpaceChangeOperation = iota + 1
	SpaceChangeUpdate
	SpaceChangeDelete
)

func (operation SpaceChangeOperation) valid() bool {
	return operation >= SpaceChangeInsert && operation <= SpaceChangeDelete
}

// SpaceChange is an owned event returned by a named-space changefeed. The
// immutable space and schema version are available from SpaceChangefeed and
// are intentionally not repeated per event. Key and Value are copies owned by
// the caller and are safe to mutate after reading.
type SpaceChange struct {
	Sequence  uint64               `json:"sequence"`
	Operation SpaceChangeOperation `json:"operation"`
	Key       []byte               `json:"key"`
	Value     []byte               `json:"value,omitempty"`
}

// SpaceChangefeedOptions configures a bounded, named-space changefeed.
// Zero MaxEvents, MaxBytes, and SchemaVersion values select sane defaults.
type SpaceChangefeedOptions struct {
	Space         string
	SchemaVersion uint64
	MaxEvents     int
	MaxBytes      int
}

type spaceChangefeedRecord struct {
	sequence  uint64
	operation SpaceChangeOperation
	keyLength uint32
	payload   []byte
}

// SpaceChangefeedStats reports the retained portion of a bounded feed.
type SpaceChangefeedStats struct {
	Space            string
	SchemaVersion    uint64
	RetainedEvents   int
	RetainedBytes    int
	MaxEvents        int
	MaxBytes         int
	CompactedThrough uint64
	LatestSequence   uint64
}

// SpaceChangefeed is a bounded, ordered log for one named space. Publishing
// never evicts unread events: a full feed returns ErrSpaceChangefeedFull until
// a consumer acknowledges progress with CompactThrough.
type SpaceChangefeed struct {
	mu sync.Mutex

	space         string
	schemaVersion uint64
	maxEvents     int
	maxBytes      int
	records       []spaceChangefeedRecord
	head          int
	count         int
	retainedBytes int

	compactedThrough uint64
	latestSequence   uint64
}

// NewSpaceChangefeed creates a bounded feed for one named space.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space := strings.TrimSpace(options.Space)
	if space == "" {
		return nil, ErrSpaceChangefeedSpaceRequired
	}
	if len(space) > MaxChangefeedCheckpointSourceBytes {
		return nil, fmt.Errorf("%w: space exceeds %d bytes", ErrSpaceChangefeedOptionsInvalid, MaxChangefeedCheckpointSourceBytes)
	}
	if options.MaxEvents < 0 || options.MaxBytes < 0 {
		return nil, ErrSpaceChangefeedOptionsInvalid
	}
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultSpaceChangefeedMaxEvents
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultSpaceChangefeedMaxBytes
	}
	schemaVersion := options.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = 1
	}
	return &SpaceChangefeed{
		space:         space,
		schemaVersion: schemaVersion,
		maxEvents:     maxEvents,
		maxBytes:      maxBytes,
		records:       make([]spaceChangefeedRecord, maxEvents),
	}, nil
}

// Space returns the immutable name bound to the feed.
func (feed *SpaceChangefeed) Space() string {
	if feed == nil {
		return ""
	}
	return feed.space
}

// SchemaVersion returns the immutable schema version bound to the feed.
func (feed *SpaceChangefeed) SchemaVersion() uint64 {
	if feed == nil {
		return 0
	}
	return feed.schemaVersion
}

// InitialCheckpoint returns the position before the first event.
func (feed *SpaceChangefeed) InitialCheckpoint() ChangefeedCheckpoint {
	if feed == nil {
		return ChangefeedCheckpoint{}
	}
	return ChangefeedCheckpoint{Source: feed.space}
}

// LatestCheckpoint returns the greatest published sequence, or sequence zero
// when no event has been published.
func (feed *SpaceChangefeed) LatestCheckpoint() ChangefeedCheckpoint {
	if feed == nil {
		return ChangefeedCheckpoint{}
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.checkpointLocked(feed.latestSequence)
}

// Publish appends one event and returns its durable consumer checkpoint. Key
// and Value are copied before the method returns. The method is deliberately
// non-blocking: callers should compact acknowledged progress and retry after
// ErrSpaceChangefeedFull.
func (feed *SpaceChangefeed) Publish(operation SpaceChangeOperation, key, value []byte) (ChangefeedCheckpoint, error) {
	if feed == nil {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedNil
	}
	if !operation.valid() {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedOperationInvalid
	}
	if len(key) == 0 {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedKeyRequired
	}
	if uint64(len(key)) > uint64(^uint32(0)) {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedEventTooLarge
	}
	payloadBytes := len(key) + len(value)
	if payloadBytes < len(key) || payloadBytes > feed.maxBytes {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedEventTooLarge
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.count == feed.maxEvents || payloadBytes > feed.maxBytes-feed.retainedBytes {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedFull
	}
	if feed.latestSequence == ^uint64(0) {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedOptionsInvalid
	}
	payload := make([]byte, payloadBytes)
	copy(payload, key)
	copy(payload[len(key):], value)
	sequence := feed.latestSequence + 1
	slot := (feed.head + feed.count) % feed.maxEvents
	feed.records[slot] = spaceChangefeedRecord{
		sequence:  sequence,
		operation: operation,
		keyLength: uint32(len(key)),
		payload:   payload,
	}
	feed.count++
	feed.retainedBytes += payloadBytes
	feed.latestSequence = sequence
	return feed.checkpointLocked(sequence), nil
}

// ReadAfter returns at most limit events after checkpoint and the checkpoint
// that covers the returned prefix. A compacted checkpoint must be recovered
// from a snapshot before the consumer can resume.
func (feed *SpaceChangefeed) ReadAfter(checkpoint ChangefeedCheckpoint, limit int) ([]SpaceChange, ChangefeedCheckpoint, error) {
	if feed == nil {
		return nil, ChangefeedCheckpoint{}, ErrSpaceChangefeedNil
	}
	if limit <= 0 {
		return nil, ChangefeedCheckpoint{}, ErrSpaceChangefeedLimitInvalid
	}

	feed.mu.Lock()
	defer feed.mu.Unlock()
	if err := feed.validateCheckpointLocked(&checkpoint); err != nil {
		return nil, ChangefeedCheckpoint{}, err
	}
	if checkpoint.Sequence == feed.latestSequence || feed.count == 0 {
		return []SpaceChange{}, checkpoint, nil
	}
	oldest := feed.records[feed.head].sequence
	if checkpoint.Sequence < oldest && oldest-checkpoint.Sequence > 1 {
		return nil, ChangefeedCheckpoint{}, ErrSpaceChangefeedCompacted
	}
	offset := int(checkpoint.Sequence + 1 - oldest)
	if offset < 0 || offset >= feed.count {
		return []SpaceChange{}, checkpoint, nil
	}
	available := feed.count - offset
	if available > limit {
		available = limit
	}
	changes := make([]SpaceChange, available)
	payloadBytes := 0
	for index := 0; index < available; index++ {
		record := feed.records[(feed.head+offset+index)%feed.maxEvents]
		payloadBytes += len(record.payload)
	}
	ownedPayload := make([]byte, payloadBytes)
	payloadOffset := 0
	nextSequence := checkpoint.Sequence
	for index := 0; index < available; index++ {
		record := feed.records[(feed.head+offset+index)%feed.maxEvents]
		payload := ownedPayload[payloadOffset : payloadOffset+len(record.payload)]
		copy(payload, record.payload)
		payloadOffset += len(record.payload)
		keyLength := int(record.keyLength)
		key := payload[:keyLength:keyLength]
		var value []byte
		if len(payload) > keyLength {
			value = payload[keyLength:len(payload):len(payload)]
		}
		changes[index] = SpaceChange{
			Sequence:  record.sequence,
			Operation: record.operation,
			Key:       key,
			Value:     value,
		}
		nextSequence = record.sequence
	}
	return changes, feed.checkpointLocked(nextSequence), nil
}

// CompactThrough releases retained events through checkpoint. It is
// idempotent for already-compacted progress and rejects future progress.
func (feed *SpaceChangefeed) CompactThrough(checkpoint ChangefeedCheckpoint) error {
	if feed == nil {
		return ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if err := feed.validateCheckpointLocked(&checkpoint); err != nil {
		if errors.Is(err, ErrSpaceChangefeedCompacted) && checkpoint.Sequence <= feed.compactedThrough {
			return nil
		}
		return err
	}
	if checkpoint.Sequence <= feed.compactedThrough {
		return nil
	}
	for feed.count > 0 {
		record := &feed.records[feed.head]
		if record.sequence > checkpoint.Sequence {
			break
		}
		feed.retainedBytes -= len(record.payload)
		*record = spaceChangefeedRecord{}
		feed.head = (feed.head + 1) % feed.maxEvents
		feed.count--
	}
	feed.compactedThrough = checkpoint.Sequence
	return nil
}

// Stats returns retained capacity and sequence boundaries for operations and
// observability. A nil feed returns the zero value.
func (feed *SpaceChangefeed) Stats() SpaceChangefeedStats {
	if feed == nil {
		return SpaceChangefeedStats{}
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return SpaceChangefeedStats{
		Space:            feed.space,
		SchemaVersion:    feed.schemaVersion,
		RetainedEvents:   feed.count,
		RetainedBytes:    feed.retainedBytes,
		MaxEvents:        feed.maxEvents,
		MaxBytes:         feed.maxBytes,
		CompactedThrough: feed.compactedThrough,
		LatestSequence:   feed.latestSequence,
	}
}

func (feed *SpaceChangefeed) checkpointLocked(sequence uint64) ChangefeedCheckpoint {
	return ChangefeedCheckpoint{Source: feed.space, Sequence: sequence}
}

func (feed *SpaceChangefeed) validateCheckpointLocked(checkpoint *ChangefeedCheckpoint) error {
	if checkpoint.Source == "" && checkpoint.Sequence == 0 {
		*checkpoint = feed.checkpointLocked(0)
	}
	if checkpoint.Source != feed.space {
		return ErrSpaceChangefeedCheckpointSource
	}
	if checkpoint.Sequence > feed.latestSequence {
		return ErrSpaceChangefeedCheckpointFuture
	}
	if checkpoint.Sequence < feed.compactedThrough {
		return ErrSpaceChangefeedCompacted
	}
	return nil
}
