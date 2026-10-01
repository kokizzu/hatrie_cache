package hatReplication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrSpaceChangefeedNil                      = errors.New("hatriecache: space changefeed is nil")
	ErrSpaceChangefeedClosed                   = errors.New("hatriecache: space changefeed is closed")
	ErrSpaceChangefeedOptionsInvalid           = errors.New("hatriecache: space changefeed options are invalid")
	ErrSpaceChangefeedSpaceRequired            = errors.New("hatriecache: space changefeed space is required")
	ErrSpaceChangefeedSchemaRequired           = errors.New("hatriecache: space changefeed schema version is required")
	ErrSpaceChangefeedChangeInvalid            = errors.New("hatriecache: space change is invalid")
	ErrSpaceChangefeedSchemaMismatch           = errors.New("hatriecache: space changefeed schema version mismatch")
	ErrSpaceChangefeedHistoryGap               = errors.New("hatriecache: space changefeed history gap")
	ErrSpaceChangefeedLimitInvalid             = errors.New("hatriecache: space changefeed replay limit is invalid")
	ErrSpaceChangefeedBatchInvalid             = errors.New("hatriecache: space changefeed batch is invalid")
	ErrSpaceChangefeedCheckpointSourceMismatch = errors.New("hatriecache: space changefeed checkpoint source mismatch")
	ErrSpaceChangefeedContextRequired          = errors.New("hatriecache: space changefeed wait context is required")
)

const (
	DefaultSpaceChangefeedCapacity       = 1024
	MaxSpaceChangefeedCapacity           = 65536
	DefaultSpaceChangefeedMaxChangeBytes = 1 << 20
	MaxSpaceChangefeedMaxChangeBytes     = 16 << 20
	MaxSpaceChangefeedBatch              = 1024
	MaxSpaceChangefeedSpaceBytes         = 256
	MaxSpaceChangefeedKeyBytes           = 64 << 10
	MaxSpaceChangefeedReplayBatch        = 1024
)

// SpaceChangeOperation describes the row transition carried by one change.
type SpaceChangeOperation string

const (
	SpaceChangeInsert SpaceChangeOperation = "insert"
	SpaceChangeUpdate SpaceChangeOperation = "update"
	SpaceChangeDelete SpaceChangeOperation = "delete"
)

// SpaceChangefeedOptions fixes the identity and bounds of one named-space
// feed. SchemaVersion changes require a new feed so consumers cannot silently
// decode payloads with an incompatible layout.
type SpaceChangefeedOptions struct {
	Space          string
	SchemaVersion  uint64
	Capacity       int
	MaxChangeBytes int
}

// SpaceChange is one copy-safe row transition. Key and Before/After contain
// caller-defined encoded tuples; the feed never interprets their format.
type SpaceChange struct {
	Sequence      uint64               `json:"sequence"`
	SchemaVersion uint64               `json:"schema_version"`
	Operation     SpaceChangeOperation `json:"operation"`
	Key           []byte               `json:"key"`
	Before        []byte               `json:"before,omitempty"`
	After         []byte               `json:"after,omitempty"`
}

// SpaceChangefeedPage is a bounded replay result. NextSequence can be used as
// the next cursor or persisted through ChangefeedCheckpoint.
type SpaceChangefeedPage struct {
	Space          string        `json:"space"`
	SchemaVersion  uint64        `json:"schema_version"`
	Events         []SpaceChange `json:"events"`
	NextSequence   uint64        `json:"next_sequence"`
	OldestSequence uint64        `json:"oldest_sequence"`
	NewestSequence uint64        `json:"newest_sequence"`
}

type spaceChangeRecord struct {
	sequence      uint64
	schemaVersion uint64
	operation     SpaceChangeOperation
	key           []byte
	before        []byte
	after         []byte
}

// SpaceChangefeed is an opt-in bounded, ordered feed for one named space. It
// does not attach to storage automatically; writers append already-encoded
// transitions after their write has committed.
type SpaceChangefeed struct {
	mu             sync.Mutex
	space          string
	schemaVersion  uint64
	capacity       int
	maxChangeBytes int
	events         []spaceChangeRecord
	start          int
	count          int
	nextSequence   uint64
	notify         chan struct{}
	closed         bool
}

// NewSpaceChangefeed creates a bounded named-space feed.
func NewSpaceChangefeed(options SpaceChangefeedOptions) (*SpaceChangefeed, error) {
	space := strings.TrimSpace(options.Space)
	if space == "" {
		return nil, ErrSpaceChangefeedSpaceRequired
	}
	if len(space) > MaxSpaceChangefeedSpaceBytes {
		return nil, fmt.Errorf("%w: space exceeds %d bytes", ErrSpaceChangefeedOptionsInvalid, MaxSpaceChangefeedSpaceBytes)
	}
	if options.SchemaVersion == 0 {
		return nil, ErrSpaceChangefeedSchemaRequired
	}
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultSpaceChangefeedCapacity
	}
	if capacity < 1 || capacity > MaxSpaceChangefeedCapacity {
		return nil, fmt.Errorf("%w: capacity must be 1..%d", ErrSpaceChangefeedOptionsInvalid, MaxSpaceChangefeedCapacity)
	}
	maxChangeBytes := options.MaxChangeBytes
	if maxChangeBytes == 0 {
		maxChangeBytes = DefaultSpaceChangefeedMaxChangeBytes
	}
	if maxChangeBytes < 1 || maxChangeBytes > MaxSpaceChangefeedMaxChangeBytes {
		return nil, fmt.Errorf("%w: max change bytes must be 1..%d", ErrSpaceChangefeedOptionsInvalid, MaxSpaceChangefeedMaxChangeBytes)
	}
	return &SpaceChangefeed{
		space:          space,
		schemaVersion:  options.SchemaVersion,
		capacity:       capacity,
		maxChangeBytes: maxChangeBytes,
		events:         make([]spaceChangeRecord, capacity),
		nextSequence:   1,
		notify:         make(chan struct{}),
	}, nil
}

// Space returns the normalized named-space identity.
func (feed *SpaceChangefeed) Space() string {
	if feed == nil {
		return ""
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.space
}

// SchemaVersion returns the immutable payload schema version.
func (feed *SpaceChangefeed) SchemaVersion() uint64 {
	if feed == nil {
		return 0
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	return feed.schemaVersion
}

// Append validates and publishes one committed row transition.
func (feed *SpaceChangefeed) Append(change SpaceChange) (SpaceChange, error) {
	if feed == nil {
		return SpaceChange{}, ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return SpaceChange{}, ErrSpaceChangefeedClosed
	}
	if feed.nextSequence == 0 {
		return SpaceChange{}, ErrSpaceChangefeedBatchInvalid
	}
	record, err := feed.validateAndCloneLocked(change)
	if err != nil {
		return SpaceChange{}, err
	}
	record.sequence = feed.nextSequence
	feed.insertLocked(record)
	feed.nextSequence++
	feed.signalLocked()
	return record.public(), nil
}

// AppendBatch validates every transition before publishing any of them. A
// malformed event never leaves a partially visible batch in the feed.
func (feed *SpaceChangefeed) AppendBatch(changes []SpaceChange) ([]SpaceChange, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedNil
	}
	if len(changes) == 0 {
		return nil, nil
	}
	if len(changes) > MaxSpaceChangefeedBatch {
		return nil, fmt.Errorf("%w: maximum batch size is %d", ErrSpaceChangefeedBatchInvalid, MaxSpaceChangefeedBatch)
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	if uint64(len(changes)) > ^uint64(0)-feed.nextSequence+1 {
		return nil, ErrSpaceChangefeedBatchInvalid
	}
	records := make([]spaceChangeRecord, len(changes))
	for index, change := range changes {
		record, err := feed.validateAndCloneLocked(change)
		if err != nil {
			return nil, err
		}
		record.sequence = feed.nextSequence + uint64(index)
		records[index] = record
	}
	for _, record := range records {
		feed.insertLocked(record)
	}
	feed.nextSequence += uint64(len(records))
	feed.signalLocked()
	result := make([]SpaceChange, len(records))
	for index, record := range records {
		result[index] = record.public()
	}
	return result, nil
}

func (feed *SpaceChangefeed) signalLocked() {
	oldNotify := feed.notify
	feed.notify = make(chan struct{})
	close(oldNotify)
}

func (feed *SpaceChangefeed) validateAndCloneLocked(change SpaceChange) (spaceChangeRecord, error) {
	if change.SchemaVersion != feed.schemaVersion {
		return spaceChangeRecord{}, fmt.Errorf("%w: feed=%d change=%d", ErrSpaceChangefeedSchemaMismatch, feed.schemaVersion, change.SchemaVersion)
	}
	if len(change.Key) == 0 || len(change.Key) > MaxSpaceChangefeedKeyBytes {
		return spaceChangeRecord{}, fmt.Errorf("%w: key is required and must be at most %d bytes", ErrSpaceChangefeedChangeInvalid, MaxSpaceChangefeedKeyBytes)
	}
	if len(change.Key)+len(change.Before)+len(change.After) > feed.maxChangeBytes {
		return spaceChangeRecord{}, fmt.Errorf("%w: payload exceeds %d bytes", ErrSpaceChangefeedChangeInvalid, feed.maxChangeBytes)
	}
	switch change.Operation {
	case SpaceChangeInsert:
		if change.Before != nil || change.After == nil {
			return spaceChangeRecord{}, fmt.Errorf("%w: insert requires after and no before", ErrSpaceChangefeedChangeInvalid)
		}
	case SpaceChangeUpdate:
		if change.Before == nil || change.After == nil {
			return spaceChangeRecord{}, fmt.Errorf("%w: update requires before and after", ErrSpaceChangefeedChangeInvalid)
		}
	case SpaceChangeDelete:
		if change.Before == nil || change.After != nil {
			return spaceChangeRecord{}, fmt.Errorf("%w: delete requires before and no after", ErrSpaceChangefeedChangeInvalid)
		}
	default:
		return spaceChangeRecord{}, fmt.Errorf("%w: unsupported operation %q", ErrSpaceChangefeedChangeInvalid, change.Operation)
	}
	return spaceChangeRecord{
		schemaVersion: change.SchemaVersion,
		operation:     change.Operation,
		key:           append([]byte(nil), change.Key...),
		before:        appendOptionalSpaceChangeBytes(change.Before),
		after:         appendOptionalSpaceChangeBytes(change.After),
	}, nil
}

func appendOptionalSpaceChangeBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

// ReadAfter returns at most limit events with sequence greater than after.
func (feed *SpaceChangefeed) ReadAfter(after uint64, limit int) (SpaceChangefeedPage, error) {
	if feed == nil {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedNil
	}
	if err := validateSpaceChangefeedLimit(limit); err != nil {
		return SpaceChangefeedPage{}, err
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedClosed
	}
	return feed.readAfterLocked(after, limit)
}

func validateSpaceChangefeedLimit(limit int) error {
	if limit < 1 || limit > MaxSpaceChangefeedReplayBatch {
		return fmt.Errorf("%w: limit must be 1..%d", ErrSpaceChangefeedLimitInvalid, MaxSpaceChangefeedReplayBatch)
	}
	return nil
}

func (feed *SpaceChangefeed) readAfterLocked(after uint64, limit int) (SpaceChangefeedPage, error) {
	page := SpaceChangefeedPage{
		Space:         feed.space,
		SchemaVersion: feed.schemaVersion,
		NextSequence:  after,
	}
	if feed.count == 0 {
		return page, nil
	}
	oldest := feed.events[feed.start].sequence
	newest := feed.nextSequence - 1
	page.OldestSequence = oldest
	page.NewestSequence = newest
	if after < oldest-1 {
		return SpaceChangefeedPage{}, fmt.Errorf("%w: after=%d oldest=%d", ErrSpaceChangefeedHistoryGap, after, oldest)
	}
	if after >= newest {
		return page, nil
	}
	available := newest - after
	if available > uint64(limit) {
		available = uint64(limit)
	}
	first := after + 1
	page.Events = make([]SpaceChange, 0, int(available))
	for sequence := first; sequence < first+available; sequence++ {
		offset := int(sequence - oldest)
		index := (feed.start + offset) % feed.capacity
		page.Events = append(page.Events, feed.events[index].public())
	}
	page.NextSequence = page.Events[len(page.Events)-1].Sequence
	return page, nil
}

// ReadFromCheckpoint validates the named-space binding before replaying.
func (feed *SpaceChangefeed) ReadFromCheckpoint(checkpoint ChangefeedCheckpoint, limit int) (SpaceChangefeedPage, error) {
	if feed == nil {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedNil
	}
	if strings.TrimSpace(checkpoint.Source) != feed.Space() {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedCheckpointSourceMismatch
	}
	return feed.ReadAfter(checkpoint.Sequence, limit)
}

// Checkpoint returns the last retained source sequence. Persist the returned
// checkpoint with ChangefeedCheckpoint.MarshalBinary after applying a page.
func (feed *SpaceChangefeed) Checkpoint() (ChangefeedCheckpoint, error) {
	if feed == nil {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return ChangefeedCheckpoint{}, ErrSpaceChangefeedClosed
	}
	return NewChangefeedCheckpoint(feed.space, feed.nextSequence-1)
}

// WaitAfter blocks until a retained event is available, the context ends, or
// the feed closes. It does not create a goroutine per idle consumer.
func (feed *SpaceChangefeed) WaitAfter(ctx context.Context, after uint64, limit int) (SpaceChangefeedPage, error) {
	if feed == nil {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedNil
	}
	if ctx == nil {
		return SpaceChangefeedPage{}, ErrSpaceChangefeedContextRequired
	}
	if err := validateSpaceChangefeedLimit(limit); err != nil {
		return SpaceChangefeedPage{}, err
	}
	for {
		feed.mu.Lock()
		if feed.closed {
			feed.mu.Unlock()
			return SpaceChangefeedPage{}, ErrSpaceChangefeedClosed
		}
		page, err := feed.readAfterLocked(after, limit)
		if err != nil || len(page.Events) != 0 {
			feed.mu.Unlock()
			return page, err
		}
		notify := feed.notify
		feed.mu.Unlock()
		select {
		case <-ctx.Done():
			return SpaceChangefeedPage{}, ctx.Err()
		case <-notify:
		}
	}
}

// Snapshot returns retained changes in sequence order.
func (feed *SpaceChangefeed) Snapshot() ([]SpaceChange, error) {
	if feed == nil {
		return nil, ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil, ErrSpaceChangefeedClosed
	}
	changes := make([]SpaceChange, 0, feed.count)
	for index := 0; index < feed.count; index++ {
		changes = append(changes, feed.events[(feed.start+index)%feed.capacity].public())
	}
	return changes, nil
}

// Close wakes waiters and rejects future appends and reads.
func (feed *SpaceChangefeed) Close() error {
	if feed == nil {
		return ErrSpaceChangefeedNil
	}
	feed.mu.Lock()
	defer feed.mu.Unlock()
	if feed.closed {
		return nil
	}
	feed.closed = true
	close(feed.notify)
	return nil
}

func (feed *SpaceChangefeed) insertLocked(record spaceChangeRecord) {
	if feed.count < feed.capacity {
		index := (feed.start + feed.count) % feed.capacity
		feed.events[index] = record
		feed.count++
		return
	}
	feed.events[feed.start] = record
	feed.start = (feed.start + 1) % feed.capacity
}

func (record spaceChangeRecord) public() SpaceChange {
	return SpaceChange{
		Sequence:      record.sequence,
		SchemaVersion: record.schemaVersion,
		Operation:     record.operation,
		Key:           append([]byte(nil), record.key...),
		Before:        appendOptionalSpaceChangeBytes(record.before),
		After:         appendOptionalSpaceChangeBytes(record.after),
	}
}
