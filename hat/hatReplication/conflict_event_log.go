package hatReplication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrConflictEventLogNil            = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventLogOptionsInvalid = errors.New("hatriecache: conflict event log options are invalid")
	ErrConflictEventInvalid           = errors.New("hatriecache: conflict event is invalid")
	ErrConflictEventCorrupt           = errors.New("hatriecache: conflict event snapshot is corrupt")
	ErrConflictEventGap               = errors.New("hatriecache: conflict event cursor is too old")
	ErrConflictEventLimit             = errors.New("hatriecache: conflict event page limit is invalid")
	ErrConflictEventStoreRequired     = errors.New("hatriecache: conflict event store is required")
)

const (
	DefaultConflictEventLogCapacity = 1024
	MaxConflictEventLogCapacity     = 4096
	DefaultConflictEventPageSize    = 128
	MaxConflictEventPageSize        = 512
	MaxConflictEventSpaceBytes      = 256
	MaxConflictEventSourceBytes     = 256
	MaxConflictEventEncodedBytes    = 1 << 20
	conflictEventKeyDigestBytes     = 16
	conflictEventFormatVersion      = 1
	conflictEventHeaderBytes        = 4 + 1 + 3 + 8 + 8 + 4
	conflictEventRecordBytes        = 8 + 1 + 2 + 2 + 2 + conflictEventKeyDigestBytes
)

var conflictEventCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

var conflictEventMagic = [4]byte{'H', 'C', 'E', '1'}

// ConflictEventDecision identifies which side of a conflict won or whether
// the configured policy rejected the write.
type ConflictEventDecision uint8

const (
	ConflictEventDecisionInvalid ConflictEventDecision = iota
	ConflictEventDecisionLeft
	ConflictEventDecisionRight
	ConflictEventDecisionRejected
)

func (decision ConflictEventDecision) String() string {
	switch decision {
	case ConflictEventDecisionLeft:
		return "left"
	case ConflictEventDecisionRight:
		return "right"
	case ConflictEventDecisionRejected:
		return "rejected"
	default:
		return "invalid"
	}
}

// ConflictEvent is one redacted conflict decision. KeyDigest is the first
// 128 bits of SHA-256(key); the raw key is never retained by this package.
type ConflictEvent struct {
	Sequence    uint64
	Space       string
	KeyDigest   [conflictEventKeyDigestBytes]byte
	LeftSource  string
	RightSource string
	Decision    ConflictEventDecision
}

// ConflictEventSnapshot is the bounded persisted state of a conflict log.
// Events are strictly ordered by Sequence.
type ConflictEventSnapshot struct {
	NextSequence uint64
	Dropped      uint64
	Events       []ConflictEvent
}

// ConflictEventPage is one cursor page. Pass NextSequence to Since for the
// next page. A gap is reported when the requested cursor predates retention.
type ConflictEventPage struct {
	Events           []ConflictEvent
	NextSequence     uint64
	EarliestSequence uint64
	LatestSequence   uint64
	Dropped          uint64
}

// ConflictEventStore persists a complete bounded snapshot. Implementations
// must make Save atomic from the caller's point of view.
type ConflictEventStore interface {
	Load() ([]byte, error)
	Save([]byte) error
}

// ConflictEventLogOptions configures an in-memory or persisted conflict log.
// A nil Store keeps the log in memory only.
type ConflictEventLogOptions struct {
	Capacity int
	Store    ConflictEventStore
}

// ConflictEventLog is a bounded, concurrency-safe, redacted conflict stream.
// Record serializes persistence with event publication, so a successful event
// is visible only after its optional store accepts the new snapshot.
type ConflictEventLog struct {
	mu           sync.RWMutex
	capacity     int
	store        ConflictEventStore
	nextSequence uint64
	dropped      uint64
	events       []ConflictEvent
}

// NewConflictEventLog creates a bounded event log and restores its optional
// store. A missing file is treated as an empty log; corrupt data is rejected.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictEventLogCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return nil, ErrConflictEventLogOptionsInvalid
	}
	log := &ConflictEventLog{capacity: capacity, store: options.Store, events: make([]ConflictEvent, 0, capacity)}
	if options.Store == nil {
		return log, nil
	}
	data, err := options.Store.Load()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return log, nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return log, nil
	}
	snapshot, err := DecodeConflictEventSnapshot(data)
	if err != nil {
		return nil, err
	}
	if len(snapshot.Events) > capacity {
		trim := len(snapshot.Events) - capacity
		snapshot.Events = snapshot.Events[trim:]
		snapshot.Dropped += uint64(trim)
	}
	log.nextSequence = snapshot.NextSequence
	log.dropped = snapshot.Dropped
	log.events = make([]ConflictEvent, len(snapshot.Events), capacity)
	copy(log.events, snapshot.Events)
	return log, nil
}

// Record appends a redacted conflict decision. The source versions must be
// valid and decision must be one of the exported decision constants.
func (log *ConflictEventLog) Record(space string, key []byte, left, right ConflictVersion, decision ConflictEventDecision) (ConflictEvent, error) {
	if log == nil {
		return ConflictEvent{}, ErrConflictEventLogNil
	}
	space, err := normalizeConflictEventSpace(space)
	if err != nil {
		return ConflictEvent{}, err
	}
	if err := validateConflictEventSource(left.NodeID); err != nil {
		return ConflictEvent{}, err
	}
	if err := validateConflictEventSource(right.NodeID); err != nil {
		return ConflictEvent{}, err
	}
	if _, err := CompareConflictVersions(left, right); err != nil {
		return ConflictEvent{}, err
	}
	if !validConflictEventDecision(decision) {
		return ConflictEvent{}, ErrConflictEventInvalid
	}
	digest := sha256.Sum256(key)
	event := ConflictEvent{
		Space:       space,
		LeftSource:  left.NodeID,
		RightSource: right.NodeID,
		Decision:    decision,
	}
	copy(event.KeyDigest[:], digest[:conflictEventKeyDigestBytes])

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.capacity == 0 {
		log.capacity = DefaultConflictEventLogCapacity
	}
	if log.capacity < 1 || log.capacity > MaxConflictEventLogCapacity || log.nextSequence == ^uint64(0) {
		return ConflictEvent{}, ErrConflictEventLogOptionsInvalid
	}
	previousEvents := []ConflictEvent(nil)
	if log.store != nil {
		previousEvents = append(previousEvents, log.events...)
	}
	previousSequence, previousDropped := log.nextSequence, log.dropped
	event.Sequence = previousSequence + 1
	log.nextSequence = event.Sequence
	if len(log.events) == log.capacity {
		copy(log.events, log.events[1:])
		log.events[len(log.events)-1] = event
		log.dropped++
	} else {
		log.events = append(log.events, event)
	}
	if err := log.persistLocked(); err != nil {
		log.nextSequence = previousSequence
		log.dropped = previousDropped
		if previousEvents != nil {
			log.events = previousEvents
		} else {
			log.events = log.events[:len(log.events)-1]
		}
		return ConflictEvent{}, err
	}
	return event, nil
}

// Since returns events with sequence greater than after. It returns a gap when
// retention has already evicted a required event, allowing consumers to seek
// from a fresh snapshot instead of silently skipping conflicts.
func (log *ConflictEventLog) Since(after uint64, limit int) (ConflictEventPage, error) {
	if log == nil {
		return ConflictEventPage{}, ErrConflictEventLogNil
	}
	if limit == 0 {
		limit = DefaultConflictEventPageSize
	}
	if limit < 1 || limit > MaxConflictEventPageSize {
		return ConflictEventPage{}, ErrConflictEventLimit
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	page := ConflictEventPage{NextSequence: after, Dropped: log.dropped}
	if len(log.events) == 0 {
		return page, nil
	}
	page.EarliestSequence = log.events[0].Sequence
	page.LatestSequence = log.events[len(log.events)-1].Sequence
	if page.EarliestSequence > 0 && after < page.EarliestSequence-1 {
		return ConflictEventPage{}, ErrConflictEventGap
	}
	start := 0
	for start < len(log.events) && log.events[start].Sequence <= after {
		start++
	}
	end := start + limit
	if end > len(log.events) {
		end = len(log.events)
	}
	page.Events = append([]ConflictEvent(nil), log.events[start:end]...)
	if len(page.Events) > 0 {
		page.NextSequence = page.Events[len(page.Events)-1].Sequence
	}
	return page, nil
}

// Snapshot returns an independent copy of the retained event window.
func (log *ConflictEventLog) Snapshot() ConflictEventSnapshot {
	if log == nil {
		return ConflictEventSnapshot{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return ConflictEventSnapshot{
		NextSequence: log.nextSequence,
		Dropped:      log.dropped,
		Events:       append([]ConflictEvent(nil), log.events...),
	}
}

// ResolveAndRecord applies the policy and records every non-equal conflict.
// Rejected conflicts are recorded before ErrConflictRejected is returned.
func (registry *ConflictPolicyRegistry) ResolveAndRecord(log *ConflictEventLog, space string, key []byte, left, right ConflictVersion) (ConflictVersion, error) {
	if log == nil {
		return ConflictVersion{}, ErrConflictEventLogNil
	}
	comparison, err := CompareConflictVersions(left, right)
	if err != nil {
		return ConflictVersion{}, err
	}
	winner, resolveErr := registry.Resolve(space, left, right)
	if resolveErr != nil {
		if errors.Is(resolveErr, ErrConflictRejected) && comparison != 0 {
			if _, recordErr := log.Record(space, key, left, right, ConflictEventDecisionRejected); recordErr != nil {
				return ConflictVersion{}, fmt.Errorf("%w: record conflict event: %v", resolveErr, recordErr)
			}
		}
		return ConflictVersion{}, resolveErr
	}
	if comparison == 0 {
		return winner, nil
	}
	decision := ConflictEventDecisionRight
	if winner == left {
		decision = ConflictEventDecisionLeft
	}
	if _, err := log.Record(space, key, left, right, decision); err != nil {
		return ConflictVersion{}, err
	}
	return winner, nil
}

// EncodeConflictEventSnapshot serializes a bounded HCE1 snapshot with a
// CRC32C trailer. It contains only digests and source identifiers, never keys.
func EncodeConflictEventSnapshot(snapshot ConflictEventSnapshot) ([]byte, error) {
	if err := validateConflictEventSnapshot(snapshot); err != nil {
		return nil, err
	}
	capacity := conflictEventHeaderBytes + 4
	for _, event := range snapshot.Events {
		capacity += conflictEventRecordBytes + len(event.Space) + len(event.LeftSource) + len(event.RightSource)
	}
	if capacity > MaxConflictEventEncodedBytes {
		return nil, ErrConflictEventLimit
	}
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, conflictEventMagic[:]...)
	encoded = append(encoded, conflictEventFormatVersion, 0, 0, 0)
	var numeric [8]byte
	binary.LittleEndian.PutUint64(numeric[:], snapshot.NextSequence)
	encoded = append(encoded, numeric[:]...)
	binary.LittleEndian.PutUint64(numeric[:], snapshot.Dropped)
	encoded = append(encoded, numeric[:]...)
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(snapshot.Events)))
	encoded = append(encoded, count[:]...)
	for _, event := range snapshot.Events {
		var fixed [conflictEventRecordBytes]byte
		binary.LittleEndian.PutUint64(fixed[0:8], event.Sequence)
		fixed[8] = byte(event.Decision)
		binary.LittleEndian.PutUint16(fixed[9:11], uint16(len(event.Space)))
		binary.LittleEndian.PutUint16(fixed[11:13], uint16(len(event.LeftSource)))
		binary.LittleEndian.PutUint16(fixed[13:15], uint16(len(event.RightSource)))
		copy(fixed[15:], event.KeyDigest[:])
		encoded = append(encoded, fixed[:]...)
		encoded = append(encoded, event.Space...)
		encoded = append(encoded, event.LeftSource...)
		encoded = append(encoded, event.RightSource...)
	}
	checksum := crc32.Checksum(encoded, conflictEventCRC32CTable)
	binary.LittleEndian.PutUint32(count[:], checksum)
	encoded = append(encoded, count[:]...)
	return encoded, nil
}

// DecodeConflictEventSnapshot validates and decodes one HCE1 snapshot without
// allocating based on untrusted lengths beyond the one-megabyte image bound.
func DecodeConflictEventSnapshot(encoded []byte) (ConflictEventSnapshot, error) {
	if len(encoded) < conflictEventHeaderBytes+4 || len(encoded) > MaxConflictEventEncodedBytes {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	if !bytes.Equal(encoded[:4], conflictEventMagic[:]) || encoded[4] != conflictEventFormatVersion {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	storedChecksum := binary.LittleEndian.Uint32(encoded[len(encoded)-4:])
	if crc32.Checksum(encoded[:len(encoded)-4], conflictEventCRC32CTable) != storedChecksum {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	offset := 8
	nextSequence := binary.LittleEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	dropped := binary.LittleEndian.Uint64(encoded[offset : offset+8])
	offset += 8
	count := binary.LittleEndian.Uint32(encoded[offset : offset+4])
	offset += 4
	if count > MaxConflictEventLogCapacity {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	if uint64(count)*conflictEventRecordBytes > uint64(len(encoded)-offset-4) {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	events := make([]ConflictEvent, 0, count)
	var previous uint64
	for index := uint32(0); index < count; index++ {
		if offset+conflictEventRecordBytes > len(encoded)-4 {
			return ConflictEventSnapshot{}, ErrConflictEventCorrupt
		}
		fixed := encoded[offset : offset+conflictEventRecordBytes]
		offset += conflictEventRecordBytes
		sequence := binary.LittleEndian.Uint64(fixed[:8])
		spaceBytes := int(binary.LittleEndian.Uint16(fixed[9:11]))
		leftBytes := int(binary.LittleEndian.Uint16(fixed[11:13]))
		rightBytes := int(binary.LittleEndian.Uint16(fixed[13:15]))
		if sequence == 0 || (index > 0 && sequence <= previous) || sequence > nextSequence || spaceBytes > MaxConflictEventSpaceBytes || leftBytes > MaxConflictEventSourceBytes || rightBytes > MaxConflictEventSourceBytes {
			return ConflictEventSnapshot{}, ErrConflictEventCorrupt
		}
		total := spaceBytes + leftBytes + rightBytes
		if total < 0 || offset+total > len(encoded)-4 {
			return ConflictEventSnapshot{}, ErrConflictEventCorrupt
		}
		event := ConflictEvent{Sequence: sequence, Decision: ConflictEventDecision(fixed[8])}
		copy(event.KeyDigest[:], fixed[15:])
		event.Space = string(encoded[offset : offset+spaceBytes])
		offset += spaceBytes
		event.LeftSource = string(encoded[offset : offset+leftBytes])
		offset += leftBytes
		event.RightSource = string(encoded[offset : offset+rightBytes])
		offset += rightBytes
		if err := validateConflictEventFields(event); err != nil {
			return ConflictEventSnapshot{}, ErrConflictEventCorrupt
		}
		events = append(events, event)
		previous = sequence
	}
	if offset != len(encoded)-4 {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	snapshot := ConflictEventSnapshot{NextSequence: nextSequence, Dropped: dropped, Events: events}
	if err := validateConflictEventSnapshot(snapshot); err != nil {
		return ConflictEventSnapshot{}, ErrConflictEventCorrupt
	}
	return snapshot, nil
}

// FileConflictEventStore atomically persists HCE1 snapshots with private file
// permissions. The parent directory must already exist.
type FileConflictEventStore struct {
	path string
}

// NewFileConflictEventStore validates a file-backed event store path.
func NewFileConflictEventStore(path string) (*FileConflictEventStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, ErrConflictEventStoreRequired
	}
	return &FileConflictEventStore{path: path}, nil
}

// Load reads one persisted snapshot.
func (store *FileConflictEventStore) Load() ([]byte, error) {
	if store == nil || store.path == "" {
		return nil, ErrConflictEventStoreRequired
	}
	data, err := os.ReadFile(store.path)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxConflictEventEncodedBytes {
		return nil, ErrConflictEventCorrupt
	}
	return data, nil
}

// Save writes one snapshot through a private temporary file and rename.
func (store *FileConflictEventStore) Save(data []byte) error {
	if store == nil || store.path == "" {
		return ErrConflictEventStoreRequired
	}
	if len(data) == 0 || len(data) > MaxConflictEventEncodedBytes {
		return ErrConflictEventLimit
	}
	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, ".hce-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, store.path); err != nil {
		return err
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}

func (log *ConflictEventLog) persistLocked() error {
	if log.store == nil {
		return nil
	}
	encoded, err := EncodeConflictEventSnapshot(ConflictEventSnapshot{NextSequence: log.nextSequence, Dropped: log.dropped, Events: log.events})
	if err != nil {
		return err
	}
	return log.store.Save(encoded)
}

func validateConflictEventSnapshot(snapshot ConflictEventSnapshot) error {
	if len(snapshot.Events) > MaxConflictEventLogCapacity {
		return ErrConflictEventCorrupt
	}
	var previous uint64
	for index, event := range snapshot.Events {
		if event.Sequence == 0 || event.Sequence <= previous || event.Sequence > snapshot.NextSequence {
			return ErrConflictEventCorrupt
		}
		if index == 0 {
			previous = event.Sequence
		} else {
			previous = event.Sequence
		}
		if err := validateConflictEventFields(event); err != nil {
			return ErrConflictEventCorrupt
		}
	}
	if len(snapshot.Events) == 0 && snapshot.NextSequence != 0 && snapshot.Dropped == 0 {
		return ErrConflictEventCorrupt
	}
	return nil
}

func validateConflictEventFields(event ConflictEvent) error {
	if event.Space == "" || strings.TrimSpace(event.Space) != event.Space || len(event.Space) > MaxConflictEventSpaceBytes || strings.IndexByte(event.Space, 0) >= 0 {
		return ErrConflictEventInvalid
	}
	if err := validateConflictEventSource(event.LeftSource); err != nil {
		return err
	}
	if err := validateConflictEventSource(event.RightSource); err != nil {
		return err
	}
	if !validConflictEventDecision(event.Decision) {
		return ErrConflictEventInvalid
	}
	return nil
}

func validateConflictEventSource(source string) error {
	if source == "" || len(source) > MaxConflictEventSourceBytes || strings.IndexByte(source, 0) >= 0 {
		return ErrConflictEventInvalid
	}
	return nil
}

func normalizeConflictEventSpace(space string) (string, error) {
	if space == "" || strings.TrimSpace(space) != space || len(space) > MaxConflictEventSpaceBytes || strings.IndexByte(space, 0) >= 0 {
		return "", ErrConflictEventInvalid
	}
	return space, nil
}

func validConflictEventDecision(decision ConflictEventDecision) bool {
	return decision >= ConflictEventDecisionLeft && decision <= ConflictEventDecisionRejected
}
