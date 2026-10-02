package hatReplication

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	ErrConflictEventLogInvalid   = errors.New("hatriecache: conflict event log is invalid")
	ErrConflictEventLogNil       = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventHistoryGap   = errors.New("hatriecache: conflict event history gap")
	ErrConflictEventSequenceFull = errors.New("hatriecache: conflict event sequence exhausted")
)

const (
	DefaultConflictEventLogCapacity = 1024
	MaxConflictEventLogCapacity     = 4096
	DefaultConflictEventReadLimit   = 256
	MaxConflictEventReadLimit       = 1024
	MaxConflictEventSpaceBytes      = 256
	MaxConflictEventSourceBytes     = 256
	MaxConflictEventSaltBytes       = 64
	MaxConflictEventKeyBytes        = 1 << 20
	MaxConflictEventLogBytes        = 8 << 20
	conflictEventSnapshotHeaderSize = 4 + 2 + 2 + 2 + 4 + 8
	conflictEventSnapshotVersion    = uint16(1)
)

var conflictEventSnapshotMagic = [4]byte{'c', 'e', 'l', '1'}

// ConflictEventDecision identifies which side a policy selected.
type ConflictEventDecision string

const (
	ConflictEventDecisionLeft     ConflictEventDecision = "left"
	ConflictEventDecisionRight    ConflictEventDecision = "right"
	ConflictEventDecisionRejected ConflictEventDecision = "rejected"
)

// ConflictEventLogOptions bounds a redacted conflict history. A zero capacity
// selects DefaultConflictEventLogCapacity. HashSalt is copied and included in
// binary snapshots so a restored log produces the same key digests.
type ConflictEventLogOptions struct {
	Capacity int
	HashSalt []byte
}

// ConflictEventRecord is the caller-owned resolution input. Key is hashed and
// never retained by the log.
type ConflictEventRecord struct {
	Space    string
	Key      string
	Left     ConflictVersion
	Right    ConflictVersion
	Winner   ConflictVersion
	Decision ConflictEventDecision
}

// ConflictEvent is the redacted, streamable form of one conflict. KeyDigest
// is a hex-encoded SHA-256 digest of HashSalt followed by the key.
type ConflictEvent struct {
	Sequence     uint64                `json:"sequence"`
	Space        string                `json:"space"`
	KeyDigest    string                `json:"key_digest"`
	LeftSource   string                `json:"left_source"`
	RightSource  string                `json:"right_source"`
	WinnerSource string                `json:"winner_source,omitempty"`
	Decision     ConflictEventDecision `json:"decision"`
}

// ConflictEventPage is one bounded cursor read. Pass NextSequence as after to
// continue. A nonzero cursor older than the retained ring returns
// ErrConflictEventHistoryGap.
type ConflictEventPage struct {
	Events         []ConflictEvent `json:"events"`
	OldestSequence uint64          `json:"oldest_sequence"`
	LatestSequence uint64          `json:"latest_sequence"`
	NextSequence   uint64          `json:"next_sequence"`
}

type conflictEventEntry struct {
	sequence     uint64
	space        string
	keyDigest    [sha256.Size]byte
	leftSource   string
	rightSource  string
	winnerSource string
	decision     ConflictEventDecision
}

// ConflictEventLog is a bounded, concurrency-safe redacted conflict stream.
// It has no connection to normal policy resolution unless callers use
// ResolveAndRecord explicitly.
type ConflictEventLog struct {
	mu           sync.RWMutex
	capacity     int
	hashSalt     []byte
	events       []conflictEventEntry
	start        int
	count        int
	nextSequence uint64
}

// NewConflictEventLog creates a bounded in-memory conflict stream.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictEventLogCapacity
	}
	if capacity < 1 || capacity > MaxConflictEventLogCapacity || len(options.HashSalt) > MaxConflictEventSaltBytes {
		return nil, ErrConflictEventLogInvalid
	}
	return &ConflictEventLog{
		capacity: capacity,
		hashSalt: append([]byte(nil), options.HashSalt...),
		events:   make([]conflictEventEntry, capacity),
	}, nil
}

// Append records one redacted conflict and evicts the oldest entry when the
// configured capacity is full.
func (log *ConflictEventLog) Append(record ConflictEventRecord) (ConflictEvent, error) {
	if log == nil {
		return ConflictEvent{}, ErrConflictEventLogNil
	}
	normalized, err := normalizeConflictEventRecord(record)
	if err != nil {
		return ConflictEvent{}, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextSequence == ^uint64(0) {
		return ConflictEvent{}, ErrConflictEventSequenceFull
	}
	log.nextSequence++
	entry := conflictEventEntry{
		sequence:     log.nextSequence,
		space:        normalized.Space,
		keyDigest:    conflictEventDigest(log.hashSalt, normalized.Key),
		leftSource:   normalized.Left.NodeID,
		rightSource:  normalized.Right.NodeID,
		winnerSource: normalized.Winner.NodeID,
		decision:     normalized.Decision,
	}
	log.appendEntryLocked(entry)
	return conflictEventPublic(entry), nil
}

// Read returns up to limit events strictly after after. A zero limit selects
// DefaultConflictEventReadLimit.
func (log *ConflictEventLog) Read(after uint64, limit int) (ConflictEventPage, error) {
	if log == nil {
		return ConflictEventPage{}, ErrConflictEventLogNil
	}
	if limit == 0 {
		limit = DefaultConflictEventReadLimit
	}
	if limit < 0 || limit > MaxConflictEventReadLimit {
		return ConflictEventPage{}, ErrConflictEventLogInvalid
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	page := ConflictEventPage{NextSequence: after}
	if log.count == 0 {
		return page, nil
	}
	oldest := log.events[log.start].sequence
	latest := log.events[(log.start+log.count-1)%log.capacity].sequence
	page.OldestSequence = oldest
	page.LatestSequence = latest
	if after != 0 && after < oldest-1 {
		return ConflictEventPage{}, ErrConflictEventHistoryGap
	}
	if after >= latest {
		return page, nil
	}
	first := oldest
	if after >= oldest {
		first = after + 1
	}
	available := int(latest-first) + 1
	if available > limit {
		available = limit
	}
	page.Events = make([]ConflictEvent, available)
	offset := int(first - oldest)
	for index := 0; index < available; index++ {
		entry := log.events[(log.start+offset+index)%log.capacity]
		page.Events[index] = conflictEventPublic(entry)
	}
	page.NextSequence = page.Events[len(page.Events)-1].Sequence
	return page, nil
}

// Len returns the number of retained events.
func (log *ConflictEventLog) Len() int {
	if log == nil {
		return 0
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.count
}

// MarshalBinary encodes the retained redacted stream as a bounded deterministic
// CRC-protected snapshot.
func (log *ConflictEventLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.capacity < 1 || log.capacity > MaxConflictEventLogCapacity || len(log.hashSalt) > MaxConflictEventSaltBytes || log.count > log.capacity {
		return nil, ErrConflictEventLogInvalid
	}
	encoded := make([]byte, 0, conflictEventSnapshotHeaderSize+len(log.hashSalt)+log.count*128+crc32.Size)
	encoded = append(encoded, conflictEventSnapshotMagic[:]...)
	encoded = appendU16(encoded, conflictEventSnapshotVersion)
	encoded = appendU16(encoded, uint16(log.capacity))
	encoded = appendU16(encoded, uint16(len(log.hashSalt)))
	encoded = appendU32(encoded, uint32(log.count))
	encoded = appendU64(encoded, log.nextSequence)
	encoded = append(encoded, log.hashSalt...)
	for index := 0; index < log.count; index++ {
		entry := log.events[(log.start+index)%log.capacity]
		encoded = appendU64(encoded, entry.sequence)
		encoded = append(encoded, conflictEventDecisionByte(entry.decision))
		var ok bool
		encoded, ok = appendConflictEventString(encoded, entry.space, MaxConflictEventSpaceBytes)
		if !ok {
			return nil, ErrConflictEventLogInvalid
		}
		encoded = append(encoded, entry.keyDigest[:]...)
		encoded, ok = appendConflictEventString(encoded, entry.leftSource, MaxConflictEventSourceBytes)
		if !ok {
			return nil, ErrConflictEventLogInvalid
		}
		encoded, ok = appendConflictEventString(encoded, entry.rightSource, MaxConflictEventSourceBytes)
		if !ok {
			return nil, ErrConflictEventLogInvalid
		}
		encoded, ok = appendConflictEventString(encoded, entry.winnerSource, MaxConflictEventSourceBytes)
		if !ok {
			return nil, ErrConflictEventLogInvalid
		}
	}
	if len(encoded)+crc32.Size > MaxConflictEventLogBytes {
		return nil, ErrConflictEventLogInvalid
	}
	return appendU32(encoded, crc32.Checksum(encoded, crc32.MakeTable(crc32.Castagnoli))), nil
}

// UnmarshalConflictEventLog restores a validated redacted stream snapshot.
func UnmarshalConflictEventLog(encoded []byte) (*ConflictEventLog, error) {
	if len(encoded) < conflictEventSnapshotHeaderSize+crc32.Size || len(encoded) > MaxConflictEventLogBytes {
		return nil, ErrConflictEventLogInvalid
	}
	if !bytesEqual(encoded[:len(conflictEventSnapshotMagic)], conflictEventSnapshotMagic[:]) {
		return nil, ErrConflictEventLogInvalid
	}
	checksumOffset := len(encoded) - crc32.Size
	if binary.BigEndian.Uint32(encoded[checksumOffset:]) != crc32.Checksum(encoded[:checksumOffset], crc32.MakeTable(crc32.Castagnoli)) {
		return nil, ErrConflictEventLogInvalid
	}
	offset := len(conflictEventSnapshotMagic)
	version, ok := readU16(encoded, &offset)
	if !ok || version != conflictEventSnapshotVersion {
		return nil, ErrConflictEventLogInvalid
	}
	capacity, ok := readU16(encoded, &offset)
	if !ok || capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return nil, ErrConflictEventLogInvalid
	}
	saltLength, ok := readU16(encoded, &offset)
	if !ok || saltLength > MaxConflictEventSaltBytes || offset+int(saltLength) > checksumOffset {
		return nil, ErrConflictEventLogInvalid
	}
	count, ok := readU32(encoded, &offset)
	if !ok || count > uint32(capacity) {
		return nil, ErrConflictEventLogInvalid
	}
	nextSequence, ok := readU64(encoded, &offset)
	if !ok {
		return nil, ErrConflictEventLogInvalid
	}
	hashSalt := append([]byte(nil), encoded[offset:offset+int(saltLength)]...)
	offset += int(saltLength)
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: int(capacity), HashSalt: hashSalt})
	if err != nil {
		return nil, err
	}
	var previous uint64
	for index := uint32(0); index < count; index++ {
		sequence, valid := readU64(encoded, &offset)
		if !valid || sequence == 0 || (index > 0 && sequence != previous+1) {
			return nil, ErrConflictEventLogInvalid
		}
		previous = sequence
		if offset >= checksumOffset {
			return nil, ErrConflictEventLogInvalid
		}
		decision, valid := conflictEventDecisionFromByte(encoded[offset])
		if !valid {
			return nil, ErrConflictEventLogInvalid
		}
		offset++
		space, valid := readConflictEventString(encoded, &offset, checksumOffset, MaxConflictEventSpaceBytes)
		if !valid || offset+sha256.Size > checksumOffset {
			return nil, ErrConflictEventLogInvalid
		}
		var digest [sha256.Size]byte
		copy(digest[:], encoded[offset:offset+sha256.Size])
		offset += sha256.Size
		leftSource, valid := readConflictEventString(encoded, &offset, checksumOffset, MaxConflictEventSourceBytes)
		if !valid {
			return nil, ErrConflictEventLogInvalid
		}
		rightSource, valid := readConflictEventString(encoded, &offset, checksumOffset, MaxConflictEventSourceBytes)
		if !valid {
			return nil, ErrConflictEventLogInvalid
		}
		winnerSource, valid := readConflictEventString(encoded, &offset, checksumOffset, MaxConflictEventSourceBytes)
		if !valid {
			return nil, ErrConflictEventLogInvalid
		}
		log.appendEntryLocked(conflictEventEntry{
			sequence:     sequence,
			space:        space,
			keyDigest:    digest,
			leftSource:   leftSource,
			rightSource:  rightSource,
			winnerSource: winnerSource,
			decision:     decision,
		})
	}
	if offset != checksumOffset || (count == 0 && nextSequence != 0) || (count != 0 && nextSequence != previous) {
		return nil, ErrConflictEventLogInvalid
	}
	log.nextSequence = nextSequence
	return log, nil
}

// ResolveAndRecord resolves a conflict and records its redacted outcome. A
// rejected conflict is retained before ErrConflictRejected is returned.
func (registry *ConflictPolicyRegistry) ResolveAndRecord(log *ConflictEventLog, space, key string, left, right ConflictVersion) (ConflictVersion, error) {
	if log == nil {
		return ConflictVersion{}, ErrConflictEventLogNil
	}
	winner, resolveErr := registry.Resolve(space, left, right)
	if resolveErr != nil && !errors.Is(resolveErr, ErrConflictRejected) {
		return ConflictVersion{}, resolveErr
	}
	decision := ConflictEventDecisionRejected
	if resolveErr == nil {
		comparison, err := CompareConflictVersions(left, right)
		if err != nil {
			return ConflictVersion{}, err
		}
		if comparison < 0 {
			decision = ConflictEventDecisionRight
		} else {
			decision = ConflictEventDecisionLeft
		}
	}
	_, recordErr := log.Append(ConflictEventRecord{
		Space: space, Key: key, Left: left, Right: right, Winner: winner, Decision: decision,
	})
	if recordErr != nil {
		return winner, recordErr
	}
	return winner, resolveErr
}

func normalizeConflictEventRecord(record ConflictEventRecord) (ConflictEventRecord, error) {
	record.Space = strings.TrimSpace(record.Space)
	if record.Space == "" || len(record.Space) > MaxConflictEventSpaceBytes || !utf8.ValidString(record.Space) || len(record.Key) > MaxConflictEventKeyBytes {
		return ConflictEventRecord{}, ErrConflictEventLogInvalid
	}
	if err := validateConflictEventSource(record.Left.NodeID); err != nil {
		return ConflictEventRecord{}, err
	}
	if err := validateConflictEventSource(record.Right.NodeID); err != nil {
		return ConflictEventRecord{}, err
	}
	if _, err := CompareConflictVersions(record.Left, record.Right); err != nil {
		return ConflictEventRecord{}, ErrConflictEventLogInvalid
	}
	switch record.Decision {
	case ConflictEventDecisionLeft:
		if !sameConflictVersion(record.Winner, record.Left) {
			return ConflictEventRecord{}, ErrConflictEventLogInvalid
		}
	case ConflictEventDecisionRight:
		if !sameConflictVersion(record.Winner, record.Right) {
			return ConflictEventRecord{}, ErrConflictEventLogInvalid
		}
	case ConflictEventDecisionRejected:
		if record.Winner.NodeID != "" || record.Winner.Timestamp != 0 || record.Winner.Sequence != 0 {
			return ConflictEventRecord{}, ErrConflictEventLogInvalid
		}
	default:
		return ConflictEventRecord{}, ErrConflictEventLogInvalid
	}
	return record, nil
}

func validateConflictEventSource(source string) error {
	if source == "" || len(source) > MaxConflictEventSourceBytes || !utf8.ValidString(source) {
		return ErrConflictEventLogInvalid
	}
	return nil
}

func sameConflictVersion(left, right ConflictVersion) bool {
	return left.Timestamp == right.Timestamp && left.NodeID == right.NodeID && left.Sequence == right.Sequence
}

func conflictEventDigest(salt []byte, key string) (digest [sha256.Size]byte) {
	hash := sha256.New()
	_, _ = hash.Write(salt)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(key))
	copy(digest[:], hash.Sum(nil))
	return digest
}

func conflictEventPublic(entry conflictEventEntry) ConflictEvent {
	return ConflictEvent{
		Sequence:     entry.sequence,
		Space:        entry.space,
		KeyDigest:    hex.EncodeToString(entry.keyDigest[:]),
		LeftSource:   entry.leftSource,
		RightSource:  entry.rightSource,
		WinnerSource: entry.winnerSource,
		Decision:     entry.decision,
	}
}

func (log *ConflictEventLog) appendEntryLocked(entry conflictEventEntry) {
	index := (log.start + log.count) % log.capacity
	if log.count == log.capacity {
		index = log.start
		log.start = (log.start + 1) % log.capacity
	} else {
		log.count++
	}
	log.events[index] = entry
}

func conflictEventDecisionByte(decision ConflictEventDecision) byte {
	switch decision {
	case ConflictEventDecisionLeft:
		return 1
	case ConflictEventDecisionRight:
		return 2
	case ConflictEventDecisionRejected:
		return 3
	default:
		return 0
	}
}

func conflictEventDecisionFromByte(value byte) (ConflictEventDecision, bool) {
	switch value {
	case 1:
		return ConflictEventDecisionLeft, true
	case 2:
		return ConflictEventDecisionRight, true
	case 3:
		return ConflictEventDecisionRejected, true
	default:
		return "", false
	}
}

func appendU16(payload []byte, value uint16) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendU32(payload []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(payload, encoded[:]...)
}

func appendU64(payload []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(payload, encoded[:]...)
}

func readU16(payload []byte, offset *int) (uint16, bool) {
	if offset == nil || *offset < 0 || *offset+2 > len(payload) {
		return 0, false
	}
	value := binary.BigEndian.Uint16(payload[*offset : *offset+2])
	*offset += 2
	return value, true
}

func readU32(payload []byte, offset *int) (uint32, bool) {
	if offset == nil || *offset < 0 || *offset+4 > len(payload) {
		return 0, false
	}
	value := binary.BigEndian.Uint32(payload[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readU64(payload []byte, offset *int) (uint64, bool) {
	if offset == nil || *offset < 0 || *offset+8 > len(payload) {
		return 0, false
	}
	value := binary.BigEndian.Uint64(payload[*offset : *offset+8])
	*offset += 8
	return value, true
}

func appendConflictEventString(payload []byte, value string, maximum int) ([]byte, bool) {
	if len(value) > maximum || len(value) > int(^uint16(0)) || !utf8.ValidString(value) {
		return payload, false
	}
	payload = appendU16(payload, uint16(len(value)))
	return append(payload, value...), true
}

func readConflictEventString(payload []byte, offset *int, end, maximum int) (string, bool) {
	length, ok := readU16(payload, offset)
	if !ok || length > uint16(maximum) || *offset+int(length) > end {
		return "", false
	}
	value := string(payload[*offset : *offset+int(length)])
	*offset += int(length)
	if !utf8.ValidString(value) {
		return "", false
	}
	return value, true
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
