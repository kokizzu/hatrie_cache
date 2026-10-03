package hatReplication

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
)

var (
	ErrConflictEventLogInvalid    = errors.New("hatriecache: conflict event log is invalid")
	ErrConflictEventLogCorrupt    = errors.New("hatriecache: conflict event log snapshot is corrupt")
	ErrConflictEventLogTooLarge   = errors.New("hatriecache: conflict event exceeds log byte budget")
	ErrConflictEventCursorExpired = errors.New("hatriecache: conflict event cursor is expired")
)

const (
	ConflictEventResolved ConflictEventDecision = iota + 1
	ConflictEventRejected
)

const (
	// DefaultConflictEventLogCapacity is a conservative example capacity for
	// callers that want a small diagnostic window.
	DefaultConflictEventLogCapacity = 1024
	// DefaultConflictEventLogMaxBytes bounds the corresponding example window.
	DefaultConflictEventLogMaxBytes = 256 << 10

	conflictEventLogMagic              = "HCE1"
	conflictEventLogVersion            = byte(1)
	conflictEventLogHashKeyBytes       = 32
	conflictEventLogKeyDigestBytes     = 16
	conflictEventLogKeyFingerprintSize = 8
	conflictEventLogMinBytes           = 256
	conflictEventLogMaxCapacity        = 1 << 20
	conflictEventMaxSpaceBytes         = 256
	conflictEventMaxNodeIDBytes        = 256
	conflictEventFixedBytes            = 82
	conflictEventDigestDomain          = "hatrie/conflict-event/v1"
)

var conflictEventLogCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

// ConflictEventDecision identifies the outcome recorded for a conflict.
type ConflictEventDecision uint8

// ConflictEvent is a redacted conflict decision. KeyDigest is a keyed digest;
// the original key is never retained or written to a snapshot.
type ConflictEvent struct {
	Sequence  uint64
	Space     string
	KeyDigest [conflictEventLogKeyDigestBytes]byte
	Left      ConflictVersion
	Right     ConflictVersion
	Winner    ConflictVersion
	Policy    ConflictPolicyMode
	Decision  ConflictEventDecision
}

// ConflictEventLogOptions bounds the in-memory diagnostic stream. HashKey must
// be kept by the caller and supplied again when restoring a snapshot.
type ConflictEventLogOptions struct {
	Capacity int
	MaxBytes int
	HashKey  []byte
}

// ConflictEventLogPage is one ordered cursor read from a conflict event log.
type ConflictEventLogPage struct {
	Events         []ConflictEvent
	OldestSequence uint64
	NewestSequence uint64
	NextSequence   uint64
	More           bool
}

// ConflictEventLog is an opt-in bounded, replayable conflict diagnostic log.
// It does not participate in conflict resolution unless a caller records the
// decision explicitly, so the default write path has no additional work.
type ConflictEventLog struct {
	mu                 sync.RWMutex
	capacity           int
	maxBytes           int
	hashKey            [conflictEventLogHashKeyBytes]byte
	hashKeyFingerprint [conflictEventLogKeyFingerprintSize]byte
	hashInnerPad       [64]byte
	hashOuterPad       [64]byte
	events             []ConflictEvent
	head               int
	count              int
	bytes              int
	nextSequence       uint64
}

// NewConflictEventLog creates an empty bounded conflict event log.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	if options.Capacity <= 0 || options.Capacity > conflictEventLogMaxCapacity || options.MaxBytes < conflictEventLogMinBytes || options.MaxBytes > 1<<30 || len(options.HashKey) < 16 {
		return nil, ErrConflictEventLogInvalid
	}
	key := sha256.Sum256(options.HashKey)
	fingerprint := sha256.Sum256(key[:])
	log := &ConflictEventLog{
		capacity:     options.Capacity,
		maxBytes:     options.MaxBytes,
		events:       make([]ConflictEvent, options.Capacity),
		nextSequence: 1,
	}
	copy(log.hashKey[:], key[:])
	copy(log.hashKeyFingerprint[:], fingerprint[:conflictEventLogKeyFingerprintSize])
	for index := range log.hashInnerPad {
		keyByte := byte(0)
		if index < conflictEventLogHashKeyBytes {
			keyByte = log.hashKey[index]
		}
		log.hashInnerPad[index] = keyByte ^ 0x36
		log.hashOuterPad[index] = keyByte ^ 0x5c
	}
	return log, nil
}

// NewConflictEventLogFromBinary restores a bounded log from MarshalBinary.
func NewConflictEventLogFromBinary(data []byte, options ConflictEventLogOptions) (*ConflictEventLog, error) {
	log, err := NewConflictEventLog(options)
	if err != nil {
		return nil, err
	}
	if err := log.RestoreBinary(data); err != nil {
		return nil, err
	}
	return log, nil
}

// Record appends a redacted conflict decision and returns the retained event.
func (log *ConflictEventLog) Record(space string, key []byte, left, right ConflictVersion, policy ConflictPolicyMode, winner ConflictVersion, decision ConflictEventDecision) (ConflictEvent, error) {
	if log == nil || space == "" || len(space) > conflictEventMaxSpaceBytes || !validConflictPolicyMode(policy) || !validConflictEventDecision(decision) {
		return ConflictEvent{}, ErrConflictEventLogInvalid
	}
	if _, err := CompareConflictVersions(left, right); err != nil {
		return ConflictEvent{}, ErrConflictEventLogInvalid
	}
	if decision == ConflictEventResolved {
		if _, err := CompareConflictVersions(winner, left); err != nil && !sameConflictVersion(winner, right) {
			return ConflictEvent{}, ErrConflictEventLogInvalid
		}
		if !sameConflictVersion(winner, left) && !sameConflictVersion(winner, right) {
			return ConflictEvent{}, ErrConflictEventLogInvalid
		}
	} else if winner != (ConflictVersion{}) {
		return ConflictEvent{}, ErrConflictEventLogInvalid
	}
	if len(left.NodeID) > conflictEventMaxNodeIDBytes || len(right.NodeID) > conflictEventMaxNodeIDBytes || len(winner.NodeID) > conflictEventMaxNodeIDBytes {
		return ConflictEvent{}, ErrConflictEventLogInvalid
	}
	eventBytes := conflictEventStoredBytes(space, left, right, winner)
	if eventBytes > log.maxBytes {
		return ConflictEvent{}, ErrConflictEventLogTooLarge
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextSequence == 0 {
		return ConflictEvent{}, ErrConflictEventLogInvalid
	}
	for log.count > 0 && (log.count >= log.capacity || log.bytes+eventBytes > log.maxBytes) {
		log.evictOldestLocked()
	}
	sequence := log.nextSequence
	log.nextSequence++
	event := ConflictEvent{
		Sequence:  sequence,
		Space:     string([]byte(space)),
		KeyDigest: log.digestKey(key),
		Left:      left,
		Right:     right,
		Winner:    winner,
		Policy:    policy,
		Decision:  decision,
	}
	index := (log.head + log.count) % log.capacity
	log.events[index] = event
	log.count++
	log.bytes += eventBytes
	return event, nil
}

// ReadAfter returns events strictly after after. A cursor older than the
// retained window fails explicitly so consumers cannot silently skip events.
func (log *ConflictEventLog) ReadAfter(after uint64, limit int) (ConflictEventLogPage, error) {
	if log == nil || limit <= 0 {
		return ConflictEventLogPage{}, ErrConflictEventLogInvalid
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	page := ConflictEventLogPage{NextSequence: after}
	if log.count == 0 {
		return page, nil
	}
	page.OldestSequence = log.events[log.head].Sequence
	lastIndex := (log.head + log.count - 1) % log.capacity
	page.NewestSequence = log.events[lastIndex].Sequence
	if page.OldestSequence > 1 && after < page.OldestSequence-1 {
		return ConflictEventLogPage{}, ErrConflictEventCursorExpired
	}
	if after == ^uint64(0) {
		return page, nil
	}
	start := after + 1
	if start < page.OldestSequence {
		start = page.OldestSequence
	}
	if start > page.NewestSequence {
		return page, nil
	}
	available := int(page.NewestSequence - start + 1)
	if available > limit {
		available = limit
	}
	page.Events = make([]ConflictEvent, available)
	for index := 0; index < available; index++ {
		sequence := start + uint64(index)
		eventIndex := (log.head + int(sequence-page.OldestSequence)) % log.capacity
		page.Events[index] = log.events[eventIndex]
	}
	page.NextSequence = page.Events[len(page.Events)-1].Sequence
	page.More = page.NextSequence < page.NewestSequence
	return page, nil
}

// MarshalBinary returns a checksummed snapshot of the retained events.
func (log *ConflictEventLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictEventLogInvalid
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	data := make([]byte, 0, 25+log.bytes+4)
	data = append(data, conflictEventLogMagic...)
	data = append(data, conflictEventLogVersion)
	var uint32Buffer [4]byte
	binary.BigEndian.PutUint32(uint32Buffer[:], uint32(log.count))
	data = append(data, uint32Buffer[:]...)
	var uint64Buffer [8]byte
	binary.BigEndian.PutUint64(uint64Buffer[:], log.nextSequence)
	data = append(data, uint64Buffer[:]...)
	data = append(data, log.hashKeyFingerprint[:]...)
	for index := 0; index < log.count; index++ {
		eventIndex := (log.head + index) % log.capacity
		data = appendConflictEvent(data, log.events[eventIndex])
	}
	checksum := crc32.Checksum(data, conflictEventLogCRC32CTable)
	binary.BigEndian.PutUint32(uint32Buffer[:], checksum)
	return append(data, uint32Buffer[:]...), nil
}

// RestoreBinary atomically replaces the log with a validated snapshot.
func (log *ConflictEventLog) RestoreBinary(data []byte) error {
	if log == nil {
		return ErrConflictEventLogInvalid
	}
	events, nextSequence, fingerprint, err := decodeConflictEventSnapshot(data, log.capacity, log.maxBytes)
	if err != nil {
		return err
	}
	if fingerprint != log.hashKeyFingerprint {
		return ErrConflictEventLogCorrupt
	}
	bytes := 0
	for _, event := range events {
		bytes += conflictEventStoredBytes(event.Space, event.Left, event.Right, event.Winner)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	for index := range log.events {
		log.events[index] = ConflictEvent{}
	}
	for index, event := range events {
		log.events[index] = event
	}
	log.head = 0
	log.count = len(events)
	log.bytes = bytes
	log.nextSequence = nextSequence
	return nil
}

func (log *ConflictEventLog) evictOldestLocked() {
	if log.count == 0 {
		return
	}
	log.bytes -= conflictEventStoredBytes(log.events[log.head].Space, log.events[log.head].Left, log.events[log.head].Right, log.events[log.head].Winner)
	log.events[log.head] = ConflictEvent{}
	log.head = (log.head + 1) % log.capacity
	log.count--
}

func (log *ConflictEventLog) digestKey(key []byte) (digest [conflictEventLogKeyDigestBytes]byte) {
	var inner [128]byte
	innerLength := len(log.hashInnerPad) + len(conflictEventDigestDomain) + len(key)
	if innerLength > len(inner) {
		innerHash := sha256.New()
		_, _ = innerHash.Write(log.hashInnerPad[:])
		_, _ = innerHash.Write([]byte(conflictEventDigestDomain))
		_, _ = innerHash.Write(key)
		innerSum := innerHash.Sum(nil)
		outerHash := sha256.New()
		_, _ = outerHash.Write(log.hashOuterPad[:])
		_, _ = outerHash.Write(innerSum)
		sum := outerHash.Sum(nil)
		copy(digest[:], sum[:conflictEventLogKeyDigestBytes])
		return digest
	}
	copy(inner[:len(log.hashInnerPad)], log.hashInnerPad[:])
	position := len(log.hashInnerPad)
	copy(inner[position:], conflictEventDigestDomain)
	position += len(conflictEventDigestDomain)
	copy(inner[position:], key)
	innerSum := sha256.Sum256(inner[:innerLength])
	var outer [96]byte
	copy(outer[:len(log.hashOuterPad)], log.hashOuterPad[:])
	copy(outer[len(log.hashOuterPad):], innerSum[:])
	sum := sha256.Sum256(outer[:])
	copy(digest[:], sum[:conflictEventLogKeyDigestBytes])
	return digest
}

func conflictEventStoredBytes(space string, left, right, winner ConflictVersion) int {
	return conflictEventFixedBytes + len(space) + len(left.NodeID) + len(right.NodeID) + len(winner.NodeID)
}

func validConflictPolicyMode(policy ConflictPolicyMode) bool {
	return policy == ConflictPolicyLastWriteWins || policy == ConflictPolicySourcePriority || policy == ConflictPolicyReject
}

func validConflictEventDecision(decision ConflictEventDecision) bool {
	return decision == ConflictEventResolved || decision == ConflictEventRejected
}

func sameConflictVersion(left, right ConflictVersion) bool {
	return left == right
}

func appendConflictEvent(data []byte, event ConflictEvent) []byte {
	var uint64Buffer [8]byte
	var uint16Buffer [2]byte
	binary.BigEndian.PutUint64(uint64Buffer[:], event.Sequence)
	data = append(data, uint64Buffer[:]...)
	binary.BigEndian.PutUint16(uint16Buffer[:], uint16(len(event.Space)))
	data = append(data, uint16Buffer[:]...)
	data = append(data, event.Space...)
	data = append(data, event.KeyDigest[:]...)
	data = appendConflictVersion(data, event.Left)
	data = appendConflictVersion(data, event.Right)
	data = appendConflictVersion(data, event.Winner)
	data = append(data, byte(event.Policy), byte(event.Decision))
	return data
}

func appendConflictVersion(data []byte, version ConflictVersion) []byte {
	var uint64Buffer [8]byte
	var uint16Buffer [2]byte
	binary.BigEndian.PutUint64(uint64Buffer[:], uint64(version.Timestamp))
	data = append(data, uint64Buffer[:]...)
	binary.BigEndian.PutUint64(uint64Buffer[:], version.Sequence)
	data = append(data, uint64Buffer[:]...)
	binary.BigEndian.PutUint16(uint16Buffer[:], uint16(len(version.NodeID)))
	data = append(data, uint16Buffer[:]...)
	data = append(data, version.NodeID...)
	return data
}

func decodeConflictEventSnapshot(data []byte, capacity, maxBytes int) ([]ConflictEvent, uint64, [conflictEventLogKeyFingerprintSize]byte, error) {
	var fingerprint [conflictEventLogKeyFingerprintSize]byte
	if len(data) < 29 || string(data[:4]) != conflictEventLogMagic || data[4] != conflictEventLogVersion {
		return nil, 0, fingerprint, ErrConflictEventLogCorrupt
	}
	wantChecksum := binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(data[:len(data)-4], conflictEventLogCRC32CTable) != wantChecksum {
		return nil, 0, fingerprint, ErrConflictEventLogCorrupt
	}
	count := int(binary.BigEndian.Uint32(data[5:9]))
	if count > capacity {
		return nil, 0, fingerprint, ErrConflictEventLogCorrupt
	}
	nextSequence := binary.BigEndian.Uint64(data[9:17])
	if nextSequence == 0 {
		return nil, 0, fingerprint, ErrConflictEventLogCorrupt
	}
	copy(fingerprint[:], data[17:25])
	position := 25
	end := len(data) - 4
	events := make([]ConflictEvent, 0, count)
	bytes := 0
	var previous uint64
	for index := 0; index < count; index++ {
		event, next, err := readConflictEvent(data, position, end)
		if err != nil || (index > 0 && event.Sequence <= previous) || event.Sequence == 0 {
			return nil, 0, fingerprint, ErrConflictEventLogCorrupt
		}
		storedBytes := conflictEventStoredBytes(event.Space, event.Left, event.Right, event.Winner)
		if storedBytes > maxBytes || bytes > maxBytes-storedBytes || !validStoredConflictEvent(event) {
			return nil, 0, fingerprint, ErrConflictEventLogCorrupt
		}
		bytes += storedBytes
		previous = event.Sequence
		events = append(events, event)
		position = next
	}
	if position != end || (count > 0 && nextSequence != previous+1) {
		return nil, 0, fingerprint, ErrConflictEventLogCorrupt
	}
	return events, nextSequence, fingerprint, nil
}

func readConflictEvent(data []byte, position, end int) (ConflictEvent, int, error) {
	var event ConflictEvent
	if position < 0 || position+10 > end {
		return event, position, ErrConflictEventLogCorrupt
	}
	event.Sequence = binary.BigEndian.Uint64(data[position : position+8])
	position += 8
	spaceLength := int(binary.BigEndian.Uint16(data[position : position+2]))
	position += 2
	if spaceLength == 0 || spaceLength > conflictEventMaxSpaceBytes || position+spaceLength+conflictEventLogKeyDigestBytes > end {
		return event, position, ErrConflictEventLogCorrupt
	}
	event.Space = string(data[position : position+spaceLength])
	position += spaceLength
	copy(event.KeyDigest[:], data[position:position+conflictEventLogKeyDigestBytes])
	position += conflictEventLogKeyDigestBytes
	var err error
	event.Left, position, err = readConflictVersion(data, position, end)
	if err != nil {
		return event, position, err
	}
	event.Right, position, err = readConflictVersion(data, position, end)
	if err != nil {
		return event, position, err
	}
	event.Winner, position, err = readConflictVersion(data, position, end)
	if err != nil || position+2 > end {
		return event, position, ErrConflictEventLogCorrupt
	}
	event.Policy = ConflictPolicyMode(data[position])
	event.Decision = ConflictEventDecision(data[position+1])
	return event, position + 2, nil
}

func readConflictVersion(data []byte, position, end int) (ConflictVersion, int, error) {
	var version ConflictVersion
	if position+18 > end {
		return version, position, ErrConflictEventLogCorrupt
	}
	version.Timestamp = int64(binary.BigEndian.Uint64(data[position : position+8]))
	position += 8
	version.Sequence = binary.BigEndian.Uint64(data[position : position+8])
	position += 8
	nodeLength := int(binary.BigEndian.Uint16(data[position : position+2]))
	position += 2
	if nodeLength == 0 || nodeLength > conflictEventMaxNodeIDBytes || position+nodeLength > end {
		return version, position, ErrConflictEventLogCorrupt
	}
	version.NodeID = string(data[position : position+nodeLength])
	return version, position + nodeLength, nil
}

func validStoredConflictEvent(event ConflictEvent) bool {
	if !validConflictPolicyMode(event.Policy) || !validConflictEventDecision(event.Decision) || event.KeyDigest == ([conflictEventLogKeyDigestBytes]byte{}) {
		return false
	}
	if _, err := CompareConflictVersions(event.Left, event.Right); err != nil {
		return false
	}
	if event.Decision == ConflictEventRejected {
		return event.Winner == (ConflictVersion{})
	}
	return sameConflictVersion(event.Winner, event.Left) || sameConflictVersion(event.Winner, event.Right)
}
