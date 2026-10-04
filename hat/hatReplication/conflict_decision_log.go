package hatReplication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
)

var (
	// ErrConflictDecisionLogInvalid indicates an invalid log configuration,
	// event, or snapshot.
	ErrConflictDecisionLogInvalid = errors.New("hatriecache: invalid conflict decision log")
	// ErrConflictDecisionLogNotConflict indicates that two versions compare
	// equal and therefore do not represent a conflict.
	ErrConflictDecisionLogNotConflict = errors.New("hatriecache: conflict decision has no winner")
	// ErrConflictDecisionLogHistoryGap indicates that the requested cursor is
	// older than the retained ring history.
	ErrConflictDecisionLogHistoryGap = errors.New("hatriecache: conflict decision history gap")
	// ErrConflictDecisionLogCorrupt indicates a snapshot checksum failure.
	ErrConflictDecisionLogCorrupt = errors.New("hatriecache: corrupt conflict decision log snapshot")
)

const (
	// ConflictDecisionLogSnapshotVersion is the current snapshot format.
	ConflictDecisionLogSnapshotVersion = 1
	// MaxConflictDecisionLogCapacity bounds the retained event ring and the
	// allocation performed while restoring a snapshot.
	MaxConflictDecisionLogCapacity = 1 << 16
	// MaxConflictDecisionLogSpaceBytes bounds the redacted source/space label.
	MaxConflictDecisionLogSpaceBytes = 256
	// MaxConflictDecisionLogKeyBytes bounds the input hashed into KeyDigest.
	MaxConflictDecisionLogKeyBytes = 4 << 10
	// MaxConflictDecisionLogNodeBytes bounds each conflict writer identifier.
	MaxConflictDecisionLogNodeBytes = 256
	// MaxConflictDecisionLogTailLimit bounds one read from the event ring.
	MaxConflictDecisionLogTailLimit = 4096
	// MaxConflictDecisionLogSnapshotBytes bounds a serialized snapshot.
	MaxConflictDecisionLogSnapshotBytes = 16 << 20
)

const (
	conflictDecisionLogHeaderSize   = 5
	conflictDecisionLogChecksumSize = 4
	conflictDecisionLogDigestSize   = 16
)

var (
	conflictDecisionLogMagic    = [4]byte{'H', 'C', 'D', '1'}
	conflictDecisionLogCRCTable = crc32.MakeTable(crc32.Castagnoli)
)

// ConflictDecisionEvent is a redacted record of one resolved conflict. The
// original key is never retained; KeyDigest is the first 16 bytes of its
// SHA-256 digest.
type ConflictDecisionEvent struct {
	Sequence  uint64
	Space     string
	KeyDigest [conflictDecisionLogDigestSize]byte
	Winner    ConflictVersion
	Loser     ConflictVersion
}

// ConflictDecisionLogTail is one bounded, ordered read from a decision log.
type ConflictDecisionLogTail struct {
	Events         []ConflictDecisionEvent
	OldestSequence uint64
	LatestSequence uint64
	Dropped        uint64
}

// ConflictDecisionLog is an opt-in bounded conflict decision ring. Construct
// one only when conflict introspection is needed; conflict resolution itself
// remains allocation-free when no log is attached by the caller.
type ConflictDecisionLog struct {
	mu           sync.RWMutex
	events       []ConflictDecisionEvent
	start        int
	count        int
	nextSequence uint64
	dropped      uint64
}

// NewConflictDecisionLog creates a bounded redacted conflict decision ring.
func NewConflictDecisionLog(capacity int) (*ConflictDecisionLog, error) {
	if capacity < 1 || capacity > MaxConflictDecisionLogCapacity {
		return nil, fmt.Errorf("%w: capacity %d", ErrConflictDecisionLogInvalid, capacity)
	}
	return &ConflictDecisionLog{
		events:       make([]ConflictDecisionEvent, capacity),
		nextSequence: 1,
	}, nil
}

// Record resolves left and right, hashes key without retaining it, and stores
// the winning and losing versions in the bounded ring.
func (log *ConflictDecisionLog) Record(space, key string, left, right ConflictVersion) (ConflictDecisionEvent, error) {
	if log == nil {
		return ConflictDecisionEvent{}, fmt.Errorf("%w: nil log", ErrConflictDecisionLogInvalid)
	}
	if len(space) == 0 || len(space) > MaxConflictDecisionLogSpaceBytes {
		return ConflictDecisionEvent{}, fmt.Errorf("%w: space length %d", ErrConflictDecisionLogInvalid, len(space))
	}
	if len(key) == 0 || len(key) > MaxConflictDecisionLogKeyBytes {
		return ConflictDecisionEvent{}, fmt.Errorf("%w: key length %d", ErrConflictDecisionLogInvalid, len(key))
	}
	if len(left.NodeID) == 0 || len(left.NodeID) > MaxConflictDecisionLogNodeBytes || len(right.NodeID) == 0 || len(right.NodeID) > MaxConflictDecisionLogNodeBytes {
		return ConflictDecisionEvent{}, ErrConflictVersionInvalid
	}
	comparison, err := CompareConflictVersions(left, right)
	if err != nil {
		return ConflictDecisionEvent{}, err
	}
	if comparison == 0 {
		return ConflictDecisionEvent{}, ErrConflictDecisionLogNotConflict
	}
	digest := sha256.Sum256([]byte(key))
	event := ConflictDecisionEvent{Space: space}
	copy(event.KeyDigest[:], digest[:conflictDecisionLogDigestSize])
	if comparison > 0 {
		event.Winner = left
		event.Loser = right
	} else {
		event.Winner = right
		event.Loser = left
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.events) == 0 {
		return ConflictDecisionEvent{}, fmt.Errorf("%w: uninitialized log", ErrConflictDecisionLogInvalid)
	}
	if log.nextSequence == 0 || log.nextSequence == ^uint64(0) {
		return ConflictDecisionEvent{}, fmt.Errorf("%w: sequence exhausted", ErrConflictDecisionLogInvalid)
	}
	event.Sequence = log.nextSequence
	log.nextSequence++
	position := (log.start + log.count) % len(log.events)
	if log.count == len(log.events) {
		position = log.start
		log.start = (log.start + 1) % len(log.events)
		log.dropped++
	} else {
		log.count++
	}
	log.events[position] = event
	return event, nil
}

// Tail returns events newer than afterSequence. A zero cursor starts at the
// oldest retained event; a nonzero cursor that falls behind retention returns
// ErrConflictDecisionLogHistoryGap.
func (log *ConflictDecisionLog) Tail(afterSequence uint64, limit int) (ConflictDecisionLogTail, error) {
	if log == nil {
		return ConflictDecisionLogTail{}, fmt.Errorf("%w: nil log", ErrConflictDecisionLogInvalid)
	}
	if limit < 1 || limit > MaxConflictDecisionLogTailLimit {
		return ConflictDecisionLogTail{}, fmt.Errorf("%w: tail limit %d", ErrConflictDecisionLogInvalid, limit)
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.events) == 0 {
		return ConflictDecisionLogTail{}, fmt.Errorf("%w: uninitialized log", ErrConflictDecisionLogInvalid)
	}
	tail := ConflictDecisionLogTail{Events: make([]ConflictDecisionEvent, 0, limit), Dropped: log.dropped}
	if log.count == 0 {
		return tail, nil
	}
	tail.OldestSequence = log.events[log.start].Sequence
	lastPosition := (log.start + log.count - 1) % len(log.events)
	tail.LatestSequence = log.events[lastPosition].Sequence
	if afterSequence != 0 && tail.OldestSequence > afterSequence && tail.OldestSequence-afterSequence > 1 {
		return ConflictDecisionLogTail{}, fmt.Errorf("%w: after %d, oldest %d", ErrConflictDecisionLogHistoryGap, afterSequence, tail.OldestSequence)
	}
	for offset := 0; offset < log.count && len(tail.Events) < limit; offset++ {
		position := (log.start + offset) % len(log.events)
		event := log.events[position]
		if event.Sequence > afterSequence {
			tail.Events = append(tail.Events, event)
		}
	}
	return tail, nil
}

// Snapshot serializes the retained decisions as a deterministic, checksummed
// HCD1 frame. The original keys and values are never present in the frame.
func (log *ConflictDecisionLog) Snapshot() ([]byte, error) {
	if log == nil {
		return nil, fmt.Errorf("%w: nil log", ErrConflictDecisionLogInvalid)
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.nextSequence == 0 || log.count < 0 || log.count > len(log.events) {
		return nil, fmt.Errorf("%w: invalid internal state", ErrConflictDecisionLogInvalid)
	}
	encoded := make([]byte, 0, conflictDecisionLogHeaderSize+conflictDecisionLogChecksumSize)
	encoded = append(encoded, conflictDecisionLogMagic[:]...)
	encoded = append(encoded, byte(ConflictDecisionLogSnapshotVersion))
	encoded = appendConflictDecisionLogUvarint(encoded, uint64(len(log.events)))
	encoded = appendConflictDecisionLogUvarint(encoded, log.nextSequence)
	encoded = appendConflictDecisionLogUvarint(encoded, log.dropped)
	encoded = appendConflictDecisionLogUvarint(encoded, uint64(log.count))
	for offset := 0; offset < log.count; offset++ {
		position := (log.start + offset) % len(log.events)
		var err error
		encoded, err = appendConflictDecisionLogEvent(encoded, log.events[position])
		if err != nil {
			return nil, err
		}
		if len(encoded)+conflictDecisionLogChecksumSize > MaxConflictDecisionLogSnapshotBytes {
			return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrConflictDecisionLogInvalid, MaxConflictDecisionLogSnapshotBytes)
		}
	}
	checksum := crc32.Checksum(encoded, conflictDecisionLogCRCTable)
	var checksumBytes [conflictDecisionLogChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

// RestoreConflictDecisionLog restores a previously snapshotted decision ring.
func RestoreConflictDecisionLog(encoded []byte) (*ConflictDecisionLog, error) {
	if len(encoded) < conflictDecisionLogHeaderSize+conflictDecisionLogChecksumSize || len(encoded) > MaxConflictDecisionLogSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot size %d", ErrConflictDecisionLogInvalid, len(encoded))
	}
	if !bytes.Equal(encoded[:len(conflictDecisionLogMagic)], conflictDecisionLogMagic[:]) {
		return nil, fmt.Errorf("%w: magic", ErrConflictDecisionLogInvalid)
	}
	if encoded[len(conflictDecisionLogMagic)] != ConflictDecisionLogSnapshotVersion {
		return nil, fmt.Errorf("%w: version %d", ErrConflictDecisionLogInvalid, encoded[len(conflictDecisionLogMagic)])
	}
	bodyEnd := len(encoded) - conflictDecisionLogChecksumSize
	position := conflictDecisionLogHeaderSize
	capacity, err := readConflictDecisionLogUvarint(encoded, &position, bodyEnd)
	if err != nil || capacity < 1 || capacity > MaxConflictDecisionLogCapacity {
		return nil, conflictDecisionLogInvalidRead(err, "capacity")
	}
	nextSequence, err := readConflictDecisionLogUvarint(encoded, &position, bodyEnd)
	if err != nil || nextSequence == 0 {
		return nil, conflictDecisionLogInvalidRead(err, "next sequence")
	}
	dropped, err := readConflictDecisionLogUvarint(encoded, &position, bodyEnd)
	if err != nil {
		return nil, err
	}
	count, err := readConflictDecisionLogUvarint(encoded, &position, bodyEnd)
	if err != nil || count > capacity || count > MaxConflictDecisionLogCapacity {
		return nil, conflictDecisionLogInvalidRead(err, "count")
	}
	events := make([]ConflictDecisionEvent, int(count))
	for index := range events {
		event, next, err := readConflictDecisionLogEvent(encoded, position, bodyEnd)
		if err != nil {
			return nil, err
		}
		if index > 0 && event.Sequence != events[index-1].Sequence+1 {
			return nil, fmt.Errorf("%w: event sequence", ErrConflictDecisionLogInvalid)
		}
		events[index] = event
		position = next
	}
	if position != bodyEnd {
		return nil, fmt.Errorf("%w: trailing snapshot payload", ErrConflictDecisionLogInvalid)
	}
	checksum := crc32.Checksum(encoded[:bodyEnd], conflictDecisionLogCRCTable)
	if got := binary.LittleEndian.Uint32(encoded[bodyEnd:]); got != checksum {
		return nil, fmt.Errorf("%w: got %08x, want %08x", ErrConflictDecisionLogCorrupt, got, checksum)
	}
	if count == 0 {
		if dropped != 0 || nextSequence != 1 {
			return nil, fmt.Errorf("%w: empty snapshot counters", ErrConflictDecisionLogInvalid)
		}
	} else {
		last := events[len(events)-1].Sequence
		if last == ^uint64(0) || dropped > ^uint64(0)-count-1 || nextSequence != last+1 || dropped+count+1 != nextSequence {
			return nil, fmt.Errorf("%w: snapshot counters", ErrConflictDecisionLogInvalid)
		}
	}
	log := &ConflictDecisionLog{events: make([]ConflictDecisionEvent, int(capacity)), nextSequence: nextSequence, dropped: dropped, count: int(count)}
	copy(log.events, events)
	return log, nil
}

func appendConflictDecisionLogEvent(dst []byte, event ConflictDecisionEvent) ([]byte, error) {
	if event.Sequence == 0 || len(event.Space) == 0 || len(event.Space) > MaxConflictDecisionLogSpaceBytes || len(event.Winner.NodeID) == 0 || len(event.Winner.NodeID) > MaxConflictDecisionLogNodeBytes || len(event.Loser.NodeID) == 0 || len(event.Loser.NodeID) > MaxConflictDecisionLogNodeBytes {
		return nil, fmt.Errorf("%w: invalid event", ErrConflictDecisionLogInvalid)
	}
	comparison, err := CompareConflictVersions(event.Winner, event.Loser)
	if err != nil || comparison == 0 {
		if err != nil {
			return nil, err
		}
		return nil, ErrConflictDecisionLogNotConflict
	}
	dst = appendConflictDecisionLogUvarint(dst, event.Sequence)
	dst = appendConflictDecisionLogBytes(dst, event.Space)
	dst = append(dst, event.KeyDigest[:]...)
	dst = appendConflictDecisionLogVersion(dst, event.Winner)
	dst = appendConflictDecisionLogVersion(dst, event.Loser)
	return dst, nil
}

func appendConflictDecisionLogVersion(dst []byte, version ConflictVersion) []byte {
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(version.Timestamp))
	dst = append(dst, timestamp[:]...)
	dst = appendConflictDecisionLogBytes(dst, version.NodeID)
	return appendConflictDecisionLogUvarint(dst, version.Sequence)
}

func readConflictDecisionLogEvent(encoded []byte, position, limit int) (ConflictDecisionEvent, int, error) {
	sequence, err := readConflictDecisionLogUvarint(encoded, &position, limit)
	if err != nil || sequence == 0 {
		return ConflictDecisionEvent{}, position, conflictDecisionLogInvalidRead(err, "event sequence")
	}
	space, next, err := readConflictDecisionLogBytes(encoded, position, limit, MaxConflictDecisionLogSpaceBytes)
	if err != nil {
		return ConflictDecisionEvent{}, position, err
	}
	if len(space) == 0 {
		return ConflictDecisionEvent{}, position, fmt.Errorf("%w: empty space", ErrConflictDecisionLogInvalid)
	}
	position = next
	if limit-position < conflictDecisionLogDigestSize {
		return ConflictDecisionEvent{}, position, fmt.Errorf("%w: key digest", ErrConflictDecisionLogInvalid)
	}
	var digest [conflictDecisionLogDigestSize]byte
	copy(digest[:], encoded[position:position+conflictDecisionLogDigestSize])
	position += conflictDecisionLogDigestSize
	winner, next, err := readConflictDecisionLogVersion(encoded, position, limit)
	if err != nil {
		return ConflictDecisionEvent{}, position, err
	}
	loser, next, err := readConflictDecisionLogVersion(encoded, next, limit)
	if err != nil {
		return ConflictDecisionEvent{}, position, err
	}
	if comparison, err := CompareConflictVersions(winner, loser); err != nil || comparison == 0 {
		if err != nil {
			return ConflictDecisionEvent{}, position, err
		}
		return ConflictDecisionEvent{}, position, ErrConflictDecisionLogNotConflict
	}
	return ConflictDecisionEvent{Sequence: sequence, Space: string(space), KeyDigest: digest, Winner: winner, Loser: loser}, next, nil
}

func readConflictDecisionLogVersion(encoded []byte, position, limit int) (ConflictVersion, int, error) {
	if limit-position < 8 {
		return ConflictVersion{}, position, fmt.Errorf("%w: version timestamp", ErrConflictDecisionLogInvalid)
	}
	version := ConflictVersion{Timestamp: int64(binary.BigEndian.Uint64(encoded[position : position+8]))}
	position += 8
	node, next, err := readConflictDecisionLogBytes(encoded, position, limit, MaxConflictDecisionLogNodeBytes)
	if err != nil {
		return ConflictVersion{}, position, err
	}
	version.NodeID = string(node)
	position = next
	version.Sequence, err = readConflictDecisionLogUvarint(encoded, &position, limit)
	if err != nil {
		return ConflictVersion{}, position, err
	}
	if version.NodeID == "" {
		return ConflictVersion{}, position, ErrConflictVersionInvalid
	}
	return version, position, nil
}

func appendConflictDecisionLogBytes(dst []byte, value string) []byte {
	dst = appendConflictDecisionLogUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readConflictDecisionLogBytes(encoded []byte, position, limit, maxLength int) ([]byte, int, error) {
	length, err := readConflictDecisionLogUvarint(encoded, &position, limit)
	if err != nil {
		return nil, position, err
	}
	if length > uint64(maxLength) || length > uint64(limit-position) {
		return nil, position, fmt.Errorf("%w: byte field length %d", ErrConflictDecisionLogInvalid, length)
	}
	end := position + int(length)
	return append([]byte(nil), encoded[position:end]...), end, nil
}

func appendConflictDecisionLogUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:size]...)
}

func readConflictDecisionLogUvarint(encoded []byte, position *int, limit int) (uint64, error) {
	if *position >= limit {
		return 0, fmt.Errorf("%w: truncated varint", ErrConflictDecisionLogInvalid)
	}
	value, size := binary.Uvarint(encoded[*position:limit])
	if size <= 0 || uint64(size) != conflictDecisionLogUvarintSize(value) {
		return 0, fmt.Errorf("%w: invalid varint", ErrConflictDecisionLogInvalid)
	}
	*position += size
	return value, nil
}

func conflictDecisionLogInvalidRead(err error, field string) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: invalid %s", ErrConflictDecisionLogInvalid, field)
}

func conflictDecisionLogUvarintSize(value uint64) uint64 {
	size := uint64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}
