package hatReplication

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"
)

var (
	ErrConflictIntrospectionNil        = errors.New("hatriecache: conflict introspection log is nil")
	ErrConflictIntrospectionInvalid    = errors.New("hatriecache: conflict introspection observation is invalid")
	ErrConflictIntrospectionCapacity   = errors.New("hatriecache: conflict introspection capacity is invalid")
	ErrConflictIntrospectionLimit      = errors.New("hatriecache: conflict introspection replay limit is invalid")
	ErrConflictIntrospectionHistoryGap = errors.New("hatriecache: conflict introspection history gap")
	ErrConflictIntrospectionCorrupt    = errors.New("hatriecache: conflict introspection snapshot is corrupt")
	ErrConflictIntrospectionSecret     = errors.New("hatriecache: conflict key redaction secret is invalid")
)

const (
	DefaultConflictIntrospectionCapacity = 1024
	MaxConflictIntrospectionCapacity     = 65536
	MaxConflictIntrospectionReplay       = 4096
	MaxConflictIntrospectionSpaceBytes   = 256
	MaxConflictIntrospectionNodeBytes    = 256
	MaxConflictIntrospectionWireBytes    = 16 << 20
	MaxConflictIntrospectionKeyBytes     = 1 << 20
	conflictIntrospectionDigestBytes     = sha256.Size
	conflictIntrospectionDigestHexBytes  = conflictIntrospectionDigestBytes * 2
	conflictIntrospectionSnapshotVersion = 1
)

const (
	ConflictIntrospectionApplied  ConflictIntrospectionDecision = "applied"
	ConflictIntrospectionRejected ConflictIntrospectionDecision = "rejected"
)

var conflictIntrospectionCRC32C = crc32.MakeTable(crc32.Castagnoli)

// ConflictIntrospectionDecision describes what the conflict resolver did with
// a distinct pair of versions.
type ConflictIntrospectionDecision string

// ConflictIntrospectionOptions bounds retained redacted conflict records.
// Capacity zero selects DefaultConflictIntrospectionCapacity.
type ConflictIntrospectionOptions struct {
	Capacity int
}

// ConflictIntrospectionObservation is the caller-owned, already-redacted
// input to ConflictIntrospectionLog.Record. KeyDigest must be a 32-byte
// hexadecimal digest; use RedactConflictKey when the original key is known.
type ConflictIntrospectionObservation struct {
	Space     string
	KeyDigest string
	Left      ConflictVersion
	Right     ConflictVersion
	Winner    *ConflictVersion
	Decision  ConflictIntrospectionDecision
}

// ConflictIntrospectionRecord is an immutable, redacted stream record.
type ConflictIntrospectionRecord struct {
	Sequence  uint64                        `json:"sequence"`
	Space     string                        `json:"space"`
	KeyDigest string                        `json:"key_digest"`
	Left      ConflictVersion               `json:"left"`
	Right     ConflictVersion               `json:"right"`
	Winner    *ConflictVersion              `json:"winner,omitempty"`
	Decision  ConflictIntrospectionDecision `json:"decision"`
}

// ConflictIntrospectionStats describes the retained portion of the bounded
// stream. Dropped counts records evicted from the ring since construction or
// restore.
type ConflictIntrospectionStats struct {
	Capacity       int    `json:"capacity"`
	Retained       int    `json:"retained"`
	Dropped        uint64 `json:"dropped"`
	OldestSequence uint64 `json:"oldest_sequence"`
	LatestSequence uint64 `json:"latest_sequence"`
}

// ConflictIntrospectionLog is a concurrency-safe bounded redacted stream.
// It does not retain keys, values, or caller-owned mutable memory.
type ConflictIntrospectionLog struct {
	mu           sync.RWMutex
	capacity     int
	records      []ConflictIntrospectionRecord
	start        int
	nextSequence uint64
	dropped      uint64
}

// NewConflictIntrospectionLog creates an in-memory bounded conflict stream.
func NewConflictIntrospectionLog(options ConflictIntrospectionOptions) (*ConflictIntrospectionLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictIntrospectionCapacity
	}
	if capacity < 1 || capacity > MaxConflictIntrospectionCapacity {
		return nil, fmt.Errorf("%w: capacity must be between 1 and %d", ErrConflictIntrospectionCapacity, MaxConflictIntrospectionCapacity)
	}
	return &ConflictIntrospectionLog{capacity: capacity, records: make([]ConflictIntrospectionRecord, 0, capacity)}, nil
}

// RedactConflictKey returns a keyed SHA-256 digest suitable for
// ConflictIntrospectionObservation.KeyDigest. The secret is never retained;
// requiring a non-trivial secret avoids turning the digest into a plain key
// hash that is easy to dictionary-attack.
func RedactConflictKey(secret, key []byte) (string, error) {
	if len(secret) < 16 {
		return "", ErrConflictIntrospectionSecret
	}
	if len(key) == 0 || len(key) > MaxConflictIntrospectionKeyBytes {
		return "", fmt.Errorf("%w: key length must be between 1 and %d", ErrConflictIntrospectionInvalid, MaxConflictIntrospectionKeyBytes)
	}
	hasher := hmac.New(sha256.New, secret)
	_, _ = hasher.Write(key)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// Record appends one validated redacted conflict observation and returns its
// assigned sequence. The returned value is independent of the caller.
func (log *ConflictIntrospectionLog) Record(observation ConflictIntrospectionObservation) (ConflictIntrospectionRecord, error) {
	if log == nil {
		return ConflictIntrospectionRecord{}, ErrConflictIntrospectionNil
	}
	normalized, err := normalizeConflictIntrospectionObservation(observation)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.nextSequence == ^uint64(0) {
		return ConflictIntrospectionRecord{}, fmt.Errorf("%w: sequence exhausted", ErrConflictIntrospectionInvalid)
	}
	normalized.Sequence = log.nextSequence + 1
	log.nextSequence = normalized.Sequence
	if len(log.records) < log.capacity {
		log.records = append(log.records, normalized)
	} else {
		log.records[log.start] = normalized
		log.start++
		if log.start == log.capacity {
			log.start = 0
		}
		log.dropped++
	}
	return copyConflictIntrospectionRecord(normalized), nil
}

// ResolveConflictWithIntrospection resolves one conflict using registry and,
// when log is non-nil, records only the redacted decision. Equal versions are
// not conflicts and are not recorded. A rejected conflict is recorded before
// ErrConflictRejected is returned.
func ResolveConflictWithIntrospection(registry *ConflictPolicyRegistry, log *ConflictIntrospectionLog, space, keyDigest string, left, right ConflictVersion) (ConflictVersion, error) {
	if registry == nil {
		return ConflictVersion{}, ErrConflictPolicyRegistryNil
	}
	if log == nil {
		return registry.Resolve(space, left, right)
	}
	comparison, err := CompareConflictVersions(left, right)
	if err != nil {
		return ConflictVersion{}, err
	}
	if comparison == 0 {
		return left, nil
	}
	winner, resolveErr := registry.Resolve(space, left, right)
	observation := ConflictIntrospectionObservation{
		Space:     space,
		KeyDigest: keyDigest,
		Left:      left,
		Right:     right,
		Decision:  ConflictIntrospectionApplied,
	}
	if resolveErr != nil {
		if !errors.Is(resolveErr, ErrConflictRejected) {
			return ConflictVersion{}, resolveErr
		}
		observation.Decision = ConflictIntrospectionRejected
	} else {
		observation.Winner = &winner
	}
	if _, recordErr := log.Record(observation); recordErr != nil {
		return ConflictVersion{}, recordErr
	}
	if resolveErr != nil {
		return ConflictVersion{}, resolveErr
	}
	return winner, nil
}

// Replay returns records after the supplied sequence in ascending sequence
// order. Cursor zero starts at the oldest retained record. A nonzero cursor
// older than the retained window fails with ErrConflictIntrospectionHistoryGap
// instead of silently skipping events.
func (log *ConflictIntrospectionLog) Replay(after uint64, limit int) ([]ConflictIntrospectionRecord, uint64, error) {
	if log == nil {
		return nil, after, ErrConflictIntrospectionNil
	}
	if limit < 1 || limit > MaxConflictIntrospectionReplay {
		return nil, after, ErrConflictIntrospectionLimit
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	if len(log.records) == 0 {
		return nil, after, nil
	}
	oldest := log.recordAtLocked(0).Sequence
	latest := log.nextSequence
	if after != 0 && after < oldest-1 {
		return nil, after, fmt.Errorf("%w: cursor %d precedes oldest %d", ErrConflictIntrospectionHistoryGap, after, oldest)
	}
	if after >= latest {
		return nil, latest, nil
	}
	out := make([]ConflictIntrospectionRecord, 0, limit)
	next := after
	for index := 0; index < len(log.records) && len(out) < limit; index++ {
		record := log.recordAtLocked(index)
		if record.Sequence <= after {
			continue
		}
		out = append(out, copyConflictIntrospectionRecord(record))
		next = record.Sequence
	}
	return out, next, nil
}

// Snapshot returns all retained records in sequence order.
func (log *ConflictIntrospectionLog) Snapshot() []ConflictIntrospectionRecord {
	if log == nil {
		return nil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	out := make([]ConflictIntrospectionRecord, 0, len(log.records))
	for index := 0; index < len(log.records); index++ {
		out = append(out, copyConflictIntrospectionRecord(log.recordAtLocked(index)))
	}
	return out
}

// Stats returns a point-in-time bounded-stream report.
func (log *ConflictIntrospectionLog) Stats() ConflictIntrospectionStats {
	if log == nil {
		return ConflictIntrospectionStats{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	stats := ConflictIntrospectionStats{Capacity: log.capacity, Retained: len(log.records), Dropped: log.dropped, LatestSequence: log.nextSequence}
	if len(log.records) > 0 {
		stats.OldestSequence = log.recordAtLocked(0).Sequence
	}
	return stats
}

// MarshalBinary encodes the bounded redacted stream as a deterministic,
// checksummed CIS1 snapshot. Raw keys and values cannot enter this format
// because records contain only validated digests and versions.
func (log *ConflictIntrospectionLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictIntrospectionNil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	data := make([]byte, 0, 32+len(log.records)*128)
	data = append(data, 'C', 'I', 'S', '1', conflictIntrospectionSnapshotVersion, 0)
	data = appendUint32(data, uint32(log.capacity))
	data = appendUint32(data, uint32(len(log.records)))
	data = appendUint64(data, log.dropped)
	data = appendUint64(data, log.nextSequence)
	for index := 0; index < len(log.records); index++ {
		var err error
		data, err = appendConflictIntrospectionRecord(data, log.recordAtLocked(index))
		if err != nil {
			return nil, err
		}
	}
	if len(data)+4 > MaxConflictIntrospectionWireBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrConflictIntrospectionInvalid, MaxConflictIntrospectionWireBytes)
	}
	return appendUint32(data, crc32.Checksum(data, conflictIntrospectionCRC32C)), nil
}

// RestoreBinary atomically replaces the retained stream with a validated CIS1
// snapshot. If the current capacity is smaller than the snapshot, only the
// newest current-capacity records are retained and the evicted count is added
// to Dropped.
func (log *ConflictIntrospectionLog) RestoreBinary(data []byte) error {
	if log == nil {
		return ErrConflictIntrospectionNil
	}
	if len(data) < 4+2+4+4+8+8+4 || len(data) > MaxConflictIntrospectionWireBytes {
		return fmt.Errorf("%w: invalid length", ErrConflictIntrospectionCorrupt)
	}
	expectedCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	actualCRC := crc32.Checksum(data[:len(data)-4], conflictIntrospectionCRC32C)
	if expectedCRC != actualCRC {
		return fmt.Errorf("%w: checksum mismatch", ErrConflictIntrospectionCorrupt)
	}
	reader := conflictIntrospectionReader{data: data[:len(data)-4]}
	magic, err := reader.take(4)
	if err != nil || string(magic) != "CIS1" {
		return fmt.Errorf("%w: magic", ErrConflictIntrospectionCorrupt)
	}
	version, err := reader.byte()
	if err != nil || version != conflictIntrospectionSnapshotVersion {
		return fmt.Errorf("%w: version", ErrConflictIntrospectionCorrupt)
	}
	if _, err := reader.byte(); err != nil {
		return fmt.Errorf("%w: reserved header", ErrConflictIntrospectionCorrupt)
	}
	wireCapacity, err := reader.u32()
	if err != nil || wireCapacity < 1 || wireCapacity > MaxConflictIntrospectionCapacity {
		return fmt.Errorf("%w: capacity", ErrConflictIntrospectionCorrupt)
	}
	count, err := reader.u32()
	if err != nil || uint64(count) > uint64(wireCapacity) || count > MaxConflictIntrospectionCapacity {
		return fmt.Errorf("%w: record count", ErrConflictIntrospectionCorrupt)
	}
	dropped, err := reader.u64()
	if err != nil {
		return fmt.Errorf("%w: dropped count", ErrConflictIntrospectionCorrupt)
	}
	nextSequence, err := reader.u64()
	if err != nil {
		return fmt.Errorf("%w: sequence", ErrConflictIntrospectionCorrupt)
	}
	decoded := make([]ConflictIntrospectionRecord, 0, int(count))
	var previous uint64
	for index := uint32(0); index < count; index++ {
		record, recordErr := readConflictIntrospectionRecord(&reader)
		if recordErr != nil {
			return fmt.Errorf("%w: record %d: %v", ErrConflictIntrospectionCorrupt, index, recordErr)
		}
		if record.Sequence == 0 || record.Sequence <= previous {
			return fmt.Errorf("%w: record sequence order", ErrConflictIntrospectionCorrupt)
		}
		previous = record.Sequence
		decoded = append(decoded, record)
	}
	if reader.remaining() != 0 {
		return fmt.Errorf("%w: trailing bytes", ErrConflictIntrospectionCorrupt)
	}
	if len(decoded) > 0 && nextSequence != decoded[len(decoded)-1].Sequence {
		return fmt.Errorf("%w: latest sequence mismatch", ErrConflictIntrospectionCorrupt)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(decoded) > log.capacity {
		extra := len(decoded) - log.capacity
		if ^uint64(0)-dropped < uint64(extra) {
			return fmt.Errorf("%w: dropped count overflow", ErrConflictIntrospectionCorrupt)
		}
		dropped += uint64(extra)
		decoded = decoded[extra:]
	}
	log.records = make([]ConflictIntrospectionRecord, len(decoded), log.capacity)
	for index := range decoded {
		log.records[index] = copyConflictIntrospectionRecord(decoded[index])
	}
	log.start = 0
	log.dropped = dropped
	log.nextSequence = nextSequence
	return nil
}

func normalizeConflictIntrospectionObservation(observation ConflictIntrospectionObservation) (ConflictIntrospectionRecord, error) {
	space := strings.TrimSpace(observation.Space)
	if err := validateConflictIntrospectionText(space, MaxConflictIntrospectionSpaceBytes, "space"); err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	digest := strings.TrimSpace(observation.KeyDigest)
	if len(digest) != conflictIntrospectionDigestHexBytes {
		return ConflictIntrospectionRecord{}, fmt.Errorf("%w: key digest must be %d hexadecimal bytes", ErrConflictIntrospectionInvalid, conflictIntrospectionDigestHexBytes)
	}
	for index := range digest {
		character := digest[index]
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') || (character >= 'A' && character <= 'F')) {
			return ConflictIntrospectionRecord{}, fmt.Errorf("%w: key digest is not hexadecimal", ErrConflictIntrospectionInvalid)
		}
	}
	if digest != strings.ToLower(digest) {
		digest = strings.ToLower(digest)
	}
	if _, err := CompareConflictVersions(observation.Left, observation.Right); err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	comparison, _ := CompareConflictVersions(observation.Left, observation.Right)
	if comparison == 0 {
		return ConflictIntrospectionRecord{}, fmt.Errorf("%w: equal versions are not a conflict", ErrConflictIntrospectionInvalid)
	}
	if err := validateConflictIntrospectionVersion(observation.Left); err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	if err := validateConflictIntrospectionVersion(observation.Right); err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	record := ConflictIntrospectionRecord{
		Space:     space,
		KeyDigest: digest,
		Left:      observation.Left,
		Right:     observation.Right,
		Decision:  observation.Decision,
	}
	switch observation.Decision {
	case ConflictIntrospectionApplied:
		if observation.Winner == nil || (*observation.Winner != observation.Left && *observation.Winner != observation.Right) {
			return ConflictIntrospectionRecord{}, fmt.Errorf("%w: applied conflict must name the left or right winner", ErrConflictIntrospectionInvalid)
		}
		winner := *observation.Winner
		record.Winner = &winner
	case ConflictIntrospectionRejected:
		if observation.Winner != nil {
			return ConflictIntrospectionRecord{}, fmt.Errorf("%w: rejected conflict cannot have a winner", ErrConflictIntrospectionInvalid)
		}
	default:
		return ConflictIntrospectionRecord{}, fmt.Errorf("%w: unsupported decision %q", ErrConflictIntrospectionInvalid, observation.Decision)
	}
	return record, nil
}

func validateConflictIntrospectionVersion(version ConflictVersion) error {
	if err := validateConflictIntrospectionText(version.NodeID, MaxConflictIntrospectionNodeBytes, "node id"); err != nil {
		return err
	}
	return nil
}

func validateConflictIntrospectionText(value string, maximum int, label string) error {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is empty, too long, or invalid UTF-8", ErrConflictIntrospectionInvalid, label)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return fmt.Errorf("%w: %s contains a control character", ErrConflictIntrospectionInvalid, label)
		}
	}
	return nil
}

func (log *ConflictIntrospectionLog) recordAtLocked(index int) ConflictIntrospectionRecord {
	if len(log.records) == log.capacity {
		index = (log.start + index) % log.capacity
	}
	return log.records[index]
}

func copyConflictIntrospectionRecord(record ConflictIntrospectionRecord) ConflictIntrospectionRecord {
	copyRecord := record
	if record.Winner != nil {
		winner := *record.Winner
		copyRecord.Winner = &winner
	}
	return copyRecord
}

func appendConflictIntrospectionRecord(data []byte, record ConflictIntrospectionRecord) ([]byte, error) {
	if record.Sequence == 0 {
		return nil, ErrConflictIntrospectionInvalid
	}
	if len(record.Space) > MaxConflictIntrospectionSpaceBytes || len(record.KeyDigest) != conflictIntrospectionDigestHexBytes || len(record.Left.NodeID) > MaxConflictIntrospectionNodeBytes || len(record.Right.NodeID) > MaxConflictIntrospectionNodeBytes {
		return nil, ErrConflictIntrospectionInvalid
	}
	winnerLength := 0
	if record.Winner != nil {
		winnerLength = len(record.Winner.NodeID)
		if winnerLength > MaxConflictIntrospectionNodeBytes {
			return nil, ErrConflictIntrospectionInvalid
		}
	}
	data = appendUint64(data, record.Sequence)
	if record.Decision == ConflictIntrospectionApplied {
		data = append(data, 1)
	} else if record.Decision == ConflictIntrospectionRejected {
		data = append(data, 2)
	} else {
		return nil, ErrConflictIntrospectionInvalid
	}
	if record.Winner != nil {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}
	data = appendUint16(data, uint16(len(record.Space)))
	data = appendUint16(data, uint16(len(record.KeyDigest)))
	data = appendUint16(data, uint16(len(record.Left.NodeID)))
	data = appendUint16(data, uint16(len(record.Right.NodeID)))
	data = appendUint16(data, uint16(winnerLength))
	data = appendUint64(data, uint64(record.Left.Timestamp))
	data = appendUint64(data, record.Left.Sequence)
	data = appendUint64(data, uint64(record.Right.Timestamp))
	data = appendUint64(data, record.Right.Sequence)
	if record.Winner != nil {
		data = appendUint64(data, uint64(record.Winner.Timestamp))
		data = appendUint64(data, record.Winner.Sequence)
	}
	data = append(data, record.Space...)
	data = append(data, record.KeyDigest...)
	data = append(data, record.Left.NodeID...)
	data = append(data, record.Right.NodeID...)
	if record.Winner != nil {
		data = append(data, record.Winner.NodeID...)
	}
	return data, nil
}

func readConflictIntrospectionRecord(reader *conflictIntrospectionReader) (ConflictIntrospectionRecord, error) {
	sequence, err := reader.u64()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	decision, err := reader.byte()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	winnerFlag, err := reader.byte()
	if err != nil || winnerFlag > 1 {
		return ConflictIntrospectionRecord{}, errors.New("invalid winner flag")
	}
	spaceLength, err := reader.u16()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	digestLength, err := reader.u16()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	leftNodeLength, err := reader.u16()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	rightNodeLength, err := reader.u16()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	winnerNodeLength, err := reader.u16()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	leftTimestamp, err := reader.u64()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	leftSequence, err := reader.u64()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	rightTimestamp, err := reader.u64()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	rightSequence, err := reader.u64()
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	var winnerTimestamp, winnerSequence uint64
	if winnerFlag == 1 {
		winnerTimestamp, err = reader.u64()
		if err != nil {
			return ConflictIntrospectionRecord{}, err
		}
		winnerSequence, err = reader.u64()
		if err != nil {
			return ConflictIntrospectionRecord{}, err
		}
	} else if winnerNodeLength != 0 {
		return ConflictIntrospectionRecord{}, errors.New("winner node without winner")
	}
	if spaceLength > MaxConflictIntrospectionSpaceBytes || digestLength != conflictIntrospectionDigestHexBytes || leftNodeLength > MaxConflictIntrospectionNodeBytes || rightNodeLength > MaxConflictIntrospectionNodeBytes || winnerNodeLength > MaxConflictIntrospectionNodeBytes {
		return ConflictIntrospectionRecord{}, errors.New("record field exceeds bounds")
	}
	space, err := reader.string(int(spaceLength))
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	digest, err := reader.string(int(digestLength))
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	leftNode, err := reader.string(int(leftNodeLength))
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	rightNode, err := reader.string(int(rightNodeLength))
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	winnerNode := ""
	if winnerFlag == 1 {
		winnerNode, err = reader.string(int(winnerNodeLength))
		if err != nil {
			return ConflictIntrospectionRecord{}, err
		}
	}
	decisionValue := ConflictIntrospectionRejected
	if decision == 1 {
		decisionValue = ConflictIntrospectionApplied
	} else if decision != 2 {
		return ConflictIntrospectionRecord{}, errors.New("invalid decision")
	}
	var winner *ConflictVersion
	if winnerFlag == 1 {
		winnerValue := ConflictVersion{Timestamp: int64(winnerTimestamp), NodeID: winnerNode, Sequence: winnerSequence}
		winner = &winnerValue
	}
	record, err := normalizeConflictIntrospectionObservation(ConflictIntrospectionObservation{
		Space:     space,
		KeyDigest: digest,
		Left:      ConflictVersion{Timestamp: int64(leftTimestamp), NodeID: leftNode, Sequence: leftSequence},
		Right:     ConflictVersion{Timestamp: int64(rightTimestamp), NodeID: rightNode, Sequence: rightSequence},
		Winner:    winner,
		Decision:  decisionValue,
	})
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	record.Sequence = sequence
	return record, nil
}

type conflictIntrospectionReader struct {
	data   []byte
	offset int
}

func (reader *conflictIntrospectionReader) take(length int) ([]byte, error) {
	if length < 0 || length > len(reader.data)-reader.offset {
		return nil, errors.New("truncated snapshot")
	}
	value := reader.data[reader.offset : reader.offset+length]
	reader.offset += length
	return value, nil
}

func (reader *conflictIntrospectionReader) byte() (byte, error) {
	value, err := reader.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (reader *conflictIntrospectionReader) u16() (uint16, error) {
	value, err := reader.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (reader *conflictIntrospectionReader) u32() (uint32, error) {
	value, err := reader.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (reader *conflictIntrospectionReader) u64() (uint64, error) {
	value, err := reader.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (reader *conflictIntrospectionReader) string(length int) (string, error) {
	value, err := reader.take(length)
	if err != nil {
		return "", err
	}
	result := string(value)
	if !utf8.ValidString(result) {
		return "", errors.New("invalid UTF-8")
	}
	for _, character := range result {
		if unicode.IsControl(character) {
			return "", errors.New("control character")
		}
	}
	return result, nil
}

func (reader *conflictIntrospectionReader) remaining() int {
	return len(reader.data) - reader.offset
}

func appendUint16(data []byte, value uint16) []byte {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendUint32(data []byte, value uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return append(data, encoded[:]...)
}

func appendUint64(data []byte, value uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], value)
	return append(data, encoded[:]...)
}
