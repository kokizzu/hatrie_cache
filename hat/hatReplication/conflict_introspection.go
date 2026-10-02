package hatReplication

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"time"
)

const (
	// DefaultConflictIntrospectionCapacity bounds the opt-in in-memory history.
	DefaultConflictIntrospectionCapacity = 1024
	// MaxConflictIntrospectionCapacity prevents an accidental unbounded history.
	MaxConflictIntrospectionCapacity = 65536
	// MaxConflictIntrospectionRead bounds one cursor page.
	MaxConflictIntrospectionRead = 4096
	// MaxConflictIntrospectionSnapshotBytes bounds one persisted history image.
	MaxConflictIntrospectionSnapshotBytes = 16 << 20
	// MaxConflictIntrospectionStringBytes bounds space and node identifiers.
	MaxConflictIntrospectionStringBytes = 1024
	// MaxConflictIntrospectionKeyBytes bounds hashing work for one key.
	MaxConflictIntrospectionKeyBytes = 1 << 20

	conflictIntrospectionVersion byte = 1
)

var (
	// ErrConflictIntrospectionInvalid indicates malformed input or a bad snapshot.
	ErrConflictIntrospectionInvalid = errors.New("hatriecache: conflict introspection input is invalid")
	// ErrConflictIntrospectionCursorExpired indicates that ring history was evicted.
	ErrConflictIntrospectionCursorExpired = errors.New("hatriecache: conflict introspection cursor expired")

	conflictIntrospectionCRC = crc32.MakeTable(crc32.Castagnoli)
)

var conflictIntrospectionMagic = [4]byte{'C', 'I', 'R', '1'}

// ConflictIntrospectionOutcome describes the result of one conflict decision.
type ConflictIntrospectionOutcome uint8

const (
	// ConflictIntrospectionLeftWon records that Left won.
	ConflictIntrospectionLeftWon ConflictIntrospectionOutcome = iota + 1
	// ConflictIntrospectionRightWon records that Right won.
	ConflictIntrospectionRightWon
	// ConflictIntrospectionRejected records that the conflict policy rejected the write.
	ConflictIntrospectionRejected
)

// ConflictIntrospectionInput contains the non-secret facts needed to record a
// conflict. Key is hashed immediately and is never retained by the log.
type ConflictIntrospectionInput struct {
	Space   string
	Key     []byte
	Left    ConflictVersion
	Right   ConflictVersion
	Winner  ConflictVersion
	Outcome ConflictIntrospectionOutcome
}

// ConflictIntrospectionRecord is one redacted conflict decision.
type ConflictIntrospectionRecord struct {
	Sequence   uint64                       `json:"sequence"`
	AtUnixNano int64                        `json:"at_unix_nano"`
	Space      string                       `json:"space"`
	KeyDigest  string                       `json:"key_digest"`
	Left       ConflictVersion              `json:"left"`
	Right      ConflictVersion              `json:"right"`
	Winner     ConflictVersion              `json:"winner"`
	Outcome    ConflictIntrospectionOutcome `json:"outcome"`
}

// ConflictIntrospectionLogOptions configures one bounded conflict history.
// A nil Now uses time.Now. The zero Capacity uses the default capacity.
type ConflictIntrospectionLogOptions struct {
	Capacity int
	Now      func() time.Time
}

// ConflictIntrospectionLog is an opt-in bounded cursor-readable history of
// redacted conflict decisions. It does not change conflict resolution and is
// not allocated unless a caller constructs one.
type ConflictIntrospectionLog struct {
	mu       sync.RWMutex
	capacity int
	now      func() time.Time
	records  []ConflictIntrospectionRecord
	start    int
	count    int
	next     uint64
}

// NewConflictIntrospectionLog creates a bounded conflict history.
func NewConflictIntrospectionLog(options ConflictIntrospectionLogOptions) (*ConflictIntrospectionLog, error) {
	capacity := options.Capacity
	if capacity == 0 {
		capacity = DefaultConflictIntrospectionCapacity
	}
	if capacity < 1 || capacity > MaxConflictIntrospectionCapacity {
		return nil, fmt.Errorf("%w: capacity must be between 1 and %d", ErrConflictIntrospectionInvalid, MaxConflictIntrospectionCapacity)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &ConflictIntrospectionLog{
		capacity: capacity,
		now:      now,
		records:  make([]ConflictIntrospectionRecord, capacity),
	}, nil
}

// Record appends one redacted conflict decision and returns its assigned cursor.
func (log *ConflictIntrospectionLog) Record(input ConflictIntrospectionInput) (ConflictIntrospectionRecord, error) {
	if log == nil || log.capacity < 1 || log.capacity > MaxConflictIntrospectionCapacity || len(log.records) != log.capacity || log.now == nil {
		return ConflictIntrospectionRecord{}, ErrConflictIntrospectionInvalid
	}
	if err := validateConflictIntrospectionInput(input); err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	digest := sha256.Sum256(input.Key)
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.next == ^uint64(0) {
		return ConflictIntrospectionRecord{}, fmt.Errorf("%w: sequence overflow", ErrConflictIntrospectionInvalid)
	}
	log.next++
	record := ConflictIntrospectionRecord{
		Sequence:   log.next,
		AtUnixNano: log.now().UnixNano(),
		Space:      input.Space,
		KeyDigest:  hex.EncodeToString(digest[:16]),
		Left:       input.Left,
		Right:      input.Right,
		Winner:     input.Winner,
		Outcome:    input.Outcome,
	}
	index := (log.start + log.count) % log.capacity
	if log.count == log.capacity {
		log.start = (log.start + 1) % log.capacity
		index = (log.start + log.count - 1) % log.capacity
	} else {
		log.count++
	}
	log.records[index] = record
	return record, nil
}

// Read returns records newer than after and the last returned sequence. A
// cursor that points before retained history returns ErrConflictIntrospectionCursorExpired.
func (log *ConflictIntrospectionLog) Read(after uint64, limit int) ([]ConflictIntrospectionRecord, uint64, error) {
	if log == nil || log.capacity < 1 || log.capacity > MaxConflictIntrospectionCapacity || len(log.records) != log.capacity || limit < 1 || limit > MaxConflictIntrospectionRead {
		return nil, after, ErrConflictIntrospectionInvalid
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.count == 0 || after == log.next {
		return nil, after, nil
	}
	oldest := log.next - uint64(log.count) + 1
	if after < oldest-1 {
		return nil, after, fmt.Errorf("%w: after=%d oldest=%d", ErrConflictIntrospectionCursorExpired, after, oldest)
	}
	if after == ^uint64(0) {
		return nil, after, nil
	}
	sequence := after + 1
	if sequence < oldest {
		sequence = oldest
	}
	if sequence > log.next {
		return nil, after, nil
	}
	count := int(log.next - sequence + 1)
	if count > limit {
		count = limit
	}
	records := make([]ConflictIntrospectionRecord, count)
	for index := range records {
		records[index] = log.records[(log.start+int(sequence-oldest)+index)%log.capacity]
	}
	return records, records[len(records)-1].Sequence, nil
}

// MarshalBinary returns a deterministic checksummed snapshot of retained
// records. It never includes raw keys or values.
func (log *ConflictIntrospectionLog) MarshalBinary() ([]byte, error) {
	if log == nil || log.capacity < 1 || log.capacity > MaxConflictIntrospectionCapacity || len(log.records) != log.capacity || log.now == nil {
		return nil, ErrConflictIntrospectionInvalid
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return marshalConflictIntrospectionSnapshot(log.capacity, log.next, log.snapshotLocked())
}

// UnmarshalConflictIntrospectionLog restores a bounded conflict history.
func UnmarshalConflictIntrospectionLog(encoded []byte) (*ConflictIntrospectionLog, error) {
	if len(encoded) < len(conflictIntrospectionMagic)+1+4 || len(encoded) > MaxConflictIntrospectionSnapshotBytes {
		return nil, ErrConflictIntrospectionInvalid
	}
	checksumOffset := len(encoded) - 4
	wantChecksum := binary.BigEndian.Uint32(encoded[checksumOffset:])
	if crc32.Checksum(encoded[:checksumOffset], conflictIntrospectionCRC) != wantChecksum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrConflictIntrospectionInvalid)
	}
	if string(encoded[:len(conflictIntrospectionMagic)]) != string(conflictIntrospectionMagic[:]) {
		return nil, fmt.Errorf("%w: magic mismatch", ErrConflictIntrospectionInvalid)
	}
	offset := len(conflictIntrospectionMagic)
	if encoded[offset] != conflictIntrospectionVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrConflictIntrospectionInvalid, encoded[offset])
	}
	offset++
	capacityValue, err := readConflictIntrospectionUvarint(encoded[:checksumOffset], &offset)
	if err != nil || capacityValue < 1 || capacityValue > MaxConflictIntrospectionCapacity {
		return nil, ErrConflictIntrospectionInvalid
	}
	next, err := readConflictIntrospectionUvarint(encoded[:checksumOffset], &offset)
	if err != nil {
		return nil, ErrConflictIntrospectionInvalid
	}
	countValue, err := readConflictIntrospectionUvarint(encoded[:checksumOffset], &offset)
	if err != nil || countValue > capacityValue || countValue > MaxConflictIntrospectionCapacity {
		return nil, ErrConflictIntrospectionInvalid
	}
	count := int(countValue)
	if count > 0 && next < countValue {
		return nil, ErrConflictIntrospectionInvalid
	}
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{Capacity: int(capacityValue)})
	if err != nil {
		return nil, err
	}
	log.next = next
	log.count = count
	for index := 0; index < count; index++ {
		record, readErr := readConflictIntrospectionRecord(encoded[:checksumOffset], &offset)
		if readErr != nil {
			return nil, readErr
		}
		wantSequence := next - uint64(count) + uint64(index) + 1
		if record.Sequence != wantSequence || record.Sequence == 0 {
			return nil, fmt.Errorf("%w: non-contiguous sequence", ErrConflictIntrospectionInvalid)
		}
		if err := validateConflictIntrospectionRecord(record); err != nil {
			return nil, err
		}
		log.records[index] = record
	}
	if offset != checksumOffset {
		return nil, fmt.Errorf("%w: trailing bytes", ErrConflictIntrospectionInvalid)
	}
	return log, nil
}

func (log *ConflictIntrospectionLog) snapshotLocked() []ConflictIntrospectionRecord {
	records := make([]ConflictIntrospectionRecord, log.count)
	for index := range records {
		records[index] = log.records[(log.start+index)%log.capacity]
	}
	return records
}

func validateConflictIntrospectionInput(input ConflictIntrospectionInput) error {
	if input.Space == "" || len(input.Space) > MaxConflictIntrospectionStringBytes || len(input.Key) == 0 || len(input.Key) > MaxConflictIntrospectionKeyBytes {
		return ErrConflictIntrospectionInvalid
	}
	if len(input.Left.NodeID) > MaxConflictIntrospectionStringBytes || len(input.Right.NodeID) > MaxConflictIntrospectionStringBytes || len(input.Winner.NodeID) > MaxConflictIntrospectionStringBytes {
		return ErrConflictIntrospectionInvalid
	}
	if _, err := CompareConflictVersions(input.Left, input.Right); err != nil {
		return fmt.Errorf("%w: versions: %v", ErrConflictIntrospectionInvalid, err)
	}
	switch input.Outcome {
	case ConflictIntrospectionLeftWon:
		comparison, err := CompareConflictVersions(input.Winner, input.Left)
		if err != nil || comparison != 0 {
			return ErrConflictIntrospectionInvalid
		}
	case ConflictIntrospectionRightWon:
		comparison, err := CompareConflictVersions(input.Winner, input.Right)
		if err != nil || comparison != 0 {
			return ErrConflictIntrospectionInvalid
		}
	case ConflictIntrospectionRejected:
		if input.Winner.Timestamp != 0 || input.Winner.NodeID != "" || input.Winner.Sequence != 0 {
			return ErrConflictIntrospectionInvalid
		}
	default:
		return ErrConflictIntrospectionInvalid
	}
	return nil
}

func validateConflictIntrospectionRecord(record ConflictIntrospectionRecord) error {
	if record.Sequence == 0 || record.Space == "" || len(record.KeyDigest) != 32 {
		return ErrConflictIntrospectionInvalid
	}
	var digest [16]byte
	if _, err := hex.Decode(digest[:], []byte(record.KeyDigest)); err != nil {
		return ErrConflictIntrospectionInvalid
	}
	return validateConflictIntrospectionInput(ConflictIntrospectionInput{
		Space:   record.Space,
		Key:     []byte{1},
		Left:    record.Left,
		Right:   record.Right,
		Winner:  record.Winner,
		Outcome: record.Outcome,
	})
}

func marshalConflictIntrospectionSnapshot(capacity int, next uint64, records []ConflictIntrospectionRecord) ([]byte, error) {
	encoded := make([]byte, 0, 128+len(records)*96)
	encoded = append(encoded, conflictIntrospectionMagic[:]...)
	encoded = append(encoded, conflictIntrospectionVersion)
	encoded = appendConflictIntrospectionUvarint(encoded, uint64(capacity))
	encoded = appendConflictIntrospectionUvarint(encoded, next)
	encoded = appendConflictIntrospectionUvarint(encoded, uint64(len(records)))
	for _, record := range records {
		if err := validateConflictIntrospectionRecord(record); err != nil {
			return nil, err
		}
		encoded = appendConflictIntrospectionUvarint(encoded, record.Sequence)
		encoded = appendConflictIntrospectionVarint(encoded, record.AtUnixNano)
		encoded = appendConflictIntrospectionBytes(encoded, []byte(record.Space))
		encoded = appendConflictIntrospectionBytes(encoded, []byte(record.KeyDigest))
		encoded = append(encoded, byte(record.Outcome))
		encoded = appendConflictIntrospectionVersion(encoded, record.Left)
		encoded = appendConflictIntrospectionVersion(encoded, record.Right)
		encoded = appendConflictIntrospectionVersion(encoded, record.Winner)
		if len(encoded) > MaxConflictIntrospectionSnapshotBytes-4 {
			return nil, fmt.Errorf("%w: snapshot too large", ErrConflictIntrospectionInvalid)
		}
	}
	checksum := crc32.Checksum(encoded, conflictIntrospectionCRC)
	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], checksum)
	encoded = append(encoded, sum[:]...)
	return encoded, nil
}

func readConflictIntrospectionRecord(data []byte, offset *int) (ConflictIntrospectionRecord, error) {
	sequence, err := readConflictIntrospectionUvarint(data, offset)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	atUnixNano, err := readConflictIntrospectionVarint(data, offset)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	space, err := readConflictIntrospectionBytes(data, offset, MaxConflictIntrospectionStringBytes)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	digest, err := readConflictIntrospectionBytes(data, offset, 32)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	if *offset >= len(data) {
		return ConflictIntrospectionRecord{}, ErrConflictIntrospectionInvalid
	}
	outcome := ConflictIntrospectionOutcome(data[*offset])
	*offset++
	left, err := readConflictIntrospectionVersion(data, offset)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	right, err := readConflictIntrospectionVersion(data, offset)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	winner, err := readConflictIntrospectionVersion(data, offset)
	if err != nil {
		return ConflictIntrospectionRecord{}, err
	}
	return ConflictIntrospectionRecord{
		Sequence:   sequence,
		AtUnixNano: atUnixNano,
		Space:      string(space),
		KeyDigest:  string(digest),
		Left:       left,
		Right:      right,
		Winner:     winner,
		Outcome:    outcome,
	}, nil
}

func appendConflictIntrospectionVersion(dst []byte, version ConflictVersion) []byte {
	dst = appendConflictIntrospectionVarint(dst, version.Timestamp)
	dst = appendConflictIntrospectionBytes(dst, []byte(version.NodeID))
	return appendConflictIntrospectionUvarint(dst, version.Sequence)
}

func readConflictIntrospectionVersion(data []byte, offset *int) (ConflictVersion, error) {
	timestamp, err := readConflictIntrospectionVarint(data, offset)
	if err != nil {
		return ConflictVersion{}, err
	}
	nodeID, err := readConflictIntrospectionBytes(data, offset, MaxConflictIntrospectionStringBytes)
	if err != nil {
		return ConflictVersion{}, err
	}
	sequence, err := readConflictIntrospectionUvarint(data, offset)
	if err != nil {
		return ConflictVersion{}, err
	}
	return ConflictVersion{Timestamp: timestamp, NodeID: string(nodeID), Sequence: sequence}, nil
}

func appendConflictIntrospectionBytes(dst []byte, value []byte) []byte {
	dst = appendConflictIntrospectionUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func readConflictIntrospectionBytes(data []byte, offset *int, max int) ([]byte, error) {
	length, err := readConflictIntrospectionUvarint(data, offset)
	if err != nil || length > uint64(max) || length > uint64(len(data)-*offset) {
		return nil, ErrConflictIntrospectionInvalid
	}
	start := *offset
	*offset += int(length)
	return data[start:*offset], nil
}

func appendConflictIntrospectionUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:length]...)
}

func appendConflictIntrospectionVarint(dst []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutVarint(encoded[:], value)
	return append(dst, encoded[:length]...)
}

func readConflictIntrospectionUvarint(data []byte, offset *int) (uint64, error) {
	if *offset < 0 || *offset >= len(data) {
		return 0, ErrConflictIntrospectionInvalid
	}
	value, length := binary.Uvarint(data[*offset:])
	if length <= 0 {
		return 0, ErrConflictIntrospectionInvalid
	}
	*offset += length
	return value, nil
}

func readConflictIntrospectionVarint(data []byte, offset *int) (int64, error) {
	if *offset < 0 || *offset >= len(data) {
		return 0, ErrConflictIntrospectionInvalid
	}
	value, length := binary.Varint(data[*offset:])
	if length <= 0 {
		return 0, ErrConflictIntrospectionInvalid
	}
	*offset += length
	return value, nil
}
