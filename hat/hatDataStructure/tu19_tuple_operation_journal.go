package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	TupleFieldOperationJournalVersion             uint16 = 1
	DefaultTupleFieldOperationJournalRecords             = 256
	MaxTupleFieldOperationJournalRecords                 = 65536
	DefaultTupleFieldOperationJournalBytes               = 8 << 20
	MaxTupleFieldOperationJournalBytes                   = 64 << 20
	MaxTupleFieldOperationJournalUpdates                 = 1024
	MaxTupleFieldOperationJournalValueBytes              = 1 << 20
	MaxTupleFieldOperationJournalOperationIDBytes        = 256
	MaxTupleFieldOperationJournalReplay                  = 65536
)

var (
	ErrTupleFieldOperationJournalNil          = errors.New("hatDataStructure: tuple operation journal is nil")
	ErrTupleFieldOperationJournalOptions      = errors.New("hatDataStructure: tuple operation journal options are invalid")
	ErrTupleFieldOperationJournalInvalid      = errors.New("hatDataStructure: tuple operation is invalid")
	ErrTupleFieldOperationJournalConflict     = errors.New("hatDataStructure: tuple operation ID conflicts")
	ErrTupleFieldOperationJournalLimit        = errors.New("hatDataStructure: tuple operation journal limit exceeded")
	ErrTupleFieldOperationJournalGap          = errors.New("hatDataStructure: tuple operation journal history gap")
	ErrTupleFieldOperationJournalFormat       = errors.New("hatDataStructure: tuple operation journal format is invalid")
	ErrTupleFieldOperationJournalChecksum     = errors.New("hatDataStructure: tuple operation journal checksum mismatch")
	ErrTupleFieldOperationJournalVersion      = errors.New("hatDataStructure: unsupported tuple operation journal version")
	ErrTupleFieldOperationJournalPersist      = errors.New("hatDataStructure: tuple operation journal persistence failed")
	ErrTupleFieldOperationJournalJoinSequence = errors.New("hatDataStructure: snapshot join sequence is unavailable")
	ErrTupleFieldOperationJournalJoinClosed   = errors.New("hatDataStructure: snapshot join is closed")
)

var tupleFieldOperationJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldOperationJournalOptions configures bounded durable operation
// records. Path enables atomic file-backed persistence; an empty path keeps
// the journal in memory.
type TupleFieldOperationJournalOptions struct {
	Path       string
	MaxRecords int
	MaxBytes   int
}

// TupleFieldOperation is one caller-supplied atomic tuple update batch.
type TupleFieldOperation struct {
	OperationID string
	Updates     []TupleFieldUpdate
}

// TupleFieldOperationRecord is the journal-assigned durable form of an
// operation. Sequence numbers are contiguous for retained records.
type TupleFieldOperationRecord struct {
	Sequence    uint64
	OperationID string
	Updates     []TupleFieldUpdate
}

// TupleFieldOperationJournalSnapshot is an immutable detached journal view.
type TupleFieldOperationJournalSnapshot struct {
	NextSequence     uint64
	CompactedThrough uint64
	Records          []TupleFieldOperationRecord
}

// TupleFieldOperationJournalJoin pins the journal history needed after a
// snapshot sequence. Close releases the pin and allows normal compaction to
// resume.
type TupleFieldOperationJournalJoin struct {
	journal          *TupleFieldOperationJournal
	snapshotSequence uint64
	mu               sync.RWMutex
	closed           bool
}

type tupleFieldOperationJournalState struct {
	nextSequence     uint64
	compactedThrough uint64
	encodedBytes     int
	records          []TupleFieldOperationRecord
}

// TupleFieldOperationJournal stores bounded, replayable tuple field updates.
// A path-backed journal publishes an update only after its checksummed
// snapshot has been atomically replaced and synced.
type TupleFieldOperationJournal struct {
	mu         sync.RWMutex
	path       string
	maxRecords int
	maxBytes   int
	state      tupleFieldOperationJournalState
	joinPins   map[uint64]int
}

// NewTupleFieldOperationJournal creates an empty bounded journal.
func NewTupleFieldOperationJournal(options TupleFieldOperationJournalOptions) (*TupleFieldOperationJournal, error) {
	options, err := normalizeTupleFieldOperationJournalOptions(options)
	if err != nil {
		return nil, err
	}
	return &TupleFieldOperationJournal{
		path:       options.Path,
		maxRecords: options.MaxRecords,
		maxBytes:   options.MaxBytes,
		state:      tupleFieldOperationJournalState{nextSequence: 1, encodedBytes: tupleFieldOperationJournalFixedBytes},
	}, nil
}

// BeginSnapshotJoin pins records after snapshotSequence until the returned
// join is closed. The caller must create a consistent snapshot at exactly
// snapshotSequence before replaying the returned journal delta.
func (journal *TupleFieldOperationJournal) BeginSnapshotJoin(snapshotSequence uint64) (*TupleFieldOperationJournalJoin, error) {
	if journal == nil {
		return nil, ErrTupleFieldOperationJournalNil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if snapshotSequence < journal.state.compactedThrough {
		return nil, ErrTupleFieldOperationJournalGap
	}
	latestSequence := journal.state.nextSequence - 1
	if snapshotSequence > latestSequence {
		return nil, ErrTupleFieldOperationJournalJoinSequence
	}
	if journal.joinPins == nil {
		journal.joinPins = make(map[uint64]int)
	}
	journal.joinPins[snapshotSequence]++
	return &TupleFieldOperationJournalJoin{
		journal:          journal,
		snapshotSequence: snapshotSequence,
	}, nil
}

// SnapshotSequence returns the journal sequence represented by the snapshot
// associated with this join.
func (join *TupleFieldOperationJournalJoin) SnapshotSequence() uint64 {
	if join == nil {
		return 0
	}
	return join.snapshotSequence
}

// Replay returns the retained WAL delta after the join snapshot sequence.
func (join *TupleFieldOperationJournalJoin) Replay(afterSequence uint64, limit int) ([]TupleFieldOperationRecord, error) {
	if join == nil {
		return nil, ErrTupleFieldOperationJournalJoinClosed
	}
	join.mu.RLock()
	defer join.mu.RUnlock()
	if join.closed {
		return nil, ErrTupleFieldOperationJournalJoinClosed
	}
	if afterSequence < join.snapshotSequence {
		return nil, ErrTupleFieldOperationJournalJoinSequence
	}
	return join.journal.Replay(afterSequence, limit)
}

// Close releases the join pin. It is safe to call Close more than once.
func (join *TupleFieldOperationJournalJoin) Close() error {
	if join == nil {
		return nil
	}
	join.mu.Lock()
	if join.closed {
		join.mu.Unlock()
		return nil
	}
	join.closed = true
	journal := join.journal
	join.mu.Unlock()
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	if count := journal.joinPins[join.snapshotSequence]; count <= 1 {
		delete(journal.joinPins, join.snapshotSequence)
	} else {
		journal.joinPins[join.snapshotSequence] = count - 1
	}
	if len(journal.joinPins) == 0 {
		journal.joinPins = nil
	}
	journal.mu.Unlock()
	return nil
}

// OpenTupleFieldOperationJournal opens a path-backed journal or creates an
// empty journal when the path does not exist.
func OpenTupleFieldOperationJournal(path string, options TupleFieldOperationJournalOptions) (*TupleFieldOperationJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrTupleFieldOperationJournalOptions
	}
	options.Path = path
	journal, err := NewTupleFieldOperationJournal(options)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return journal, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrTupleFieldOperationJournalPersist, path, err)
	}
	if err := journal.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return journal, nil
}

// Append validates and durably publishes one operation. Repeating an
// operation ID with identical updates is idempotent while the record is
// retained; reusing it with different updates is rejected.
func (journal *TupleFieldOperationJournal) Append(operation TupleFieldOperation) (TupleFieldOperationRecord, error) {
	if journal == nil {
		return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalNil
	}
	normalized, err := normalizeTupleFieldOperation(operation)
	if err != nil {
		return TupleFieldOperationRecord{}, err
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	for _, record := range journal.state.records {
		if record.OperationID != normalized.OperationID {
			continue
		}
		if tupleFieldUpdatesEqual(record.Updates, normalized.Updates) {
			return cloneTupleFieldOperationRecord(record), nil
		}
		return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalConflict
	}

	record := TupleFieldOperationRecord{
		Sequence:    journal.state.nextSequence,
		OperationID: normalized.OperationID,
		Updates:     normalized.Updates,
	}
	candidate := journal.state
	candidate.records = append(candidate.records, record)
	candidate.nextSequence++
	recordBytes, err := tupleFieldOperationRecordSize(record)
	if err != nil {
		return TupleFieldOperationRecord{}, err
	}
	candidate.encodedBytes += recordBytes
	for {
		if len(candidate.records) <= journal.maxRecords && candidate.encodedBytes <= journal.maxBytes {
			if journal.path != "" {
				encoded, marshalErr := marshalTupleFieldOperationJournalState(candidate)
				if marshalErr != nil {
					return TupleFieldOperationRecord{}, marshalErr
				}
				if err := persistTupleFieldOperationJournal(journal.path, encoded); err != nil {
					return TupleFieldOperationRecord{}, err
				}
			}
			journal.state = candidate
			return cloneTupleFieldOperationRecord(record), nil
		}
		if len(candidate.records) == 0 {
			return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalLimit
		}
		removed := candidate.records[0]
		if minimumPinned, pinned := journal.minimumSnapshotJoinSequenceLocked(); pinned && removed.Sequence > minimumPinned {
			return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalLimit
		}
		candidate.records = candidate.records[1:]
		candidate.compactedThrough = removed.Sequence
		removedBytes, sizeErr := tupleFieldOperationRecordSize(removed)
		if sizeErr != nil {
			return TupleFieldOperationRecord{}, sizeErr
		}
		candidate.encodedBytes -= removedBytes
		if len(candidate.records) == 0 && candidate.encodedBytes > journal.maxBytes {
			return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalLimit
		}
	}
}

func (journal *TupleFieldOperationJournal) minimumSnapshotJoinSequenceLocked() (uint64, bool) {
	var minimum uint64
	pinned := false
	for sequence := range journal.joinPins {
		if !pinned || sequence < minimum {
			minimum = sequence
			pinned = true
		}
	}
	return minimum, pinned
}

// Snapshot returns a detached view of the retained journal state.
func (journal *TupleFieldOperationJournal) Snapshot() TupleFieldOperationJournalSnapshot {
	if journal == nil {
		return TupleFieldOperationJournalSnapshot{}
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return TupleFieldOperationJournalSnapshot{
		NextSequence:     journal.state.nextSequence,
		CompactedThrough: journal.state.compactedThrough,
		Records:          cloneTupleFieldOperationRecords(journal.state.records),
	}
}

// Replay returns retained records after afterSequence. A zero limit returns
// all retained records. A compacted history gap fails closed.
func (journal *TupleFieldOperationJournal) Replay(afterSequence uint64, limit int) ([]TupleFieldOperationRecord, error) {
	if journal == nil {
		return nil, ErrTupleFieldOperationJournalNil
	}
	if limit < 0 || limit > MaxTupleFieldOperationJournalReplay {
		return nil, ErrTupleFieldOperationJournalLimit
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if afterSequence < journal.state.compactedThrough {
		return nil, ErrTupleFieldOperationJournalGap
	}
	start := 0
	if afterSequence > journal.state.compactedThrough {
		offset := afterSequence - journal.state.compactedThrough
		if offset >= uint64(len(journal.state.records)) {
			return []TupleFieldOperationRecord{}, nil
		}
		start = int(offset)
	}
	if start >= len(journal.state.records) {
		return []TupleFieldOperationRecord{}, nil
	}
	end := len(journal.state.records)
	if limit > 0 && limit < end-start {
		end = start + limit
	}
	return cloneTupleFieldOperationRecords(journal.state.records[start:end]), nil
}

// MarshalBinary returns a deterministic checksummed TJF1 snapshot.
func (journal *TupleFieldOperationJournal) MarshalBinary() ([]byte, error) {
	if journal == nil {
		return nil, ErrTupleFieldOperationJournalNil
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return marshalTupleFieldOperationJournalState(journal.state)
}

// UnmarshalBinary validates and atomically replaces the journal state.
func (journal *TupleFieldOperationJournal) UnmarshalBinary(data []byte) error {
	if journal == nil {
		return ErrTupleFieldOperationJournalNil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	state, err := unmarshalTupleFieldOperationJournalState(data, journal.maxRecords, journal.maxBytes)
	if err != nil {
		return err
	}
	journal.state = state
	return nil
}

// Save persists the current state when the journal was opened with Path.
func (journal *TupleFieldOperationJournal) Save() error {
	if journal == nil {
		return ErrTupleFieldOperationJournalNil
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if journal.path == "" {
		return ErrTupleFieldOperationJournalOptions
	}
	data, err := marshalTupleFieldOperationJournalState(journal.state)
	if err != nil {
		return err
	}
	return persistTupleFieldOperationJournal(journal.path, data)
}

// ReplayTupleFieldOperationRecords applies records in sequence order. The
// returned tuple is unchanged when any operation fails.
func ReplayTupleFieldOperationRecords(tuple TupleFieldOffsetCache, records []TupleFieldOperationRecord) (TupleFieldOffsetCache, error) {
	current := tuple
	var previous uint64
	for index, record := range records {
		if record.Sequence == 0 || (index > 0 && record.Sequence != previous+1) {
			return tuple, ErrTupleFieldOperationJournalInvalid
		}
		updates := cloneTupleFieldUpdates(record.Updates)
		next, err := current.ApplyUpdates(updates)
		if err != nil {
			return tuple, err
		}
		current = next
		previous = record.Sequence
	}
	return current, nil
}

func normalizeTupleFieldOperationJournalOptions(options TupleFieldOperationJournalOptions) (TupleFieldOperationJournalOptions, error) {
	options.Path = strings.TrimSpace(options.Path)
	if options.MaxRecords == 0 {
		options.MaxRecords = DefaultTupleFieldOperationJournalRecords
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultTupleFieldOperationJournalBytes
	}
	if options.MaxRecords < 1 || options.MaxRecords > MaxTupleFieldOperationJournalRecords ||
		options.MaxBytes < tupleFieldOperationJournalHeaderBytes+tupleFieldOperationJournalChecksumBytes ||
		options.MaxBytes > MaxTupleFieldOperationJournalBytes {
		return TupleFieldOperationJournalOptions{}, ErrTupleFieldOperationJournalOptions
	}
	return options, nil
}

func normalizeTupleFieldOperation(operation TupleFieldOperation) (TupleFieldOperation, error) {
	operation.OperationID = strings.TrimSpace(operation.OperationID)
	if operation.OperationID == "" || len(operation.OperationID) > MaxTupleFieldOperationJournalOperationIDBytes || strings.IndexByte(operation.OperationID, 0) >= 0 {
		return TupleFieldOperation{}, ErrTupleFieldOperationJournalInvalid
	}
	if len(operation.Updates) == 0 || len(operation.Updates) > MaxTupleFieldOperationJournalUpdates {
		return TupleFieldOperation{}, ErrTupleFieldOperationJournalInvalid
	}
	for updateIndex, update := range operation.Updates {
		if update.Index < 0 || update.Index > MaxTupleFieldOperationJournalUpdates*MaxTupleFieldOperationJournalUpdates {
			return TupleFieldOperation{}, fmt.Errorf("%w: update %d index", ErrTupleFieldOperationJournalInvalid, updateIndex)
		}
		for priorIndex := 0; priorIndex < updateIndex; priorIndex++ {
			if operation.Updates[priorIndex].Index == update.Index {
				return TupleFieldOperation{}, ErrTupleFieldOperationJournalInvalid
			}
		}
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > MaxTupleFieldOperationJournalValueBytes {
				return TupleFieldOperation{}, ErrTupleFieldOperationJournalLimit
			}
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 || len(update.Insert) > MaxTupleFieldOperationJournalValueBytes {
				return TupleFieldOperation{}, ErrTupleFieldOperationJournalInvalid
			}
		case TupleFieldAddInt64:
		default:
			return TupleFieldOperation{}, ErrTupleFieldOperationJournalInvalid
		}
	}
	operation.Updates = cloneTupleFieldUpdates(operation.Updates)
	return operation, nil
}

const (
	tupleFieldOperationJournalMagic           = "TJF1"
	tupleFieldOperationJournalHeaderBytes     = 4 + 2 + 2 + 8 + 8 + 4
	tupleFieldOperationJournalChecksumBytes   = 4
	tupleFieldOperationJournalFixedBytes      = tupleFieldOperationJournalHeaderBytes + tupleFieldOperationJournalChecksumBytes
	maxTupleFieldOperationJournalEncodedBytes = MaxTupleFieldOperationJournalBytes
)

func marshalTupleFieldOperationJournalState(state tupleFieldOperationJournalState) ([]byte, error) {
	if state.nextSequence == 0 || state.compactedThrough >= state.nextSequence {
		return nil, ErrTupleFieldOperationJournalInvalid
	}
	if len(state.records) > MaxTupleFieldOperationJournalRecords {
		return nil, ErrTupleFieldOperationJournalLimit
	}
	var buffer bytes.Buffer
	buffer.Grow(tupleFieldOperationJournalHeaderBytes + tupleFieldOperationJournalChecksumBytes)
	buffer.WriteString(tupleFieldOperationJournalMagic)
	writeU16(&buffer, TupleFieldOperationJournalVersion)
	writeU16(&buffer, 0)
	writeU64(&buffer, state.compactedThrough)
	writeU64(&buffer, state.nextSequence)
	writeU32(&buffer, uint32(len(state.records)))
	for _, record := range state.records {
		if err := encodeTupleFieldOperationRecord(&buffer, record); err != nil {
			return nil, err
		}
	}
	checksum := crc32.Checksum(buffer.Bytes(), tupleFieldOperationJournalCRCTable)
	writeU32(&buffer, checksum)
	if buffer.Len() > maxTupleFieldOperationJournalEncodedBytes {
		return nil, ErrTupleFieldOperationJournalLimit
	}
	return buffer.Bytes(), nil
}

func encodeTupleFieldOperationRecord(buffer *bytes.Buffer, record TupleFieldOperationRecord) error {
	if record.Sequence == 0 || record.OperationID == "" || len(record.OperationID) > MaxTupleFieldOperationJournalOperationIDBytes || len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldOperationJournalUpdates {
		return ErrTupleFieldOperationJournalInvalid
	}
	writeU64(buffer, record.Sequence)
	writeUvarint(buffer, uint64(len(record.OperationID)))
	buffer.WriteString(record.OperationID)
	writeUvarint(buffer, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		if err := encodeTupleFieldUpdate(buffer, update); err != nil {
			return err
		}
	}
	return nil
}

func encodeTupleFieldUpdate(buffer *bytes.Buffer, update TupleFieldUpdate) error {
	if update.Index < 0 || update.Index > MaxTupleFieldOperationJournalUpdates*MaxTupleFieldOperationJournalUpdates {
		return ErrTupleFieldOperationJournalInvalid
	}
	writeUvarint(buffer, uint64(update.Index))
	buffer.WriteByte(byte(update.Kind))
	switch update.Kind {
	case TupleFieldSet:
		if len(update.Value) > MaxTupleFieldOperationJournalValueBytes {
			return ErrTupleFieldOperationJournalLimit
		}
		writeBytes(buffer, update.Value)
	case TupleFieldSplice:
		if update.Start < 0 || update.Remove < 0 || len(update.Insert) > MaxTupleFieldOperationJournalValueBytes {
			return ErrTupleFieldOperationJournalInvalid
		}
		writeUvarint(buffer, uint64(update.Start))
		writeUvarint(buffer, uint64(update.Remove))
		writeBytes(buffer, update.Insert)
	case TupleFieldAddInt64:
		writeVarint(buffer, update.Delta)
	default:
		return ErrTupleFieldOperationJournalInvalid
	}
	return nil
}

func tupleFieldOperationRecordSize(record TupleFieldOperationRecord) (int, error) {
	if record.Sequence == 0 || record.OperationID == "" || len(record.OperationID) > MaxTupleFieldOperationJournalOperationIDBytes || len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldOperationJournalUpdates {
		return 0, ErrTupleFieldOperationJournalInvalid
	}
	size := 8 + tupleFieldOperationJournalVarintSize(uint64(len(record.OperationID))) + len(record.OperationID) + tupleFieldOperationJournalVarintSize(uint64(len(record.Updates)))
	for _, update := range record.Updates {
		if update.Index < 0 || update.Index > MaxTupleFieldOperationJournalUpdates*MaxTupleFieldOperationJournalUpdates {
			return 0, ErrTupleFieldOperationJournalInvalid
		}
		size += tupleFieldOperationJournalVarintSize(uint64(update.Index)) + 1
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > MaxTupleFieldOperationJournalValueBytes {
				return 0, ErrTupleFieldOperationJournalLimit
			}
			size += tupleFieldOperationJournalVarintSize(uint64(len(update.Value))) + len(update.Value)
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 || len(update.Insert) > MaxTupleFieldOperationJournalValueBytes {
				return 0, ErrTupleFieldOperationJournalInvalid
			}
			size += tupleFieldOperationJournalVarintSize(uint64(update.Start)) + tupleFieldOperationJournalVarintSize(uint64(update.Remove)) + tupleFieldOperationJournalVarintSize(uint64(len(update.Insert))) + len(update.Insert)
		case TupleFieldAddInt64:
			var encoded [binary.MaxVarintLen64]byte
			size += binary.PutVarint(encoded[:], update.Delta)
		default:
			return 0, ErrTupleFieldOperationJournalInvalid
		}
		if size > maxTupleFieldOperationJournalEncodedBytes {
			return 0, ErrTupleFieldOperationJournalLimit
		}
	}
	return size, nil
}

func tupleFieldOperationJournalVarintSize(value uint64) int {
	if value == 0 {
		return 1
	}
	return (tupleFieldOperationJournalBitsLen64(value) + 6) / 7
}

func tupleFieldOperationJournalBitsLen64(value uint64) int {
	bits := 0
	for value > 0 {
		bits++
		value >>= 1
	}
	return bits
}

func unmarshalTupleFieldOperationJournalState(data []byte, maxRecords, maxBytes int) (tupleFieldOperationJournalState, error) {
	if len(data) < tupleFieldOperationJournalHeaderBytes+tupleFieldOperationJournalChecksumBytes || len(data) > maxBytes {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalFormat
	}
	if string(data[:4]) != tupleFieldOperationJournalMagic {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalFormat
	}
	if binary.BigEndian.Uint16(data[4:6]) != TupleFieldOperationJournalVersion {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalVersion
	}
	if binary.BigEndian.Uint16(data[6:8]) != 0 {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalFormat
	}
	wantChecksum := binary.BigEndian.Uint32(data[len(data)-tupleFieldOperationJournalChecksumBytes:])
	gotChecksum := crc32.Checksum(data[:len(data)-tupleFieldOperationJournalChecksumBytes], tupleFieldOperationJournalCRCTable)
	if wantChecksum != gotChecksum {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalChecksum
	}
	state := tupleFieldOperationJournalState{
		compactedThrough: binary.BigEndian.Uint64(data[8:16]),
		nextSequence:     binary.BigEndian.Uint64(data[16:24]),
	}
	recordCount := binary.BigEndian.Uint32(data[24:28])
	if recordCount > uint32(maxRecords) || recordCount > MaxTupleFieldOperationJournalRecords || state.nextSequence == 0 || state.compactedThrough >= state.nextSequence {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalLimit
	}
	reader := tupleFieldOperationJournalReader{data: data[28 : len(data)-tupleFieldOperationJournalChecksumBytes]}
	state.records = make([]TupleFieldOperationRecord, 0, int(recordCount))
	for index := uint32(0); index < recordCount; index++ {
		record, err := reader.readRecord()
		if err != nil {
			return tupleFieldOperationJournalState{}, err
		}
		wantSequence := state.compactedThrough + uint64(index) + 1
		if record.Sequence != wantSequence {
			return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalFormat
		}
		state.records = append(state.records, record)
	}
	if reader.remaining() != 0 || (recordCount > 0 && state.records[len(state.records)-1].Sequence >= state.nextSequence) {
		return tupleFieldOperationJournalState{}, ErrTupleFieldOperationJournalFormat
	}
	return state, nil
}

type tupleFieldOperationJournalReader struct {
	data []byte
	pos  int
}

func (reader *tupleFieldOperationJournalReader) readRecord() (TupleFieldOperationRecord, error) {
	sequence, err := reader.readU64()
	if err != nil {
		return TupleFieldOperationRecord{}, err
	}
	operationID, err := reader.readString(MaxTupleFieldOperationJournalOperationIDBytes)
	if err != nil || operationID == "" || strings.IndexByte(operationID, 0) >= 0 {
		return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalFormat
	}
	count, err := reader.readUvarint()
	if err != nil || count == 0 || count > MaxTupleFieldOperationJournalUpdates {
		return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalFormat
	}
	updates := make([]TupleFieldUpdate, 0, int(count))
	for index := uint64(0); index < count; index++ {
		update, readErr := reader.readUpdate()
		if readErr != nil {
			return TupleFieldOperationRecord{}, readErr
		}
		for _, prior := range updates {
			if prior.Index == update.Index {
				return TupleFieldOperationRecord{}, ErrTupleFieldOperationJournalFormat
			}
		}
		updates = append(updates, update)
	}
	return TupleFieldOperationRecord{Sequence: sequence, OperationID: operationID, Updates: updates}, nil
}

func (reader *tupleFieldOperationJournalReader) readUpdate() (TupleFieldUpdate, error) {
	index, err := reader.readUvarint()
	if err != nil || index > MaxTupleFieldOperationJournalUpdates*MaxTupleFieldOperationJournalUpdates {
		return TupleFieldUpdate{}, ErrTupleFieldOperationJournalFormat
	}
	kind, err := reader.readByte()
	if err != nil {
		return TupleFieldUpdate{}, err
	}
	update := TupleFieldUpdate{Index: int(index), Kind: TupleFieldUpdateKind(kind)}
	switch update.Kind {
	case TupleFieldSet:
		update.Value, err = reader.readBytes(MaxTupleFieldOperationJournalValueBytes)
	case TupleFieldSplice:
		var start, remove uint64
		start, err = reader.readUvarint()
		if err == nil {
			remove, err = reader.readUvarint()
		}
		if err == nil {
			update.Insert, err = reader.readBytes(MaxTupleFieldOperationJournalValueBytes)
		}
		if start > uint64(^uint(0)>>1) || remove > uint64(^uint(0)>>1) {
			err = ErrTupleFieldOperationJournalFormat
		}
		update.Start, update.Remove = int(start), int(remove)
	case TupleFieldAddInt64:
		update.Delta, err = reader.readVarint()
	default:
		return TupleFieldUpdate{}, ErrTupleFieldOperationJournalFormat
	}
	if err != nil {
		return TupleFieldUpdate{}, err
	}
	return update, nil
}

func (reader *tupleFieldOperationJournalReader) readU64() (uint64, error) {
	if len(reader.data)-reader.pos < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	value := binary.BigEndian.Uint64(reader.data[reader.pos : reader.pos+8])
	reader.pos += 8
	return value, nil
}

func (reader *tupleFieldOperationJournalReader) readByte() (byte, error) {
	if reader.pos >= len(reader.data) {
		return 0, io.ErrUnexpectedEOF
	}
	value := reader.data[reader.pos]
	reader.pos++
	return value, nil
}

func (reader *tupleFieldOperationJournalReader) readUvarint() (uint64, error) {
	value, size := binary.Uvarint(reader.data[reader.pos:])
	if size == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if size < 0 {
		return 0, ErrTupleFieldOperationJournalFormat
	}
	reader.pos += size
	return value, nil
}

func (reader *tupleFieldOperationJournalReader) readVarint() (int64, error) {
	value, size := binary.Varint(reader.data[reader.pos:])
	if size == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if size < 0 {
		return 0, ErrTupleFieldOperationJournalFormat
	}
	reader.pos += size
	return value, nil
}

func (reader *tupleFieldOperationJournalReader) readBytes(max int) ([]byte, error) {
	length, err := reader.readUvarint()
	if err != nil || length > uint64(max) || length > uint64(len(reader.data)-reader.pos) {
		return nil, ErrTupleFieldOperationJournalFormat
	}
	value := append([]byte(nil), reader.data[reader.pos:reader.pos+int(length)]...)
	reader.pos += int(length)
	return value, nil
}

func (reader *tupleFieldOperationJournalReader) readString(max int) (string, error) {
	value, err := reader.readBytes(max)
	return string(value), err
}

func (reader *tupleFieldOperationJournalReader) remaining() int {
	return len(reader.data) - reader.pos
}

func persistTupleFieldOperationJournal(path string, data []byte) error {
	directory := filepath.Dir(path)
	base := filepath.Base(path)
	temporary, err := os.CreateTemp(directory, "."+base+".tmp-")
	if err != nil {
		return fmt.Errorf("%w: create temporary file: %v", ErrTupleFieldOperationJournalPersist, err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if chmodErr := temporary.Chmod(0o600); chmodErr != nil {
		err = chmodErr
	} else {
		var written int
		written, err = temporary.Write(data)
		if err == nil && written != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("%w: write temporary file: %v", ErrTupleFieldOperationJournalPersist, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("%w: replace journal: %v", ErrTupleFieldOperationJournalPersist, err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("%w: open journal directory: %v", ErrTupleFieldOperationJournalPersist, err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return fmt.Errorf("%w: sync journal directory: %v", ErrTupleFieldOperationJournalPersist, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: close journal directory: %v", ErrTupleFieldOperationJournalPersist, closeErr)
	}
	return nil
}

func cloneTupleFieldOperationRecords(records []TupleFieldOperationRecord) []TupleFieldOperationRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]TupleFieldOperationRecord, len(records))
	for index, record := range records {
		cloned[index] = cloneTupleFieldOperationRecord(record)
	}
	return cloned
}

func cloneTupleFieldOperationRecord(record TupleFieldOperationRecord) TupleFieldOperationRecord {
	record.Updates = cloneTupleFieldUpdates(record.Updates)
	return record
}

func cloneTupleFieldUpdates(updates []TupleFieldUpdate) []TupleFieldUpdate {
	if len(updates) == 0 {
		return nil
	}
	cloned := make([]TupleFieldUpdate, len(updates))
	for index, update := range updates {
		cloned[index] = update
		cloned[index].Value = append([]byte(nil), update.Value...)
		cloned[index].Insert = append([]byte(nil), update.Insert...)
	}
	return cloned
}

func tupleFieldUpdatesEqual(left, right []TupleFieldUpdate) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Index != right[index].Index || left[index].Kind != right[index].Kind ||
			left[index].Start != right[index].Start || left[index].Remove != right[index].Remove ||
			left[index].Delta != right[index].Delta || !bytes.Equal(left[index].Value, right[index].Value) ||
			!bytes.Equal(left[index].Insert, right[index].Insert) {
			return false
		}
	}
	return true
}

func writeU16(buffer *bytes.Buffer, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeU32(buffer *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeU64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}

func writeUvarint(buffer *bytes.Buffer, value uint64) {
	var encoded [binary.MaxVarintLen64]byte
	buffer.Write(encoded[:binary.PutUvarint(encoded[:], value)])
}

func writeVarint(buffer *bytes.Buffer, value int64) {
	var encoded [binary.MaxVarintLen64]byte
	buffer.Write(encoded[:binary.PutVarint(encoded[:], value)])
}

func writeBytes(buffer *bytes.Buffer, value []byte) {
	writeUvarint(buffer, uint64(len(value)))
	buffer.Write(value)
}
