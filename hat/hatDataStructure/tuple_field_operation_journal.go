package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	defaultTupleFieldJournalMaxRecordBytes = 1 << 20
	minTupleFieldJournalMaxRecordBytes     = 128
	maxTupleFieldJournalMaxRecordBytes     = 16 << 20
	defaultTupleFieldJournalSchemaVersion  = 1
	tupleFieldJournalFrameVersion          = 1
	tupleFieldJournalHeaderBytes           = 36
)

var (
	// ErrTupleFieldJournalCorrupt reports a malformed, truncated, or checksum
	// invalid journal frame.
	ErrTupleFieldJournalCorrupt = errors.New("hatDataStructure: corrupt tuple field journal")
	// ErrTupleFieldJournalSchemaMismatch reports replay against another tuple
	// schema version.
	ErrTupleFieldJournalSchemaMismatch = errors.New("hatDataStructure: tuple field journal schema mismatch")
	// ErrTupleFieldJournalRecordTooLarge reports a frame outside the configured
	// bounded record size.
	ErrTupleFieldJournalRecordTooLarge = errors.New("hatDataStructure: tuple field journal record is too large")
	// ErrTupleFieldJournalInvalid reports invalid journal options or a bad path.
	ErrTupleFieldJournalInvalid = errors.New("hatDataStructure: invalid tuple field journal")
	// ErrTupleFieldJournalSequence reports a generation gap in the journal.
	ErrTupleFieldJournalSequence = errors.New("hatDataStructure: tuple field journal sequence gap")
	// ErrTupleFieldJournalClosed reports use after Close.
	ErrTupleFieldJournalClosed = errors.New("hatDataStructure: tuple field journal is closed")
)

var tupleFieldJournalMagic = [4]byte{'H', 'T', 'J', '1'}

// TupleFieldOperationJournalOptions controls one durable tuple-operation
// journal. Writes are synchronized before Apply returns unless UnsafeNoSync is
// explicitly enabled. SchemaVersion lets a caller reject replay into a
// physically incompatible tuple layout.
type TupleFieldOperationJournalOptions struct {
	SchemaVersion  uint64
	UnsafeNoSync   bool
	MaxRecordBytes int
}

// TupleFieldOperationJournalRecord identifies one successfully appended batch.
type TupleFieldOperationJournalRecord struct {
	Sequence      uint64
	SchemaVersion uint64
	UpdateCount   int
}

// TupleFieldOperationJournal durably records typed tuple field batches and
// keeps the replayed tuple state in memory. It is safe for concurrent callers;
// each Apply is serialized and either fully published or not published.
type TupleFieldOperationJournal struct {
	mu             sync.Mutex
	file           *os.File
	state          TupleFieldOffsetCache
	sequence       uint64
	schemaVersion  uint64
	fieldCount     int
	maxRecordBytes int
	syncWrites     bool
	closed         bool
}

// OpenTupleFieldOperationJournal opens or creates a journal and replays all
// complete frames on top of initial. A malformed or incompatible frame is
// rejected before a journal handle is returned.
func OpenTupleFieldOperationJournal(path string, initial TupleFieldOffsetCache, options TupleFieldOperationJournalOptions) (*TupleFieldOperationJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrTupleFieldJournalInvalid)
	}
	schemaVersion := options.SchemaVersion
	if schemaVersion == 0 {
		schemaVersion = defaultTupleFieldJournalSchemaVersion
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = defaultTupleFieldJournalMaxRecordBytes
	}
	if maxRecordBytes < minTupleFieldJournalMaxRecordBytes || maxRecordBytes > maxTupleFieldJournalMaxRecordBytes || maxRecordBytes < tupleFieldJournalHeaderBytes {
		return nil, fmt.Errorf("%w: max record bytes %d outside %d..%d", ErrTupleFieldJournalInvalid, maxRecordBytes, minTupleFieldJournalMaxRecordBytes, maxTupleFieldJournalMaxRecordBytes)
	}
	state, err := cloneTupleFieldOffsetCache(initial)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	journal := &TupleFieldOperationJournal{
		file:           file,
		state:          state,
		schemaVersion:  schemaVersion,
		fieldCount:     state.FieldCount(),
		maxRecordBytes: maxRecordBytes,
		syncWrites:     !options.UnsafeNoSync,
	}
	if err := journal.replay(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return journal, nil
}

// Apply validates and appends one atomic field-operation batch. The state is
// updated only after the complete frame has been written and, by default,
// synchronized to stable storage.
func (journal *TupleFieldOperationJournal) Apply(updates []TupleFieldUpdate) (TupleFieldOperationJournalRecord, error) {
	if journal == nil {
		return TupleFieldOperationJournalRecord{}, ErrTupleFieldJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ensureOpenLocked(); err != nil {
		return TupleFieldOperationJournalRecord{}, err
	}
	if len(updates) == 0 {
		return TupleFieldOperationJournalRecord{Sequence: journal.sequence, SchemaVersion: journal.schemaVersion}, nil
	}
	next, err := journal.state.ApplyUpdates(updates)
	if err != nil {
		return TupleFieldOperationJournalRecord{}, err
	}
	if journal.sequence == ^uint64(0) {
		return TupleFieldOperationJournalRecord{}, ErrTupleFieldJournalSequence
	}
	sequence := journal.sequence + 1
	payload, err := encodeTupleFieldJournalUpdates(updates, journal.maxRecordBytes)
	if err != nil {
		return TupleFieldOperationJournalRecord{}, err
	}
	frame, err := marshalTupleFieldJournalFrame(sequence, journal.schemaVersion, journal.fieldCount, payload, journal.maxRecordBytes)
	if err != nil {
		return TupleFieldOperationJournalRecord{}, err
	}
	if err := journal.appendFrameLocked(frame); err != nil {
		return TupleFieldOperationJournalRecord{}, err
	}
	journal.state = next
	journal.sequence = sequence
	return TupleFieldOperationJournalRecord{Sequence: sequence, SchemaVersion: journal.schemaVersion, UpdateCount: len(updates)}, nil
}

// Snapshot returns an independently owned tuple state.
func (journal *TupleFieldOperationJournal) Snapshot() (TupleFieldOffsetCache, error) {
	if journal == nil {
		return TupleFieldOffsetCache{}, ErrTupleFieldJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.ensureOpenLocked(); err != nil {
		return TupleFieldOffsetCache{}, err
	}
	return cloneTupleFieldOffsetCache(journal.state)
}

// Sequence returns the last committed journal sequence. A nil journal returns
// zero; a closed journal retains its last sequence for diagnostics.
func (journal *TupleFieldOperationJournal) Sequence() uint64 {
	if journal == nil {
		return 0
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.sequence
}

// Close closes the journal and is safe to call repeatedly.
func (journal *TupleFieldOperationJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	return journal.file.Close()
}

func (journal *TupleFieldOperationJournal) replay() error {
	if _, err := journal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: seek: %v", ErrTupleFieldJournalCorrupt, err)
	}
	header := make([]byte, tupleFieldJournalHeaderBytes)
	for {
		read, err := io.ReadFull(journal.file, header)
		if err == io.EOF && read == 0 {
			break
		}
		if err != nil {
			return fmt.Errorf("%w: incomplete frame header", ErrTupleFieldJournalCorrupt)
		}
		sequence, schemaVersion, fieldCount, payloadLength, err := parseTupleFieldJournalHeader(header)
		if err != nil {
			return err
		}
		if schemaVersion != journal.schemaVersion {
			return fmt.Errorf("%w: got %d, want %d", ErrTupleFieldJournalSchemaMismatch, schemaVersion, journal.schemaVersion)
		}
		if fieldCount != journal.fieldCount {
			return fmt.Errorf("%w: field count got %d, want %d", ErrTupleFieldJournalSchemaMismatch, fieldCount, journal.fieldCount)
		}
		if sequence != journal.sequence+1 {
			return fmt.Errorf("%w: got %d after %d", ErrTupleFieldJournalSequence, sequence, journal.sequence)
		}
		if uint64(payloadLength) > uint64(journal.maxRecordBytes-tupleFieldJournalHeaderBytes) {
			return fmt.Errorf("%w: payload %d", ErrTupleFieldJournalRecordTooLarge, payloadLength)
		}
		payload := make([]byte, int(payloadLength))
		if _, err := io.ReadFull(journal.file, payload); err != nil {
			return fmt.Errorf("%w: incomplete frame payload", ErrTupleFieldJournalCorrupt)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[32:36]) {
			return fmt.Errorf("%w: checksum at sequence %d", ErrTupleFieldJournalCorrupt, sequence)
		}
		updates, err := decodeTupleFieldJournalUpdates(payload, journal.fieldCount)
		if err != nil {
			return fmt.Errorf("%w: sequence %d: %v", ErrTupleFieldJournalCorrupt, sequence, err)
		}
		next, err := journal.state.ApplyUpdates(updates)
		if err != nil {
			return fmt.Errorf("%w: sequence %d: %v", ErrTupleFieldJournalCorrupt, sequence, err)
		}
		journal.state = next
		journal.sequence = sequence
	}
	if _, err := journal.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%w: seek end: %v", ErrTupleFieldJournalCorrupt, err)
	}
	return nil
}

func (journal *TupleFieldOperationJournal) ensureOpenLocked() error {
	if journal == nil || journal.closed || journal.file == nil {
		return ErrTupleFieldJournalClosed
	}
	return nil
}

func (journal *TupleFieldOperationJournal) appendFrameLocked(frame []byte) error {
	offset, err := journal.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if err := writeTupleFieldJournalBytes(journal.file, frame); err != nil {
		_ = journal.file.Truncate(offset)
		_, _ = journal.file.Seek(0, io.SeekEnd)
		return err
	}
	if journal.syncWrites {
		if err := journal.file.Sync(); err != nil {
			_ = journal.file.Truncate(offset)
			_, _ = journal.file.Seek(0, io.SeekEnd)
			return err
		}
	}
	return nil
}

func cloneTupleFieldOffsetCache(source TupleFieldOffsetCache) (TupleFieldOffsetCache, error) {
	if len(source.offsets) == 0 || source.offsets[0] != 0 || source.offsets[len(source.offsets)-1] != uint32(len(source.data)) {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: initial tuple has no offsets", ErrTupleFieldJournalInvalid)
	}
	for index := 1; index < len(source.offsets); index++ {
		if source.offsets[index] < source.offsets[index-1] || source.offsets[index] > uint32(len(source.data)) {
			return TupleFieldOffsetCache{}, fmt.Errorf("%w: initial tuple offsets are invalid", ErrTupleFieldJournalInvalid)
		}
	}
	if source.valid != nil && len(source.valid) != len(source.offsets)-1 {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: initial tuple validity length is invalid", ErrTupleFieldJournalInvalid)
	}
	return TupleFieldOffsetCache{
		data:    append([]byte(nil), source.data...),
		offsets: append([]uint32(nil), source.offsets...),
		valid:   append([]bool(nil), source.valid...),
	}, nil
}

func marshalTupleFieldJournalFrame(sequence, schemaVersion uint64, fieldCount int, payload []byte, maxRecordBytes int) ([]byte, error) {
	if fieldCount < 0 || uint64(fieldCount) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: field count", ErrTupleFieldJournalInvalid)
	}
	if len(payload) > maxRecordBytes-tupleFieldJournalHeaderBytes || uint64(len(payload)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: payload %d", ErrTupleFieldJournalRecordTooLarge, len(payload))
	}
	header := make([]byte, tupleFieldJournalHeaderBytes)
	copy(header[0:4], tupleFieldJournalMagic[:])
	header[4] = tupleFieldJournalFrameVersion
	binary.BigEndian.PutUint16(header[6:8], tupleFieldJournalHeaderBytes)
	binary.BigEndian.PutUint64(header[8:16], sequence)
	binary.BigEndian.PutUint64(header[16:24], schemaVersion)
	binary.BigEndian.PutUint32(header[24:28], uint32(fieldCount))
	binary.BigEndian.PutUint32(header[28:32], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[32:36], crc32.ChecksumIEEE(payload))
	frame := make([]byte, 0, tupleFieldJournalHeaderBytes+len(payload))
	frame = append(frame, header...)
	frame = append(frame, payload...)
	return frame, nil
}

func parseTupleFieldJournalHeader(header []byte) (uint64, uint64, int, uint32, error) {
	if len(header) != tupleFieldJournalHeaderBytes || !equalTupleFieldJournalMagic(header[0:4]) {
		return 0, 0, 0, 0, fmt.Errorf("%w: magic or header length", ErrTupleFieldJournalCorrupt)
	}
	if header[4] != tupleFieldJournalFrameVersion || header[5] != 0 || binary.BigEndian.Uint16(header[6:8]) != tupleFieldJournalHeaderBytes {
		return 0, 0, 0, 0, fmt.Errorf("%w: unsupported frame header", ErrTupleFieldJournalCorrupt)
	}
	fieldCount := binary.BigEndian.Uint32(header[24:28])
	if uint64(fieldCount) > uint64(^uint(0)>>1) {
		return 0, 0, 0, 0, fmt.Errorf("%w: field count", ErrTupleFieldJournalCorrupt)
	}
	return binary.BigEndian.Uint64(header[8:16]), binary.BigEndian.Uint64(header[16:24]), int(fieldCount), binary.BigEndian.Uint32(header[28:32]), nil
}

func equalTupleFieldJournalMagic(value []byte) bool {
	return len(value) == len(tupleFieldJournalMagic) && value[0] == tupleFieldJournalMagic[0] && value[1] == tupleFieldJournalMagic[1] && value[2] == tupleFieldJournalMagic[2] && value[3] == tupleFieldJournalMagic[3]
}

func encodeTupleFieldJournalUpdates(updates []TupleFieldUpdate, maxRecordBytes int) ([]byte, error) {
	if uint64(len(updates)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: update count", ErrTupleFieldJournalRecordTooLarge)
	}
	limit := uint64(maxRecordBytes - tupleFieldJournalHeaderBytes)
	if limit > uint64(^uint32(0)) {
		limit = uint64(^uint32(0))
	}
	length := uint64(4)
	addLength := func(add uint64) error {
		if length > limit || add > limit-length {
			return fmt.Errorf("%w: payload %d", ErrTupleFieldJournalRecordTooLarge, length+add)
		}
		length += add
		return nil
	}
	for _, update := range updates {
		if err := addLength(5); err != nil {
			return nil, err
		}
		switch update.Kind {
		case TupleFieldSet:
			if err := addLength(4); err != nil {
				return nil, err
			}
			if err := addLength(uint64(len(update.Value))); err != nil {
				return nil, err
			}
		case TupleFieldSplice:
			if err := addLength(12); err != nil {
				return nil, err
			}
			if err := addLength(uint64(len(update.Insert))); err != nil {
				return nil, err
			}
		case TupleFieldAddInt64:
			if err := addLength(8); err != nil {
				return nil, err
			}
		default:
			return nil, ErrTupleFieldUpdateKind
		}
	}
	payload := make([]byte, int(length))
	position := 0
	binary.BigEndian.PutUint32(payload[position:position+4], uint32(len(updates)))
	position += 4
	for _, update := range updates {
		binary.BigEndian.PutUint32(payload[position:position+4], uint32(update.Index))
		position += 4
		payload[position] = byte(update.Kind)
		position++
		switch update.Kind {
		case TupleFieldSet:
			binary.BigEndian.PutUint32(payload[position:position+4], uint32(len(update.Value)))
			position += 4
			copy(payload[position:], update.Value)
			position += len(update.Value)
		case TupleFieldSplice:
			binary.BigEndian.PutUint32(payload[position:position+4], uint32(update.Start))
			position += 4
			binary.BigEndian.PutUint32(payload[position:position+4], uint32(update.Remove))
			position += 4
			binary.BigEndian.PutUint32(payload[position:position+4], uint32(len(update.Insert)))
			position += 4
			copy(payload[position:], update.Insert)
			position += len(update.Insert)
		case TupleFieldAddInt64:
			binary.BigEndian.PutUint64(payload[position:position+8], uint64(update.Delta))
			position += 8
		}
	}
	return payload, nil
}

func decodeTupleFieldJournalUpdates(payload []byte, fieldCount int) ([]TupleFieldUpdate, error) {
	if len(payload) < 4 {
		return nil, errors.New("payload is too short")
	}
	position := 0
	count := binary.BigEndian.Uint32(payload[position : position+4])
	position += 4
	if uint64(count) > uint64(fieldCount) {
		return nil, errors.New("update count exceeds field count")
	}
	updates := make([]TupleFieldUpdate, 0, int(count))
	for index := uint32(0); index < count; index++ {
		if len(payload)-position < 5 {
			return nil, errors.New("update header is truncated")
		}
		fieldIndex := binary.BigEndian.Uint32(payload[position : position+4])
		position += 4
		if uint64(fieldIndex) >= uint64(fieldCount) {
			return nil, errors.New("update field index is invalid")
		}
		kind := TupleFieldUpdateKind(payload[position])
		position++
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: kind}
		switch kind {
		case TupleFieldSet:
			value, next, err := readTupleFieldJournalBytes(payload, position)
			if err != nil {
				return nil, err
			}
			update.Value = value
			position = next
		case TupleFieldSplice:
			if len(payload)-position < 8 {
				return nil, errors.New("splice range is truncated")
			}
			start := binary.BigEndian.Uint32(payload[position : position+4])
			position += 4
			remove := binary.BigEndian.Uint32(payload[position : position+4])
			position += 4
			insert, next, err := readTupleFieldJournalBytes(payload, position)
			if err != nil {
				return nil, err
			}
			if uint64(start) > uint64(^uint(0)>>1) || uint64(remove) > uint64(^uint(0)>>1) {
				return nil, errors.New("splice range exceeds int")
			}
			update.Start = int(start)
			update.Remove = int(remove)
			update.Insert = insert
			position = next
		case TupleFieldAddInt64:
			if len(payload)-position < 8 {
				return nil, errors.New("add delta is truncated")
			}
			update.Delta = int64(binary.BigEndian.Uint64(payload[position : position+8]))
			position += 8
		default:
			return nil, errors.New("unsupported update kind")
		}
		updates = append(updates, update)
	}
	if position != len(payload) {
		return nil, errors.New("payload has trailing bytes")
	}
	return updates, nil
}

func readTupleFieldJournalBytes(payload []byte, position int) ([]byte, int, error) {
	if len(payload)-position < 4 {
		return nil, 0, errors.New("byte value length is truncated")
	}
	length := binary.BigEndian.Uint32(payload[position : position+4])
	position += 4
	if uint64(length) > uint64(len(payload)-position) {
		return nil, 0, errors.New("byte value is truncated")
	}
	end := position + int(length)
	return append([]byte(nil), payload[position:end]...), end, nil
}

func writeTupleFieldJournalBytes(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
