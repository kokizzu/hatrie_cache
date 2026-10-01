package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

const (
	// DefaultTupleFieldUpdateJournalRecordBytes bounds one record when a caller
	// does not provide a smaller journal or replay limit.
	DefaultTupleFieldUpdateJournalRecordBytes = 64 << 10
	// MaxTupleFieldUpdateJournalRecordBytes is the hard safety bound for one
	// encoded field-operation record, including its CRC.
	MaxTupleFieldUpdateJournalRecordBytes = 1 << 20
	// MaxTupleFieldUpdateJournalUpdates bounds the number of operations in one
	// atomic tuple update batch.
	MaxTupleFieldUpdateJournalUpdates = 1024

	tupleFieldUpdateJournalWireVersion byte = 1
)

var (
	// ErrTupleFieldUpdateJournalNil indicates a missing writer, reader, callback,
	// or nil journal receiver.
	ErrTupleFieldUpdateJournalNil = errors.New("hatDataStructure: tuple update journal argument is nil")
	// ErrTupleFieldUpdateJournalInvalid indicates a semantically invalid record.
	ErrTupleFieldUpdateJournalInvalid = errors.New("hatDataStructure: tuple update journal record is invalid")
	// ErrTupleFieldUpdateJournalWire indicates a bad magic, version, checksum,
	// or malformed field encoding.
	ErrTupleFieldUpdateJournalWire = errors.New("hatDataStructure: tuple update journal wire is invalid")
	// ErrTupleFieldUpdateJournalTruncated indicates an incomplete record or
	// frame. A recovery caller must not apply that record.
	ErrTupleFieldUpdateJournalTruncated = errors.New("hatDataStructure: tuple update journal record is truncated")
	// ErrTupleFieldUpdateJournalLimit indicates a configured or encoded bound was
	// exceeded before an unbounded allocation could occur.
	ErrTupleFieldUpdateJournalLimit = errors.New("hatDataStructure: tuple update journal limit exceeded")
	// ErrTupleFieldUpdateJournalSequence indicates non-monotonic replay or an
	// exhausted append sequence.
	ErrTupleFieldUpdateJournalSequence = errors.New("hatDataStructure: tuple update journal sequence is invalid")
	// ErrTupleFieldUpdateJournalSchemaMismatch prevents applying a record to a
	// tuple from a different physical schema version.
	ErrTupleFieldUpdateJournalSchemaMismatch = errors.New("hatDataStructure: tuple update journal schema version mismatch")
	// ErrTupleFieldUpdateJournalWriter indicates a permanent append failure. A
	// partial write or failed sync poisons the journal so callers cannot append
	// after a potentially torn frame.
	ErrTupleFieldUpdateJournalWriter = errors.New("hatDataStructure: tuple update journal writer failed")
)

var tupleFieldUpdateJournalMagic = [4]byte{'H', 'T', 'J', '1'}
var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalRecord is one versioned, replayable tuple mutation.
// TupleID is opaque to this package and lets a caller route the record to its
// owning row or space. Updates are applied atomically by
// ApplyTupleFieldUpdateJournalRecord.
type TupleFieldUpdateJournalRecord struct {
	Sequence      uint64
	TupleID       uint64
	SchemaVersion uint64
	Updates       []TupleFieldUpdate
}

// TupleFieldUpdateJournalOptions configures a writer-backed journal.
// MaxRecordBytes is inclusive of the encoded record checksum. InitialSequence
// is the first sequence assigned; zero selects one. By default an fsync-like
// Sync call is made after every successful append when the writer exposes
// Sync() error. NoSync disables that call for batching or benchmarks.
type TupleFieldUpdateJournalOptions struct {
	MaxRecordBytes  int
	InitialSequence uint64
	NoSync          bool
}

// TupleFieldUpdateJournalReplayOptions configures strict framed replay.
type TupleFieldUpdateJournalReplayOptions struct {
	MaxRecordBytes int
}

// TupleFieldUpdateJournal appends length-framed HTJ1 records to a caller-owned
// writer. The journal does not own or close the writer. A failed write or sync
// permanently stops appends until the caller creates a new journal after
// repairing or replacing the underlying storage.
type TupleFieldUpdateJournal struct {
	mu             sync.Mutex
	writer         io.Writer
	syncer         interface{ Sync() error }
	maxRecordBytes int
	nextSequence   uint64
	noSync         bool
	failed         error
}

// NewTupleFieldUpdateJournal creates a bounded durable-record writer. Passing
// an *os.File gives the default append path a Sync call after every record;
// callers using a buffered or transactional store can set NoSync and call
// their own durability barrier after a batch.
func NewTupleFieldUpdateJournal(writer io.Writer, options TupleFieldUpdateJournalOptions) (*TupleFieldUpdateJournal, error) {
	if writer == nil {
		return nil, ErrTupleFieldUpdateJournalNil
	}
	maxRecordBytes, err := normalizeTupleFieldUpdateJournalRecordBytes(options.MaxRecordBytes)
	if err != nil {
		return nil, err
	}
	sequence := options.InitialSequence
	if sequence == 0 {
		sequence = 1
	}
	journal := &TupleFieldUpdateJournal{
		writer:         writer,
		maxRecordBytes: maxRecordBytes,
		nextSequence:   sequence,
		noSync:         options.NoSync,
	}
	if syncer, ok := writer.(interface{ Sync() error }); ok {
		journal.syncer = syncer
	}
	return journal, nil
}

// Append validates and writes one length-framed record. The sequence is
// advanced only after the complete frame and configured durability barrier
// succeed.
func (journal *TupleFieldUpdateJournal) Append(tupleID, schemaVersion uint64, updates []TupleFieldUpdate) (TupleFieldUpdateJournalRecord, error) {
	if journal == nil {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalNil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.failed != nil {
		return TupleFieldUpdateJournalRecord{}, journal.failed
	}
	if journal.nextSequence == 0 {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalSequence
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence:      journal.nextSequence,
		TupleID:       tupleID,
		SchemaVersion: schemaVersion,
		Updates:       updates,
	}
	recordSize, err := tupleFieldUpdateJournalRecordSize(record, journal.maxRecordBytes)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	const frameLengthCapacity = binary.MaxVarintLen64
	frame := make([]byte, frameLengthCapacity+recordSize)
	payload, err := encodeTupleFieldUpdateJournalRecord(frame[frameLengthCapacity:frameLengthCapacity], record)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	frameLength := binary.PutUvarint(frame[:frameLengthCapacity], uint64(len(payload)))
	copy(frame[frameLength:], payload)
	frame = frame[:frameLength+len(payload)]
	if err := writeTupleFieldUpdateJournalBytes(journal.writer, frame); err != nil {
		journal.failed = fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalWriter, err)
		return TupleFieldUpdateJournalRecord{}, journal.failed
	}
	if !journal.noSync && journal.syncer != nil {
		if err := journal.syncer.Sync(); err != nil {
			journal.failed = fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalWriter, err)
			return TupleFieldUpdateJournalRecord{}, journal.failed
		}
	}
	journal.nextSequence++
	return record, nil
}

// Sync explicitly completes a durability barrier when NoSync is enabled or
// when a caller wants to flush a batch early.
func (journal *TupleFieldUpdateJournal) Sync() error {
	if journal == nil {
		return ErrTupleFieldUpdateJournalNil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.failed != nil {
		return journal.failed
	}
	if journal.syncer == nil {
		return nil
	}
	if err := journal.syncer.Sync(); err != nil {
		journal.failed = fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalWriter, err)
		return journal.failed
	}
	return nil
}

// LastSequence returns the last successfully appended sequence, or zero when
// no append has completed.
func (journal *TupleFieldUpdateJournal) LastSequence() uint64 {
	if journal == nil {
		return 0
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.nextSequence - 1
}

// MarshalTupleFieldUpdateJournalRecord encodes a bounded checksummed HTJ1
// record. The returned bytes are owned by the caller.
func MarshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	return marshalTupleFieldUpdateJournalRecord(record, DefaultTupleFieldUpdateJournalRecordBytes)
}

func marshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord, maxRecordBytes int) ([]byte, error) {
	maxRecordBytes, err := normalizeTupleFieldUpdateJournalRecordBytes(maxRecordBytes)
	if err != nil {
		return nil, err
	}
	size, err := tupleFieldUpdateJournalRecordSize(record, maxRecordBytes)
	if err != nil {
		return nil, err
	}
	return encodeTupleFieldUpdateJournalRecord(make([]byte, 0, size), record)
}

func encodeTupleFieldUpdateJournalRecord(encoded []byte, record TupleFieldUpdateJournalRecord) ([]byte, error) {
	encoded = append(encoded, tupleFieldUpdateJournalMagic[:]...)
	encoded = append(encoded, tupleFieldUpdateJournalWireVersion)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, record.Sequence)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, record.TupleID)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, record.SchemaVersion)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		encoded = append(encoded, byte(update.Kind))
		encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Index))
		switch update.Kind {
		case TupleFieldSet:
			encoded = appendTupleFieldUpdateJournalBytes(encoded, update.Value)
		case TupleFieldSplice:
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Start))
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Remove))
			encoded = appendTupleFieldUpdateJournalBytes(encoded, update.Insert)
		case TupleFieldAddInt64:
			encoded = appendTupleFieldUpdateJournalVarint(encoded, update.Delta)
		}
	}
	checksum := crc32.Checksum(encoded, tupleFieldUpdateJournalCRCTable)
	var checksumBytes [4]byte
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

// UnmarshalTupleFieldUpdateJournalRecord strictly validates and owns a decoded
// HTJ1 record. It verifies the CRC before exposing any update to a caller.
func UnmarshalTupleFieldUpdateJournalRecord(encoded []byte) (TupleFieldUpdateJournalRecord, error) {
	return unmarshalTupleFieldUpdateJournalRecord(encoded, DefaultTupleFieldUpdateJournalRecordBytes)
}

func unmarshalTupleFieldUpdateJournalRecord(encoded []byte, maxRecordBytes int) (TupleFieldUpdateJournalRecord, error) {
	maxRecordBytes, err := normalizeTupleFieldUpdateJournalRecordBytes(maxRecordBytes)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	if len(encoded) > maxRecordBytes {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	if len(encoded) < len(tupleFieldUpdateJournalMagic)+1+4 {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
	}
	if string(encoded[:len(tupleFieldUpdateJournalMagic)]) != string(tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalWire
	}
	if encoded[len(tupleFieldUpdateJournalMagic)] != tupleFieldUpdateJournalWireVersion {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalWire
	}
	bodyEnd := len(encoded) - 4
	wantChecksum := binary.BigEndian.Uint32(encoded[bodyEnd:])
	if gotChecksum := crc32.Checksum(encoded[:bodyEnd], tupleFieldUpdateJournalCRCTable); gotChecksum != wantChecksum {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalWire
	}
	offset := len(tupleFieldUpdateJournalMagic) + 1
	sequence, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	tupleID, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	schemaVersion, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	updateCount, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	if updateCount > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	updates := make([]TupleFieldUpdate, int(updateCount))
	for index := range updates {
		if offset >= bodyEnd {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
		}
		kind := TupleFieldUpdateKind(encoded[offset])
		offset++
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		if fieldIndex > uint64(^uint(0)>>1) {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
		}
		updates[index].Index = int(fieldIndex)
		updates[index].Kind = kind
		switch kind {
		case TupleFieldSet:
			updates[index].Value, err = readTupleFieldUpdateJournalBytes(encoded[:bodyEnd], &offset)
		case TupleFieldSplice:
			start, startErr := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
			if startErr != nil {
				return TupleFieldUpdateJournalRecord{}, startErr
			}
			remove, removeErr := readTupleFieldUpdateJournalUvarint(encoded[:bodyEnd], &offset)
			if removeErr != nil {
				return TupleFieldUpdateJournalRecord{}, removeErr
			}
			if start > uint64(^uint(0)>>1) || remove > uint64(^uint(0)>>1) {
				return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
			}
			updates[index].Start = int(start)
			updates[index].Remove = int(remove)
			updates[index].Insert, err = readTupleFieldUpdateJournalBytes(encoded[:bodyEnd], &offset)
		case TupleFieldAddInt64:
			updates[index].Delta, err = readTupleFieldUpdateJournalVarint(encoded[:bodyEnd], &offset)
		default:
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalWire
		}
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
	}
	if offset != bodyEnd {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalWire
	}
	record := TupleFieldUpdateJournalRecord{Sequence: sequence, TupleID: tupleID, SchemaVersion: schemaVersion, Updates: updates}
	if err := validateTupleFieldUpdateJournalRecord(record, maxRecordBytes); err != nil {
		if errors.Is(err, ErrTupleFieldUpdateJournalLimit) {
			return TupleFieldUpdateJournalRecord{}, err
		}
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalWire, err)
	}
	return record, nil
}

// ApplyTupleFieldUpdateJournalRecord validates schema identity and applies one
// record copy-on-write. On any error the input tuple remains unchanged.
func ApplyTupleFieldUpdateJournalRecord(tuple VersionedTuple, format TupleFormat, record TupleFieldUpdateJournalRecord) (VersionedTuple, error) {
	if err := validateTupleFieldUpdateJournalRecord(record, MaxTupleFieldUpdateJournalRecordBytes); err != nil {
		return VersionedTuple{}, err
	}
	if record.SchemaVersion != format.Version() || record.SchemaVersion != tuple.Version() {
		return VersionedTuple{}, ErrTupleFieldUpdateJournalSchemaMismatch
	}
	if err := tuple.Validate(format); err != nil {
		return VersionedTuple{}, err
	}
	return tuple.ApplyUpdates(format, record.Updates)
}

// ReplayTupleFieldUpdateJournal reads length-framed records in order. Records
// at or before afterSequence are validated and skipped; later records are
// passed to apply only after checksum, bounds, and sequence validation. A
// truncated tail is an error rather than an implicitly applied partial record.
func ReplayTupleFieldUpdateJournal(reader io.Reader, afterSequence uint64, options TupleFieldUpdateJournalReplayOptions, apply func(TupleFieldUpdateJournalRecord) error) (uint64, error) {
	if reader == nil || apply == nil {
		return afterSequence, ErrTupleFieldUpdateJournalNil
	}
	maxRecordBytes, err := normalizeTupleFieldUpdateJournalRecordBytes(options.MaxRecordBytes)
	if err != nil {
		return afterSequence, err
	}
	lastEmitted := afterSequence
	var previous uint64
	havePrevious := false
	var payload []byte
	for {
		frameLength, readErr := readTupleFieldUpdateJournalFrameLength(reader)
		if readErr == io.EOF {
			return lastEmitted, nil
		}
		if readErr != nil {
			return lastEmitted, readErr
		}
		if frameLength == 0 || frameLength > uint64(maxRecordBytes) {
			return lastEmitted, ErrTupleFieldUpdateJournalLimit
		}
		if cap(payload) < int(frameLength) {
			payload = make([]byte, int(frameLength))
		} else {
			payload = payload[:int(frameLength)]
		}
		if _, err := io.ReadFull(reader, payload); err != nil {
			return lastEmitted, ErrTupleFieldUpdateJournalTruncated
		}
		record, err := unmarshalTupleFieldUpdateJournalRecord(payload, maxRecordBytes)
		if err != nil {
			return lastEmitted, err
		}
		if havePrevious && record.Sequence <= previous {
			return lastEmitted, ErrTupleFieldUpdateJournalSequence
		}
		havePrevious = true
		previous = record.Sequence
		if record.Sequence <= afterSequence {
			continue
		}
		if err := apply(record); err != nil {
			return lastEmitted, err
		}
		lastEmitted = record.Sequence
	}
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord, maxRecordBytes int) error {
	if record.Sequence == 0 || record.SchemaVersion == 0 || len(record.Updates) == 0 {
		return ErrTupleFieldUpdateJournalInvalid
	}
	if len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return ErrTupleFieldUpdateJournalLimit
	}
	for index, update := range record.Updates {
		if update.Index < 0 {
			return fmt.Errorf("%w: update %d has negative index", ErrTupleFieldUpdateJournalInvalid, index)
		}
		for previous := 0; previous < index; previous++ {
			if record.Updates[previous].Index == update.Index {
				return ErrTupleFieldUpdateJournalInvalid
			}
		}
		switch update.Kind {
		case TupleFieldSet:
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 {
				return ErrTupleFieldUpdateJournalInvalid
			}
		case TupleFieldAddInt64:
		default:
			return ErrTupleFieldUpdateJournalInvalid
		}
	}
	_, err := tupleFieldUpdateJournalRecordSize(record, maxRecordBytes)
	return err
}

func tupleFieldUpdateJournalRecordSize(record TupleFieldUpdateJournalRecord, maxRecordBytes int) (int, error) {
	if err := validateTupleFieldUpdateJournalRecordShape(record); err != nil {
		return 0, err
	}
	size := len(tupleFieldUpdateJournalMagic) + 1
	for _, value := range []uint64{record.Sequence, record.TupleID, record.SchemaVersion, uint64(len(record.Updates))} {
		if err := addTupleFieldUpdateJournalSize(&size, tupleFieldUpdateJournalUvarintSize(value), maxRecordBytes); err != nil {
			return 0, err
		}
	}
	for _, update := range record.Updates {
		if err := addTupleFieldUpdateJournalSize(&size, 1+tupleFieldUpdateJournalUvarintSize(uint64(update.Index)), maxRecordBytes); err != nil {
			return 0, err
		}
		switch update.Kind {
		case TupleFieldSet:
			if err := addTupleFieldUpdateJournalBytesSize(&size, len(update.Value), maxRecordBytes); err != nil {
				return 0, err
			}
		case TupleFieldSplice:
			if err := addTupleFieldUpdateJournalSize(&size, tupleFieldUpdateJournalUvarintSize(uint64(update.Start))+tupleFieldUpdateJournalUvarintSize(uint64(update.Remove)), maxRecordBytes); err != nil {
				return 0, err
			}
			if err := addTupleFieldUpdateJournalBytesSize(&size, len(update.Insert), maxRecordBytes); err != nil {
				return 0, err
			}
		case TupleFieldAddInt64:
			if err := addTupleFieldUpdateJournalSize(&size, tupleFieldUpdateJournalVarintSize(update.Delta), maxRecordBytes); err != nil {
				return 0, err
			}
		}
	}
	if err := addTupleFieldUpdateJournalSize(&size, 4, maxRecordBytes); err != nil {
		return 0, err
	}
	return size, nil
}

func validateTupleFieldUpdateJournalRecordShape(record TupleFieldUpdateJournalRecord) error {
	if record.Sequence == 0 || record.SchemaVersion == 0 || len(record.Updates) == 0 {
		return ErrTupleFieldUpdateJournalInvalid
	}
	if len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return ErrTupleFieldUpdateJournalLimit
	}
	for index, update := range record.Updates {
		if update.Index < 0 {
			return fmt.Errorf("%w: update %d has negative index", ErrTupleFieldUpdateJournalInvalid, index)
		}
		for previous := 0; previous < index; previous++ {
			if record.Updates[previous].Index == update.Index {
				return ErrTupleFieldUpdateJournalInvalid
			}
		}
		switch update.Kind {
		case TupleFieldSet:
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 {
				return ErrTupleFieldUpdateJournalInvalid
			}
		case TupleFieldAddInt64:
		default:
			return ErrTupleFieldUpdateJournalInvalid
		}
	}
	return nil
}

func addTupleFieldUpdateJournalSize(size *int, addition, max int) error {
	if addition < 0 || addition > max || *size > max-addition {
		return ErrTupleFieldUpdateJournalLimit
	}
	*size += addition
	return nil
}

func addTupleFieldUpdateJournalBytesSize(size *int, length, max int) error {
	if length < 0 {
		return ErrTupleFieldUpdateJournalLimit
	}
	prefix := tupleFieldUpdateJournalUvarintSize(uint64(length))
	if prefix > max || length > max-prefix {
		return ErrTupleFieldUpdateJournalLimit
	}
	return addTupleFieldUpdateJournalSize(size, prefix+length, max)
}

func normalizeTupleFieldUpdateJournalRecordBytes(value int) (int, error) {
	if value == 0 {
		return DefaultTupleFieldUpdateJournalRecordBytes, nil
	}
	if value < len(tupleFieldUpdateJournalMagic)+1+4 || value > MaxTupleFieldUpdateJournalRecordBytes {
		return 0, ErrTupleFieldUpdateJournalLimit
	}
	return value, nil
}

func appendTupleFieldUpdateJournalUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendTupleFieldUpdateJournalVarint(dst []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutVarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendTupleFieldUpdateJournalBytes(dst, value []byte) []byte {
	dst = appendTupleFieldUpdateJournalUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func tupleFieldUpdateJournalUvarintSize(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func tupleFieldUpdateJournalVarintSize(value int64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutVarint(encoded[:], value)
}

func readTupleFieldUpdateJournalUvarint(encoded []byte, offset *int) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	value, width := binary.Uvarint(encoded[*offset:])
	if width == 0 {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	if width < 0 {
		return 0, ErrTupleFieldUpdateJournalWire
	}
	*offset += width
	return value, nil
}

func readTupleFieldUpdateJournalVarint(encoded []byte, offset *int) (int64, error) {
	if *offset >= len(encoded) {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	value, width := binary.Varint(encoded[*offset:])
	if width == 0 {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	if width < 0 {
		return 0, ErrTupleFieldUpdateJournalWire
	}
	*offset += width
	return value, nil
}

func readTupleFieldUpdateJournalBytes(encoded []byte, offset *int) ([]byte, error) {
	length, err := readTupleFieldUpdateJournalUvarint(encoded, offset)
	if err != nil {
		return nil, err
	}
	if length > uint64(len(encoded)-*offset) {
		return nil, ErrTupleFieldUpdateJournalTruncated
	}
	end := *offset + int(length)
	value := append([]byte(nil), encoded[*offset:end]...)
	*offset = end
	return value, nil
}

func writeTupleFieldUpdateJournalBytes(writer io.Writer, encoded []byte) error {
	for len(encoded) > 0 {
		written, err := writer.Write(encoded)
		if written > 0 {
			encoded = encoded[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func readTupleFieldUpdateJournalFrameLength(reader io.Reader) (uint64, error) {
	var value uint64
	var shift uint
	for index := 0; index < binary.MaxVarintLen64; index++ {
		var one [1]byte
		if _, err := io.ReadFull(reader, one[:]); err != nil {
			if err == io.EOF && index == 0 {
				return 0, io.EOF
			}
			return 0, ErrTupleFieldUpdateJournalTruncated
		}
		byteValue := one[0]
		if index == binary.MaxVarintLen64-1 && byteValue > 1 {
			return 0, ErrTupleFieldUpdateJournalWire
		}
		value |= uint64(byteValue&0x7f) << shift
		if byteValue < 0x80 {
			return value, nil
		}
		shift += 7
	}
	return 0, ErrTupleFieldUpdateJournalWire
}
