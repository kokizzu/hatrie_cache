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
	// DefaultTupleFieldUpdateJournalMaxRecordBytes bounds one journal frame.
	DefaultTupleFieldUpdateJournalMaxRecordBytes = 1 << 20
	// MaxTupleFieldUpdateJournalRecordBytes prevents accidental unbounded frame
	// allocation when a journal path is opened with external configuration.
	MaxTupleFieldUpdateJournalRecordBytes = 64 << 20
	// MaxTupleFieldUpdateJournalUpdates bounds the number of field operations in
	// one atomic replay step.
	MaxTupleFieldUpdateJournalUpdates = 1 << 16
	// MaxTupleFieldUpdateJournalBatchBytes bounds temporary encoding memory for
	// one append batch before any file write begins.
	MaxTupleFieldUpdateJournalBatchBytes = 64 << 20
	// MaxTupleFieldUpdateJournalBatchRecords bounds the number of frames in one
	// append batch before temporary frame metadata is allocated.
	MaxTupleFieldUpdateJournalBatchRecords = 1 << 16

	tupleFieldUpdateJournalHeaderBytes      = 9
	tupleFieldUpdateJournalMinimumBodyBytes = 8 + 8 + 4 + 4
)

var (
	// ErrTupleUpdateJournalClosed indicates that the journal has been closed.
	ErrTupleUpdateJournalClosed = errors.New("hatDataStructure: tuple update journal is closed")
	// ErrTupleUpdateJournalCorrupt indicates invalid framing, checksum, or
	// sequence data. A torn final frame is recovered by truncating it instead.
	ErrTupleUpdateJournalCorrupt = errors.New("hatDataStructure: tuple update journal is corrupt")
	// ErrTupleUpdateJournalRecordTooLarge indicates a frame over the configured
	// bound.
	ErrTupleUpdateJournalRecordTooLarge = errors.New("hatDataStructure: tuple update journal record is too large")
	// ErrTupleUpdateJournalSchemaVersion indicates a zero schema version.
	ErrTupleUpdateJournalSchemaVersion = errors.New("hatDataStructure: tuple update journal schema version is required")
	// ErrTupleUpdateJournalUpdateInvalid indicates an invalid operation or an
	// operation that cannot be represented by the bounded wire format.
	ErrTupleUpdateJournalUpdateInvalid = errors.New("hatDataStructure: tuple update journal update is invalid")
)

var tupleFieldUpdateJournalMagic = [4]byte{'H', 'T', 'U', '1'}
var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalOptions configures one durable tuple update log.
// SyncOnAppend defaults to true when OpenTupleFieldUpdateJournal is used. The
// WithOptions constructor permits buffered appends for callers that explicitly
// accept a crash window; Close still syncs once before closing.
type TupleFieldUpdateJournalOptions struct {
	MaxRecordBytes int
	SyncOnAppend   bool
}

// TupleFieldUpdateJournalRecord is one schema-versioned, replayable update
// batch. Updates are copied when decoded and may be retained by the caller.
type TupleFieldUpdateJournalRecord struct {
	Sequence      uint64
	SchemaVersion uint64
	Updates       []TupleFieldUpdate
}

// TupleFieldUpdateJournalAppend describes one record in AppendBatch. Sequence
// numbers are assigned in input order by the journal.
type TupleFieldUpdateJournalAppend struct {
	SchemaVersion uint64
	Updates       []TupleFieldUpdate
}

// TupleFieldUpdateJournal appends bounded HTU1 records and replays them in
// sequence order. The journal is intentionally independent from any storage
// engine; callers decide which VersionedTuple each named log belongs to.
type TupleFieldUpdateJournal struct {
	mu             sync.Mutex
	path           string
	file           *os.File
	maxRecordBytes int
	syncOnAppend   bool
	nextSequence   uint64
	closed         bool
}

// OpenTupleFieldUpdateJournal opens or creates a synchronously durable update
// journal with the documented default record bound.
func OpenTupleFieldUpdateJournal(path string) (*TupleFieldUpdateJournal, error) {
	return OpenTupleFieldUpdateJournalWithOptions(path, TupleFieldUpdateJournalOptions{
		SyncOnAppend: true,
	})
}

// OpenTupleFieldUpdateJournalWithOptions opens or recovers a tuple update
// journal. A partial final frame is treated as a crash tail and truncated;
// corruption in a complete frame is rejected.
func OpenTupleFieldUpdateJournalWithOptions(path string, options TupleFieldUpdateJournalOptions) (*TupleFieldUpdateJournal, error) {
	path = strings.TrimSpace(path)
	if path = filepath.Clean(path); path == "." || path == "" {
		return nil, errors.New("hatDataStructure: tuple update journal path is required")
	}
	maxRecordBytes, err := normalizeTupleFieldUpdateJournalMaxRecordBytes(options.MaxRecordBytes)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	validBytes, lastSequence, scanErr := scanTupleFieldUpdateJournal(file, maxRecordBytes, nil)
	if scanErr != nil {
		_ = file.Close()
		return nil, scanErr
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if validBytes < info.Size() {
		if err := file.Truncate(validBytes); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	journal := &TupleFieldUpdateJournal{
		path:           path,
		file:           file,
		maxRecordBytes: maxRecordBytes,
		syncOnAppend:   options.SyncOnAppend,
		nextSequence:   lastSequence + 1,
	}
	if journal.nextSequence == 0 {
		_ = file.Close()
		return nil, ErrTupleUpdateJournalCorrupt
	}
	return journal, nil
}

// Append durably appends one schema-versioned atomic update batch and returns
// its strictly increasing journal sequence.
func (journal *TupleFieldUpdateJournal) Append(schemaVersion uint64, updates []TupleFieldUpdate) (uint64, error) {
	if journal == nil {
		return 0, ErrTupleUpdateJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	firstSequence, _, err := journal.appendTupleFieldUpdateJournalBatchLocked([]TupleFieldUpdateJournalAppend{{
		SchemaVersion: schemaVersion,
		Updates:       updates,
	}})
	return firstSequence, err
}

// AppendBatch validates and appends several records in input order. With
// SyncOnAppend enabled, one filesystem sync covers the complete batch. If
// validation, writing, or syncing fails, the batch is rolled back to its
// starting offset and no sequence is consumed.
func (journal *TupleFieldUpdateJournal) AppendBatch(records []TupleFieldUpdateJournalAppend) ([]uint64, error) {
	if journal == nil {
		return nil, ErrTupleUpdateJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	firstSequence, lastSequence, err := journal.appendTupleFieldUpdateJournalBatchLocked(records)
	if err != nil {
		return nil, err
	}
	sequences := make([]uint64, len(records))
	for index := range sequences {
		sequences[index] = firstSequence + uint64(index)
	}
	if len(sequences) > 0 && sequences[len(sequences)-1] != lastSequence {
		return nil, ErrTupleUpdateJournalCorrupt
	}
	return sequences, nil
}

func (journal *TupleFieldUpdateJournal) appendTupleFieldUpdateJournalBatchLocked(records []TupleFieldUpdateJournalAppend) (uint64, uint64, error) {
	if journal.closed || journal.file == nil {
		return 0, 0, ErrTupleUpdateJournalClosed
	}
	if len(records) == 0 {
		return 0, 0, ErrTupleUpdateJournalUpdateInvalid
	}
	if len(records) > MaxTupleFieldUpdateJournalBatchRecords {
		return 0, 0, ErrTupleUpdateJournalRecordTooLarge
	}
	if journal.nextSequence == 0 || uint64(len(records)) > ^uint64(0)-journal.nextSequence {
		return 0, 0, ErrTupleUpdateJournalCorrupt
	}
	frames := make([][]byte, len(records))
	var totalFrameBytes int
	for index, appendRecord := range records {
		sequence := journal.nextSequence + uint64(index)
		if sequence == 0 {
			return 0, 0, ErrTupleUpdateJournalCorrupt
		}
		encoded, err := marshalTupleFieldUpdateJournalRecord(TupleFieldUpdateJournalRecord{
			Sequence:      sequence,
			SchemaVersion: appendRecord.SchemaVersion,
			Updates:       appendRecord.Updates,
		}, journal.maxRecordBytes)
		if err != nil {
			return 0, 0, err
		}
		if totalFrameBytes > MaxTupleFieldUpdateJournalBatchBytes-len(encoded) {
			return 0, 0, ErrTupleUpdateJournalRecordTooLarge
		}
		totalFrameBytes += len(encoded)
		frames[index] = encoded
	}
	info, err := journal.file.Stat()
	if err != nil {
		return 0, 0, err
	}
	offset := info.Size()
	for _, frame := range frames {
		n, writeErr := journal.file.Write(frame)
		if writeErr != nil {
			return 0, 0, journal.rollbackTupleFieldUpdateJournalAppend(offset, writeErr)
		}
		if n != len(frame) {
			return 0, 0, journal.rollbackTupleFieldUpdateJournalAppend(offset, io.ErrShortWrite)
		}
	}
	if journal.syncOnAppend {
		if err := journal.file.Sync(); err != nil {
			return 0, 0, journal.rollbackTupleFieldUpdateJournalAppend(offset, err)
		}
	}
	firstSequence := journal.nextSequence
	journal.nextSequence += uint64(len(records))
	return firstSequence, journal.nextSequence - 1, nil
}

// LastSequence returns the highest recovered or appended sequence.
func (journal *TupleFieldUpdateJournal) LastSequence() uint64 {
	if journal == nil {
		return 0
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.nextSequence == 0 {
		return ^uint64(0)
	}
	return journal.nextSequence - 1
}

// Sync flushes buffered appends without closing the journal. It is useful with
// SyncOnAppend disabled when the caller chooses an explicit group-commit point.
func (journal *TupleFieldUpdateJournal) Sync() error {
	if journal == nil {
		return ErrTupleUpdateJournalClosed
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed || journal.file == nil {
		return ErrTupleUpdateJournalClosed
	}
	return journal.file.Sync()
}

// Replay visits records after afterSequence and returns the highest sequence
// present in the log. The callback receives independent decoded bytes.
func (journal *TupleFieldUpdateJournal) Replay(afterSequence uint64, visit func(TupleFieldUpdateJournalRecord) error) (uint64, error) {
	if journal == nil {
		return 0, ErrTupleUpdateJournalClosed
	}
	if visit == nil {
		return 0, errors.New("hatDataStructure: tuple update journal replay callback is nil")
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed || journal.file == nil {
		return 0, ErrTupleUpdateJournalClosed
	}
	reader, err := os.Open(journal.path)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	_, lastSequence, err := scanTupleFieldUpdateJournal(reader, journal.maxRecordBytes, func(record TupleFieldUpdateJournalRecord) error {
		if record.Sequence <= afterSequence {
			return nil
		}
		return visit(record)
	})
	return lastSequence, err
}

// ReplayInto applies all records after afterSequence to a copy of tuple and
// returns the updated tuple plus the highest sequence present. On any error,
// the original tuple is returned unchanged so a caller cannot checkpoint a
// partially applied replay.
func (journal *TupleFieldUpdateJournal) ReplayInto(format TupleFormat, tuple VersionedTuple, afterSequence uint64) (VersionedTuple, uint64, error) {
	if err := tuple.Validate(format); err != nil {
		return tuple, afterSequence, err
	}
	updated := tuple
	lastSequence, err := journal.Replay(afterSequence, func(record TupleFieldUpdateJournalRecord) error {
		if record.SchemaVersion != format.Version() {
			return fmt.Errorf("%w: expected %d, got %d", ErrTupleUpdateJournalSchemaVersion, format.Version(), record.SchemaVersion)
		}
		candidate, applyErr := updated.ApplyUpdates(format, record.Updates)
		if applyErr != nil {
			return applyErr
		}
		updated = candidate
		return nil
	})
	if err != nil {
		return tuple, afterSequence, err
	}
	return updated, lastSequence, nil
}

// Close syncs pending buffered bytes and closes the journal. It is safe to
// call more than once.
func (journal *TupleFieldUpdateJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return nil
	}
	journal.closed = true
	if journal.file == nil {
		return nil
	}
	syncErr := journal.file.Sync()
	closeErr := journal.file.Close()
	journal.file = nil
	return errors.Join(syncErr, closeErr)
}

// MarshalTupleFieldUpdateJournalRecord encodes one bounded HTU1 frame for
// transport or storage outside TupleFieldUpdateJournal.
func MarshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	return marshalTupleFieldUpdateJournalRecord(record, DefaultTupleFieldUpdateJournalMaxRecordBytes)
}

// UnmarshalTupleFieldUpdateJournalRecord validates and decodes one complete
// HTU1 frame.
func UnmarshalTupleFieldUpdateJournalRecord(encoded []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(encoded) > DefaultTupleFieldUpdateJournalMaxRecordBytes {
		return TupleFieldUpdateJournalRecord{}, ErrTupleUpdateJournalRecordTooLarge
	}
	record, consumed, err := readTupleFieldUpdateJournalRecord(bytes.NewReader(encoded), DefaultTupleFieldUpdateJournalMaxRecordBytes)
	if err != nil {
		if errors.Is(err, errTupleFieldUpdateJournalTornTail) {
			return TupleFieldUpdateJournalRecord{}, ErrTupleUpdateJournalCorrupt
		}
		return TupleFieldUpdateJournalRecord{}, err
	}
	if consumed != len(encoded) {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: trailing bytes", ErrTupleUpdateJournalCorrupt)
	}
	return record, nil
}

func normalizeTupleFieldUpdateJournalMaxRecordBytes(value int) (int, error) {
	if value == 0 {
		return DefaultTupleFieldUpdateJournalMaxRecordBytes, nil
	}
	if value < tupleFieldUpdateJournalHeaderBytes+tupleFieldUpdateJournalMinimumBodyBytes || value > MaxTupleFieldUpdateJournalRecordBytes {
		return 0, fmt.Errorf("%w: max bytes %d is outside [%d, %d]", ErrTupleUpdateJournalRecordTooLarge, value, tupleFieldUpdateJournalHeaderBytes+tupleFieldUpdateJournalMinimumBodyBytes, MaxTupleFieldUpdateJournalRecordBytes)
	}
	return value, nil
}

func marshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord, maxRecordBytes int) ([]byte, error) {
	if record.Sequence == 0 {
		return nil, fmt.Errorf("%w: sequence is zero", ErrTupleUpdateJournalCorrupt)
	}
	if record.SchemaVersion == 0 {
		return nil, ErrTupleUpdateJournalSchemaVersion
	}
	if len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return nil, ErrTupleUpdateJournalUpdateInvalid
	}
	payloadCapacity := 8 + 8 + 4
	for _, update := range record.Updates {
		if err := validateTupleFieldUpdateJournalUpdate(update); err != nil {
			return nil, err
		}
		additional := 1 + binary.MaxVarintLen64
		switch update.Kind {
		case TupleFieldSet:
			additional += binary.MaxVarintLen64
			if len(update.Value) > maxRecordBytes || additional > maxRecordBytes-len(update.Value) {
				return nil, ErrTupleUpdateJournalRecordTooLarge
			}
			additional += len(update.Value)
		case TupleFieldSplice:
			additional += 2 * binary.MaxVarintLen64
			if len(update.Insert) > maxRecordBytes || additional > maxRecordBytes-len(update.Insert) {
				return nil, ErrTupleUpdateJournalRecordTooLarge
			}
			additional += len(update.Insert)
		case TupleFieldAddInt64:
			additional += 8
		}
		if payloadCapacity > maxRecordBytes-additional {
			return nil, ErrTupleUpdateJournalRecordTooLarge
		}
		payloadCapacity += additional
	}
	payload := make([]byte, 0, payloadCapacity)
	var fixed [8]byte
	binary.BigEndian.PutUint64(fixed[:], record.Sequence)
	payload = append(payload, fixed[:]...)
	binary.BigEndian.PutUint64(fixed[:], record.SchemaVersion)
	payload = append(payload, fixed[:]...)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], uint32(len(record.Updates)))
	payload = append(payload, count[:]...)
	for _, update := range record.Updates {
		payload = appendTupleFieldUpdateJournalUvarint(payload, uint64(update.Index))
		payload = append(payload, byte(update.Kind))
		switch update.Kind {
		case TupleFieldSet:
			payload = appendTupleFieldUpdateJournalBytes(payload, update.Value)
		case TupleFieldSplice:
			payload = appendTupleFieldUpdateJournalUvarint(payload, uint64(update.Start))
			payload = appendTupleFieldUpdateJournalUvarint(payload, uint64(update.Remove))
			payload = appendTupleFieldUpdateJournalBytes(payload, update.Insert)
		case TupleFieldAddInt64:
			binary.BigEndian.PutUint64(fixed[:], uint64(update.Delta))
			payload = append(payload, fixed[:]...)
		}
	}
	bodyLength := len(payload) + 4
	frameLength := tupleFieldUpdateJournalHeaderBytes + bodyLength
	if frameLength > maxRecordBytes || bodyLength > int(^uint32(0)) {
		return nil, ErrTupleUpdateJournalRecordTooLarge
	}
	frame := make([]byte, 0, frameLength)
	frame = append(frame, tupleFieldUpdateJournalMagic[:]...)
	frame = append(frame, 1)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(bodyLength))
	frame = append(frame, length[:]...)
	frame = append(frame, payload...)
	checksum := crc32.New(tupleFieldUpdateJournalCRCTable)
	_, _ = checksum.Write(frame[4:])
	binary.BigEndian.PutUint32(length[:], checksum.Sum32())
	frame = append(frame, length[:]...)
	return frame, nil
}

func validateTupleFieldUpdateJournalUpdate(update TupleFieldUpdate) error {
	if update.Index < 0 {
		return fmt.Errorf("%w: field index %d", ErrTupleUpdateJournalUpdateInvalid, update.Index)
	}
	switch update.Kind {
	case TupleFieldSet:
		return nil
	case TupleFieldSplice:
		if update.Start < 0 || update.Remove < 0 {
			return fmt.Errorf("%w: splice offsets must be non-negative", ErrTupleUpdateJournalUpdateInvalid)
		}
		return nil
	case TupleFieldAddInt64:
		return nil
	default:
		return fmt.Errorf("%w: kind %d", ErrTupleUpdateJournalUpdateInvalid, update.Kind)
	}
}

func appendTupleFieldUpdateJournalUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func appendTupleFieldUpdateJournalBytes(dst, value []byte) []byte {
	dst = appendTupleFieldUpdateJournalUvarint(dst, uint64(len(value)))
	return append(dst, value...)
}

func scanTupleFieldUpdateJournal(reader io.Reader, maxRecordBytes int, visit func(TupleFieldUpdateJournalRecord) error) (int64, uint64, error) {
	var offset int64
	var lastSequence uint64
	for {
		record, consumed, err := readTupleFieldUpdateJournalRecord(reader, maxRecordBytes)
		if errors.Is(err, io.EOF) {
			return offset, lastSequence, nil
		}
		if errors.Is(err, errTupleFieldUpdateJournalTornTail) {
			return offset, lastSequence, nil
		}
		if err != nil {
			return offset, lastSequence, err
		}
		if record.Sequence == 0 || (lastSequence != 0 && record.Sequence != lastSequence+1) {
			return offset, lastSequence, fmt.Errorf("%w: sequence %d follows %d", ErrTupleUpdateJournalCorrupt, record.Sequence, lastSequence)
		}
		if visit != nil {
			if err := visit(record); err != nil {
				return offset, lastSequence, err
			}
		}
		offset += int64(consumed)
		lastSequence = record.Sequence
	}
}

var errTupleFieldUpdateJournalTornTail = errors.New("hatDataStructure: tuple update journal torn tail")

func readTupleFieldUpdateJournalRecord(reader io.Reader, maxRecordBytes int) (TupleFieldUpdateJournalRecord, int, error) {
	var header [tupleFieldUpdateJournalHeaderBytes]byte
	n, err := io.ReadFull(reader, header[:])
	if err != nil {
		if err == io.EOF && n == 0 {
			return TupleFieldUpdateJournalRecord{}, 0, io.EOF
		}
		return TupleFieldUpdateJournalRecord{}, n, errTupleFieldUpdateJournalTornTail
	}
	if !bytes.Equal(header[:4], tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, 0, fmt.Errorf("%w: magic", ErrTupleUpdateJournalCorrupt)
	}
	if header[4] != 1 {
		return TupleFieldUpdateJournalRecord{}, 0, fmt.Errorf("%w: version %d", ErrTupleUpdateJournalCorrupt, header[4])
	}
	bodyLength := binary.BigEndian.Uint32(header[5:])
	if bodyLength < tupleFieldUpdateJournalMinimumBodyBytes || int(bodyLength) > maxRecordBytes-tupleFieldUpdateJournalHeaderBytes {
		return TupleFieldUpdateJournalRecord{}, 0, ErrTupleUpdateJournalRecordTooLarge
	}
	body := make([]byte, int(bodyLength))
	n, err = io.ReadFull(reader, body)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalHeaderBytes + n, errTupleFieldUpdateJournalTornTail
	}
	checksum := crc32.New(tupleFieldUpdateJournalCRCTable)
	_, _ = checksum.Write(header[4:])
	_, _ = checksum.Write(body[:len(body)-4])
	if got, want := binary.BigEndian.Uint32(body[len(body)-4:]), checksum.Sum32(); got != want {
		return TupleFieldUpdateJournalRecord{}, 0, fmt.Errorf("%w: checksum %08x != %08x", ErrTupleUpdateJournalCorrupt, got, want)
	}
	payload := body[:len(body)-4]
	record, err := decodeTupleFieldUpdateJournalPayload(payload)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, 0, err
	}
	return record, tupleFieldUpdateJournalHeaderBytes + len(body), nil
}

func decodeTupleFieldUpdateJournalPayload(payload []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(payload) < 8+8+4 {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: payload is too short", ErrTupleUpdateJournalCorrupt)
	}
	offset := 0
	record := TupleFieldUpdateJournalRecord{
		Sequence:      binary.BigEndian.Uint64(payload[offset : offset+8]),
		SchemaVersion: binary.BigEndian.Uint64(payload[offset+8 : offset+16]),
	}
	offset += 16
	count := binary.BigEndian.Uint32(payload[offset : offset+4])
	offset += 4
	if record.Sequence == 0 {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: sequence is zero", ErrTupleUpdateJournalCorrupt)
	}
	if record.SchemaVersion == 0 {
		return TupleFieldUpdateJournalRecord{}, ErrTupleUpdateJournalSchemaVersion
	}
	if count == 0 || count > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, ErrTupleUpdateJournalUpdateInvalid
	}
	record.Updates = make([]TupleFieldUpdate, 0, int(count))
	for index := uint32(0); index < count; index++ {
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(payload, &offset)
		if err != nil || fieldIndex > uint64(^uint(0)>>1) {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: field index", ErrTupleUpdateJournalCorrupt)
		}
		if offset >= len(payload) {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: missing update kind", ErrTupleUpdateJournalCorrupt)
		}
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: TupleFieldUpdateKind(payload[offset])}
		offset++
		switch update.Kind {
		case TupleFieldSet:
			update.Value, err = readTupleFieldUpdateJournalBytes(payload, &offset)
		case TupleFieldSplice:
			var start, remove uint64
			start, err = readTupleFieldUpdateJournalUvarint(payload, &offset)
			if err == nil {
				remove, err = readTupleFieldUpdateJournalUvarint(payload, &offset)
			}
			if err == nil && (start > uint64(^uint(0)>>1) || remove > uint64(^uint(0)>>1)) {
				err = ErrTupleUpdateJournalUpdateInvalid
			}
			update.Start = int(start)
			update.Remove = int(remove)
			if err == nil {
				update.Insert, err = readTupleFieldUpdateJournalBytes(payload, &offset)
			}
		case TupleFieldAddInt64:
			if len(payload)-offset < 8 {
				err = io.ErrUnexpectedEOF
			} else {
				update.Delta = int64(binary.BigEndian.Uint64(payload[offset : offset+8]))
				offset += 8
			}
		default:
			err = ErrTupleUpdateJournalUpdateInvalid
		}
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: update %d: %v", ErrTupleUpdateJournalCorrupt, index, err)
		}
		if err := validateTupleFieldUpdateJournalUpdate(update); err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		record.Updates = append(record.Updates, update)
	}
	if offset != len(payload) {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: payload has %d trailing bytes", ErrTupleUpdateJournalCorrupt, len(payload)-offset)
	}
	return record, nil
}

func readTupleFieldUpdateJournalUvarint(data []byte, offset *int) (uint64, error) {
	if *offset >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	value, length := binary.Uvarint(data[*offset:])
	if length <= 0 {
		return 0, ErrTupleUpdateJournalUpdateInvalid
	}
	*offset += length
	return value, nil
}

func readTupleFieldUpdateJournalBytes(data []byte, offset *int) ([]byte, error) {
	length, err := readTupleFieldUpdateJournalUvarint(data, offset)
	if err != nil || length > uint64(len(data)-*offset) {
		if err != nil {
			return nil, err
		}
		return nil, io.ErrUnexpectedEOF
	}
	end := *offset + int(length)
	value := append([]byte(nil), data[*offset:end]...)
	*offset = end
	return value, nil
}

func (journal *TupleFieldUpdateJournal) rollbackTupleFieldUpdateJournalAppend(offset int64, cause error) error {
	if err := journal.file.Truncate(offset); err != nil {
		return errors.Join(cause, err)
	}
	if _, err := journal.file.Seek(offset, io.SeekStart); err != nil {
		return errors.Join(cause, err)
	}
	if err := journal.file.Sync(); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
