package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	// ErrTupleFieldUpdateJournalWire reports malformed or unsupported HTFJ1
	// framing, version, sequence, key, or operation data.
	ErrTupleFieldUpdateJournalWire = errors.New("hatDataStructure: invalid tuple update journal wire")
	// ErrTupleFieldUpdateJournalChecksum reports a failed CRC32C check.
	ErrTupleFieldUpdateJournalChecksum = errors.New("hatDataStructure: tuple update journal checksum mismatch")
	// ErrTupleFieldUpdateJournalLimit reports a bounded record limit failure.
	ErrTupleFieldUpdateJournalLimit = errors.New("hatDataStructure: tuple update journal limit exceeded")
)

const (
	// TupleFieldUpdateJournalVersion is the current durable record version.
	TupleFieldUpdateJournalVersion uint8 = 1
	// MaxTupleFieldUpdateJournalUpdates bounds operations in one record.
	MaxTupleFieldUpdateJournalUpdates = 64
	// MaxTupleFieldUpdateJournalBytes bounds one encoded record, including CRC.
	MaxTupleFieldUpdateJournalBytes = 16 << 20
	// MaxTupleFieldUpdateJournalKeyBytes bounds the tuple key in one record.
	MaxTupleFieldUpdateJournalKeyBytes   = 1 << 20
	maxTupleFieldUpdateJournalFieldBytes = 1 << 20
)

var tupleFieldUpdateJournalMagic = [4]byte{'H', 'T', 'F', 'J'}
var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalRecord is a bounded, replayable tuple mutation. The
// key and sequence identify the durable mutation; Updates are applied as one
// atomic batch by ApplyTo.
type TupleFieldUpdateJournalRecord struct {
	Sequence uint64
	Key      string
	Updates  []TupleFieldUpdate
}

// MarshalTupleFieldUpdateJournalRecord encodes one deterministic HTFJ1 record.
// The result is self-checksummed with CRC32C and owns its returned bytes.
func MarshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	encodedSize, err := validateTupleFieldUpdateJournalRecord(record)
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, encodedSize)
	data = append(data, tupleFieldUpdateJournalMagic[:]...)
	data = append(data, TupleFieldUpdateJournalVersion)
	data = appendTupleFieldUpdateJournalUvarint(data, record.Sequence)
	data = appendTupleFieldUpdateJournalBytes(data, []byte(record.Key))
	data = appendTupleFieldUpdateJournalUvarint(data, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		data = appendTupleFieldUpdateJournalUvarint(data, uint64(update.Index))
		data = append(data, byte(update.Kind))
		switch update.Kind {
		case TupleFieldSet:
			data = appendTupleFieldUpdateJournalBytes(data, update.Value)
		case TupleFieldSplice:
			data = appendTupleFieldUpdateJournalUvarint(data, uint64(update.Start))
			data = appendTupleFieldUpdateJournalUvarint(data, uint64(update.Remove))
			data = appendTupleFieldUpdateJournalBytes(data, update.Insert)
		case TupleFieldAddInt64:
			data = appendTupleFieldUpdateJournalUvarint(data, tupleFieldUpdateJournalZigZag(update.Delta))
		}
	}
	checksum := crc32.Checksum(data, tupleFieldUpdateJournalCRCTable)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	data = append(data, checksumBytes[:]...)
	return data, nil
}

// UnmarshalTupleFieldUpdateJournalRecord validates and decodes one HTFJ1
// record. All variable-length fields are copied so the returned record does
// not retain the wire buffer.
func UnmarshalTupleFieldUpdateJournalRecord(data []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(data) > MaxTupleFieldUpdateJournalBytes {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: record length %d", ErrTupleFieldUpdateJournalLimit, len(data))
	}
	if len(data) < len(tupleFieldUpdateJournalMagic)+1+4 {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: record length %d", ErrTupleFieldUpdateJournalWire, len(data))
	}
	if !bytes.Equal(data[:len(tupleFieldUpdateJournalMagic)], tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: magic", ErrTupleFieldUpdateJournalWire)
	}
	versionOffset := len(tupleFieldUpdateJournalMagic)
	if data[versionOffset] != TupleFieldUpdateJournalVersion {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: version %d", ErrTupleFieldUpdateJournalWire, data[versionOffset])
	}
	checksumOffset := len(data) - 4
	wantChecksum := binary.LittleEndian.Uint32(data[checksumOffset:])
	if gotChecksum := crc32.Checksum(data[:checksumOffset], tupleFieldUpdateJournalCRCTable); gotChecksum != wantChecksum {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: got %08x, want %08x", ErrTupleFieldUpdateJournalChecksum, gotChecksum, wantChecksum)
	}
	reader := tupleFieldUpdateJournalReader{data: data, limit: checksumOffset, offset: versionOffset + 1}
	sequence, err := reader.uvarint()
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	keyBytes, err := reader.bytes(MaxTupleFieldUpdateJournalKeyBytes)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	updateCount, err := reader.uvarint()
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	if updateCount == 0 || updateCount > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: update count %d", ErrTupleFieldUpdateJournalLimit, updateCount)
	}
	updates := make([]TupleFieldUpdate, int(updateCount))
	for index := range updates {
		fieldIndex, err := reader.uvarint()
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		if fieldIndex > uint64(^uint(0)>>1) {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: field index %d", ErrTupleFieldUpdateJournalWire, fieldIndex)
		}
		kind, err := reader.byte()
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: TupleFieldUpdateKind(kind)}
		switch update.Kind {
		case TupleFieldSet:
			update.Value, err = reader.bytes(maxTupleFieldUpdateJournalFieldBytes)
		case TupleFieldSplice:
			start, startErr := reader.uvarint()
			if startErr != nil {
				err = startErr
				break
			}
			remove, removeErr := reader.uvarint()
			if removeErr != nil {
				err = removeErr
				break
			}
			if start > uint64(^uint(0)>>1) || remove > uint64(^uint(0)>>1) {
				err = fmt.Errorf("%w: splice range", ErrTupleFieldUpdateJournalWire)
				break
			}
			update.Start = int(start)
			update.Remove = int(remove)
			update.Insert, err = reader.bytes(maxTupleFieldUpdateJournalFieldBytes)
		case TupleFieldAddInt64:
			delta, deltaErr := reader.uvarint()
			if deltaErr != nil {
				err = deltaErr
				break
			}
			update.Delta = tupleFieldUpdateJournalUnZigZag(delta)
		default:
			err = fmt.Errorf("%w: operation kind %d", ErrTupleFieldUpdateJournalWire, kind)
		}
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		updates[index] = update
	}
	if reader.offset != reader.limit {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: trailing payload", ErrTupleFieldUpdateJournalWire)
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence: sequence,
		Key:      string(keyBytes),
		Updates:  updates,
	}
	if _, err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	return record, nil
}

// ApplyTo replays the record through the existing atomic tuple update path.
func (record TupleFieldUpdateJournalRecord) ApplyTo(cache TupleFieldOffsetCache) (TupleFieldOffsetCache, error) {
	if _, err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldOffsetCache{}, err
	}
	return cache.ApplyUpdates(record.Updates)
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) (int, error) {
	if record.Sequence == 0 || len(record.Key) == 0 {
		return 0, fmt.Errorf("%w: sequence and key are required", ErrTupleFieldUpdateJournalWire)
	}
	if len(record.Key) > MaxTupleFieldUpdateJournalKeyBytes {
		return 0, fmt.Errorf("%w: key length %d", ErrTupleFieldUpdateJournalLimit, len(record.Key))
	}
	if len(record.Updates) == 0 {
		return 0, fmt.Errorf("%w: updates are required", ErrTupleFieldUpdateJournalWire)
	}
	if len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return 0, fmt.Errorf("%w: update count %d", ErrTupleFieldUpdateJournalLimit, len(record.Updates))
	}
	encodedSize := len(tupleFieldUpdateJournalMagic) + 1 + tupleFieldUpdateJournalUvarintSize(record.Sequence)
	encodedSize += tupleFieldUpdateJournalBytesSize(len(record.Key))
	encodedSize += tupleFieldUpdateJournalUvarintSize(uint64(len(record.Updates)))
	for updateIndex, update := range record.Updates {
		if update.Index < 0 {
			return 0, fmt.Errorf("%w: update %d has a negative field index", ErrTupleFieldUpdateJournalWire, updateIndex)
		}
		for priorIndex := 0; priorIndex < updateIndex; priorIndex++ {
			if record.Updates[priorIndex].Index == update.Index {
				return 0, fmt.Errorf("%w: duplicate field %d", ErrTupleFieldUpdateJournalWire, update.Index)
			}
		}
		encodedSize += tupleFieldUpdateJournalUvarintSize(uint64(update.Index)) + 1
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > maxTupleFieldUpdateJournalFieldBytes {
				return 0, fmt.Errorf("%w: SET field length %d", ErrTupleFieldUpdateJournalLimit, len(update.Value))
			}
			encodedSize += tupleFieldUpdateJournalBytesSize(len(update.Value))
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 {
				return 0, fmt.Errorf("%w: negative splice range", ErrTupleFieldUpdateJournalWire)
			}
			if len(update.Insert) > maxTupleFieldUpdateJournalFieldBytes {
				return 0, fmt.Errorf("%w: splice insert length %d", ErrTupleFieldUpdateJournalLimit, len(update.Insert))
			}
			encodedSize += tupleFieldUpdateJournalUvarintSize(uint64(update.Start))
			encodedSize += tupleFieldUpdateJournalUvarintSize(uint64(update.Remove))
			encodedSize += tupleFieldUpdateJournalBytesSize(len(update.Insert))
		case TupleFieldAddInt64:
			encodedSize += tupleFieldUpdateJournalUvarintSize(tupleFieldUpdateJournalZigZag(update.Delta))
		default:
			return 0, fmt.Errorf("%w: operation kind %d", ErrTupleFieldUpdateJournalWire, update.Kind)
		}
		if encodedSize > MaxTupleFieldUpdateJournalBytes-4 {
			return 0, fmt.Errorf("%w: encoded size %d", ErrTupleFieldUpdateJournalLimit, encodedSize+4)
		}
	}
	return encodedSize + 4, nil
}

type tupleFieldUpdateJournalReader struct {
	data   []byte
	limit  int
	offset int
}

func (reader *tupleFieldUpdateJournalReader) byte() (byte, error) {
	if reader.offset >= reader.limit {
		return 0, fmt.Errorf("%w: truncated byte", ErrTupleFieldUpdateJournalWire)
	}
	value := reader.data[reader.offset]
	reader.offset++
	return value, nil
}

func (reader *tupleFieldUpdateJournalReader) uvarint() (uint64, error) {
	if reader.offset >= reader.limit {
		return 0, fmt.Errorf("%w: truncated varint", ErrTupleFieldUpdateJournalWire)
	}
	value, size := binary.Uvarint(reader.data[reader.offset:reader.limit])
	if size <= 0 {
		return 0, fmt.Errorf("%w: invalid varint", ErrTupleFieldUpdateJournalWire)
	}
	reader.offset += size
	return value, nil
}

func (reader *tupleFieldUpdateJournalReader) bytes(maximum int) ([]byte, error) {
	length, err := reader.uvarint()
	if err != nil {
		return nil, err
	}
	if length > uint64(maximum) {
		return nil, fmt.Errorf("%w: byte field length %d", ErrTupleFieldUpdateJournalLimit, length)
	}
	if length > uint64(reader.limit-reader.offset) {
		return nil, fmt.Errorf("%w: truncated byte field", ErrTupleFieldUpdateJournalWire)
	}
	value := append([]byte(nil), reader.data[reader.offset:reader.offset+int(length)]...)
	reader.offset += int(length)
	return value, nil
}

func appendTupleFieldUpdateJournalUvarint(data []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(data, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendTupleFieldUpdateJournalBytes(data, value []byte) []byte {
	data = appendTupleFieldUpdateJournalUvarint(data, uint64(len(value)))
	return append(data, value...)
}

func tupleFieldUpdateJournalUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func tupleFieldUpdateJournalBytesSize(length int) int {
	return tupleFieldUpdateJournalUvarintSize(uint64(length)) + length
}

func tupleFieldUpdateJournalZigZag(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func tupleFieldUpdateJournalUnZigZag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}
