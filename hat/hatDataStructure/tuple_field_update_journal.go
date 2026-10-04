package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var (
	// ErrTupleFieldUpdateJournalInvalid indicates a malformed or unsupported
	// tuple field-update journal record.
	ErrTupleFieldUpdateJournalInvalid = errors.New("hatDataStructure: invalid tuple field-update journal record")
	// ErrTupleFieldUpdateJournalCorrupt indicates a checksum failure.
	ErrTupleFieldUpdateJournalCorrupt = errors.New("hatDataStructure: corrupt tuple field-update journal record")
)

const (
	// TupleFieldUpdateJournalVersion is the current binary record version.
	TupleFieldUpdateJournalVersion = 1
	// MaxTupleFieldUpdateJournalKeyBytes bounds the key copied by the decoder.
	MaxTupleFieldUpdateJournalKeyBytes = 1 << 20
	// MaxTupleFieldUpdateJournalUpdates bounds the number of operations in one
	// record.
	MaxTupleFieldUpdateJournalUpdates = 1024
	// MaxTupleFieldUpdateJournalBytes bounds the complete encoded record,
	// including its header and checksum.
	MaxTupleFieldUpdateJournalBytes = 16 << 20
)

const (
	tupleFieldUpdateJournalHeaderSize   = 5
	tupleFieldUpdateJournalChecksumSize = 4
)

var tupleFieldUpdateJournalMagic = [4]byte{'H', 'T', 'U', '1'}
var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalRecord is one durable, replayable tuple operation
// batch. The sequence and key identify the journal entry; Updates are applied
// atomically by TupleFieldOffsetCache.ApplyUpdates.
type TupleFieldUpdateJournalRecord struct {
	Sequence uint64
	Key      string
	Updates  []TupleFieldUpdate
}

// MarshalTupleFieldUpdateJournal encodes a bounded, versioned tuple update
// record with a CRC32C checksum. The returned bytes own copies of all input
// values and are deterministic for equivalent records.
func MarshalTupleFieldUpdateJournal(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return nil, err
	}
	size, err := tupleFieldUpdateJournalEncodedSize(record)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, size)
	encoded = append(encoded, tupleFieldUpdateJournalMagic[:]...)
	encoded = append(encoded, byte(TupleFieldUpdateJournalVersion))
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, record.Sequence)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(record.Key)))
	encoded = append(encoded, record.Key...)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Index))
		encoded = append(encoded, byte(update.Kind))
		switch update.Kind {
		case TupleFieldSet:
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(update.Value)))
			encoded = append(encoded, update.Value...)
		case TupleFieldSplice:
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Start))
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Remove))
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(update.Insert)))
			encoded = append(encoded, update.Insert...)
		case TupleFieldAddInt64:
			var delta [8]byte
			binary.BigEndian.PutUint64(delta[:], uint64(update.Delta))
			encoded = append(encoded, delta[:]...)
		}
	}
	checksum := crc32.Checksum(encoded, tupleFieldUpdateJournalCRCTable)
	var checksumBytes [tupleFieldUpdateJournalChecksumSize]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

// UnmarshalTupleFieldUpdateJournal verifies and decodes one tuple update
// record. The returned key and byte fields are independent of encoded.
func UnmarshalTupleFieldUpdateJournal(encoded []byte) (TupleFieldUpdateJournalRecord, error) {
	var record TupleFieldUpdateJournalRecord
	if len(encoded) < tupleFieldUpdateJournalHeaderSize+tupleFieldUpdateJournalChecksumSize || len(encoded) > MaxTupleFieldUpdateJournalBytes {
		return record, fmt.Errorf("%w: encoded size %d", ErrTupleFieldUpdateJournalInvalid, len(encoded))
	}
	if !bytes.Equal(encoded[:len(tupleFieldUpdateJournalMagic)], tupleFieldUpdateJournalMagic[:]) {
		return record, fmt.Errorf("%w: magic", ErrTupleFieldUpdateJournalInvalid)
	}
	if encoded[len(tupleFieldUpdateJournalMagic)] != TupleFieldUpdateJournalVersion {
		return record, fmt.Errorf("%w: version %d", ErrTupleFieldUpdateJournalInvalid, encoded[len(tupleFieldUpdateJournalMagic)])
	}
	bodyEnd := len(encoded) - tupleFieldUpdateJournalChecksumSize
	position := tupleFieldUpdateJournalHeaderSize
	sequence, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
	if err != nil || sequence == 0 {
		return record, tupleFieldUpdateJournalInvalidRead(err, "sequence")
	}
	keyLength, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
	if err != nil || keyLength == 0 || keyLength > MaxTupleFieldUpdateJournalKeyBytes || keyLength > uint64(bodyEnd-position) {
		return record, tupleFieldUpdateJournalInvalidRead(err, "key")
	}
	keyStart := position
	position += int(keyLength)
	updateCount, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
	if err != nil || updateCount == 0 || updateCount > MaxTupleFieldUpdateJournalUpdates {
		return record, tupleFieldUpdateJournalInvalidRead(err, "update count")
	}
	updates := make([]TupleFieldUpdate, int(updateCount))
	for index := range updates {
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
		if err != nil || fieldIndex > uint64(^uint(0)>>1) {
			return record, tupleFieldUpdateJournalInvalidRead(err, "field index")
		}
		if position >= bodyEnd {
			return record, fmt.Errorf("%w: update kind", ErrTupleFieldUpdateJournalInvalid)
		}
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: TupleFieldUpdateKind(encoded[position])}
		position++
		switch update.Kind {
		case TupleFieldSet:
			value, next, err := readTupleFieldUpdateJournalBytes(encoded, position, bodyEnd)
			if err != nil {
				return record, err
			}
			update.Value = value
			position = next
		case TupleFieldSplice:
			start, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
			if err != nil || start > uint64(^uint(0)>>1) {
				return record, tupleFieldUpdateJournalInvalidRead(err, "splice start")
			}
			remove, err := readTupleFieldUpdateJournalUvarint(encoded, &position, bodyEnd)
			if err != nil || remove > uint64(^uint(0)>>1) {
				return record, tupleFieldUpdateJournalInvalidRead(err, "splice remove")
			}
			insert, next, err := readTupleFieldUpdateJournalBytes(encoded, position, bodyEnd)
			if err != nil {
				return record, err
			}
			update.Start = int(start)
			update.Remove = int(remove)
			update.Insert = insert
			position = next
		case TupleFieldAddInt64:
			if bodyEnd-position < 8 {
				return record, fmt.Errorf("%w: int64 delta", ErrTupleFieldUpdateJournalInvalid)
			}
			update.Delta = int64(binary.BigEndian.Uint64(encoded[position : position+8]))
			position += 8
		default:
			return record, fmt.Errorf("%w: update kind %d", ErrTupleFieldUpdateJournalInvalid, update.Kind)
		}
		updates[index] = update
	}
	if position != bodyEnd {
		return record, fmt.Errorf("%w: trailing payload", ErrTupleFieldUpdateJournalInvalid)
	}
	wantChecksum := crc32.Checksum(encoded[:bodyEnd], tupleFieldUpdateJournalCRCTable)
	gotChecksum := binary.LittleEndian.Uint32(encoded[bodyEnd:])
	if gotChecksum != wantChecksum {
		return record, fmt.Errorf("%w: got %08x, want %08x", ErrTupleFieldUpdateJournalCorrupt, gotChecksum, wantChecksum)
	}
	record.Sequence = sequence
	record.Key = string(append([]byte(nil), encoded[keyStart:keyStart+int(keyLength)]...))
	record.Updates = updates
	return record, nil
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) error {
	if record.Sequence == 0 {
		return fmt.Errorf("%w: sequence must be positive", ErrTupleFieldUpdateJournalInvalid)
	}
	if len(record.Key) == 0 || len(record.Key) > MaxTupleFieldUpdateJournalKeyBytes {
		return fmt.Errorf("%w: key length %d", ErrTupleFieldUpdateJournalInvalid, len(record.Key))
	}
	if len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return fmt.Errorf("%w: update count %d", ErrTupleFieldUpdateJournalInvalid, len(record.Updates))
	}
	for index, update := range record.Updates {
		if update.Index < 0 {
			return fmt.Errorf("%w: update %d has negative field index", ErrTupleFieldUpdateJournalInvalid, index)
		}
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > MaxTupleFieldUpdateJournalBytes || len(update.Insert) != 0 || update.Start != 0 || update.Remove != 0 || update.Delta != 0 {
				return fmt.Errorf("%w: invalid set update %d", ErrTupleFieldUpdateJournalInvalid, index)
			}
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 || len(update.Insert) > MaxTupleFieldUpdateJournalBytes || len(update.Value) != 0 || update.Delta != 0 {
				return fmt.Errorf("%w: invalid splice update %d", ErrTupleFieldUpdateJournalInvalid, index)
			}
		case TupleFieldAddInt64:
			if len(update.Value) != 0 || len(update.Insert) != 0 || update.Start != 0 || update.Remove != 0 {
				return fmt.Errorf("%w: invalid int64 update %d", ErrTupleFieldUpdateJournalInvalid, index)
			}
		default:
			return fmt.Errorf("%w: update %d has kind %d", ErrTupleFieldUpdateJournalInvalid, index, update.Kind)
		}
	}
	return nil
}

func tupleFieldUpdateJournalEncodedSize(record TupleFieldUpdateJournalRecord) (int, error) {
	size := uint64(tupleFieldUpdateJournalHeaderSize + tupleFieldUpdateJournalChecksumSize)
	var err error
	add := func(value uint64) {
		if err != nil || value > uint64(MaxTupleFieldUpdateJournalBytes)-size {
			if err == nil {
				err = fmt.Errorf("%w: encoded size exceeds %d bytes", ErrTupleFieldUpdateJournalInvalid, MaxTupleFieldUpdateJournalBytes)
			}
			return
		}
		size += value
	}
	add(tupleFieldUpdateJournalUvarintSize(record.Sequence))
	add(tupleFieldUpdateJournalUvarintSize(uint64(len(record.Key))))
	add(uint64(len(record.Key)))
	add(tupleFieldUpdateJournalUvarintSize(uint64(len(record.Updates))))
	for _, update := range record.Updates {
		add(tupleFieldUpdateJournalUvarintSize(uint64(update.Index)))
		add(1)
		switch update.Kind {
		case TupleFieldSet:
			add(tupleFieldUpdateJournalUvarintSize(uint64(len(update.Value))))
			add(uint64(len(update.Value)))
		case TupleFieldSplice:
			add(tupleFieldUpdateJournalUvarintSize(uint64(update.Start)))
			add(tupleFieldUpdateJournalUvarintSize(uint64(update.Remove)))
			add(tupleFieldUpdateJournalUvarintSize(uint64(len(update.Insert))))
			add(uint64(len(update.Insert)))
		case TupleFieldAddInt64:
			add(8)
		}
	}
	if err != nil {
		return 0, err
	}
	return int(size), nil
}

func appendTupleFieldUpdateJournalUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func readTupleFieldUpdateJournalUvarint(encoded []byte, position *int, limit int) (uint64, error) {
	if *position >= limit {
		return 0, fmt.Errorf("%w: truncated varint", ErrTupleFieldUpdateJournalInvalid)
	}
	value, size := binary.Uvarint(encoded[*position:limit])
	if size <= 0 || uint64(size) != tupleFieldUpdateJournalUvarintSize(value) {
		return 0, fmt.Errorf("%w: invalid varint", ErrTupleFieldUpdateJournalInvalid)
	}
	*position += size
	return value, nil
}

func readTupleFieldUpdateJournalBytes(encoded []byte, position, limit int) ([]byte, int, error) {
	length, err := readTupleFieldUpdateJournalUvarint(encoded, &position, limit)
	if err != nil {
		return nil, position, err
	}
	if length > MaxTupleFieldUpdateJournalBytes || length > uint64(limit-position) {
		return nil, position, fmt.Errorf("%w: byte field length %d", ErrTupleFieldUpdateJournalInvalid, length)
	}
	end := position + int(length)
	return append([]byte(nil), encoded[position:end]...), end, nil
}

func tupleFieldUpdateJournalInvalidRead(err error, field string) error {
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: invalid %s", ErrTupleFieldUpdateJournalInvalid, field)
}

func tupleFieldUpdateJournalUvarintSize(value uint64) uint64 {
	size := uint64(1)
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}
