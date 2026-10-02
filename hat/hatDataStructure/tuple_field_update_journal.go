package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	tupleFieldUpdateJournalMagic   = "TFJ1"
	tupleFieldUpdateJournalVersion = 1
	// MaxTupleFieldUpdateJournalBytes bounds one durable or transferred record.
	MaxTupleFieldUpdateJournalBytes = 16 << 20
	// MaxTupleFieldUpdateJournalUpdates bounds the number of operations in one
	// atomic record and prevents untrusted input from allocating without limit.
	MaxTupleFieldUpdateJournalUpdates = 1024
)

var (
	// ErrTupleFieldUpdateJournalInvalid reports malformed or unsupported wire
	// data, including invalid operation fields and trailing bytes.
	ErrTupleFieldUpdateJournalInvalid = errors.New("hatDataStructure: invalid tuple field update journal")
	// ErrTupleFieldUpdateJournalChecksum reports a checksum mismatch.
	ErrTupleFieldUpdateJournalChecksum = errors.New("hatDataStructure: tuple field update journal checksum mismatch")
)

var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournal is one ordered, atomically replayable tuple update
// record. Sequence is owned by the caller and is not interpreted by Apply.
type TupleFieldUpdateJournal struct {
	Sequence uint64
	Updates  []TupleFieldUpdate
}

// MarshalTupleFieldUpdateJournal encodes one bounded TFJ1 record. The
// operation payloads are length-delimited and the record ends with CRC32C.
func MarshalTupleFieldUpdateJournal(sequence uint64, updates []TupleFieldUpdate) ([]byte, error) {
	record := TupleFieldUpdateJournal{Sequence: sequence, Updates: updates}
	return record.MarshalBinary()
}

// MarshalBinary implements encoding.BinaryMarshaler for a TFJ1 record.
func (record TupleFieldUpdateJournal) MarshalBinary() ([]byte, error) {
	if len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return nil, fmt.Errorf("%w: update count %d exceeds %d", ErrTupleFieldUpdateJournalInvalid, len(record.Updates), MaxTupleFieldUpdateJournalUpdates)
	}
	seen := make(map[int]struct{}, len(record.Updates))
	encoded := make([]byte, 0, 32)
	encoded = append(encoded, tupleFieldUpdateJournalMagic...)
	encoded = append(encoded, tupleFieldUpdateJournalVersion)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, record.Sequence)
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		if update.Index < 0 {
			return nil, fmt.Errorf("%w: negative field index %d", ErrTupleFieldUpdateJournalInvalid, update.Index)
		}
		if _, exists := seen[update.Index]; exists {
			return nil, fmt.Errorf("%w: field %d: %w", ErrTupleFieldUpdateJournalInvalid, update.Index, ErrTupleFieldUpdateDuplicate)
		}
		seen[update.Index] = struct{}{}
		if update.Index > int(^uint(0)>>1) {
			return nil, fmt.Errorf("%w: field index %d is too large", ErrTupleFieldUpdateJournalInvalid, update.Index)
		}
		encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Index))
		encoded = append(encoded, byte(update.Kind))
		switch update.Kind {
		case TupleFieldSet:
			var ok bool
			encoded, ok = appendTupleFieldUpdateJournalBytes(encoded, update.Value)
			if !ok {
				return nil, fmt.Errorf("%w: SET payload is too large", ErrTupleFieldUpdateJournalInvalid)
			}
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 {
				return nil, fmt.Errorf("%w: negative splice range", ErrTupleFieldUpdateJournalInvalid)
			}
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Start))
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(update.Remove))
			var ok bool
			encoded, ok = appendTupleFieldUpdateJournalBytes(encoded, update.Insert)
			if !ok {
				return nil, fmt.Errorf("%w: SPLICE payload is too large", ErrTupleFieldUpdateJournalInvalid)
			}
		case TupleFieldAddInt64:
			encoded = appendTupleFieldUpdateJournalUvarint(encoded, tupleFieldUpdateJournalEncodeInt64(update.Delta))
		default:
			return nil, fmt.Errorf("%w: unsupported operation kind %d", ErrTupleFieldUpdateJournalInvalid, update.Kind)
		}
		if len(encoded)+4 > MaxTupleFieldUpdateJournalBytes {
			return nil, fmt.Errorf("%w: record exceeds %d bytes", ErrTupleFieldUpdateJournalInvalid, MaxTupleFieldUpdateJournalBytes)
		}
	}
	checksum := crc32.Checksum(encoded, tupleFieldUpdateJournalCRCTable)
	var checksumBytes [4]byte
	binary.BigEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

func appendTupleFieldUpdateJournalBytes(encoded, value []byte) ([]byte, bool) {
	if len(value) > MaxTupleFieldUpdateJournalBytes || len(encoded)+10+len(value)+4 > MaxTupleFieldUpdateJournalBytes {
		return encoded, false
	}
	encoded = appendTupleFieldUpdateJournalUvarint(encoded, uint64(len(value)))
	encoded = append(encoded, value...)
	return encoded, true
}

func appendTupleFieldUpdateJournalUvarint(encoded []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(buffer[:], value)
	return append(encoded, buffer[:count]...)
}

// UnmarshalTupleFieldUpdateJournal validates and decodes one complete TFJ1
// record. Returned byte fields own their storage and may be modified safely.
func UnmarshalTupleFieldUpdateJournal(encoded []byte) (TupleFieldUpdateJournal, error) {
	if len(encoded) < len(tupleFieldUpdateJournalMagic)+1+1+1+4 || len(encoded) > MaxTupleFieldUpdateJournalBytes {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: record length %d", ErrTupleFieldUpdateJournalInvalid, len(encoded))
	}
	if string(encoded[:len(tupleFieldUpdateJournalMagic)]) != tupleFieldUpdateJournalMagic {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: bad magic", ErrTupleFieldUpdateJournalInvalid)
	}
	versionOffset := len(tupleFieldUpdateJournalMagic)
	if encoded[versionOffset] != tupleFieldUpdateJournalVersion {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: unsupported version %d", ErrTupleFieldUpdateJournalInvalid, encoded[versionOffset])
	}
	checksumOffset := len(encoded) - 4
	wantChecksum := binary.BigEndian.Uint32(encoded[checksumOffset:])
	if gotChecksum := crc32.Checksum(encoded[:checksumOffset], tupleFieldUpdateJournalCRCTable); gotChecksum != wantChecksum {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: got %08x, want %08x", ErrTupleFieldUpdateJournalChecksum, gotChecksum, wantChecksum)
	}

	offset := versionOffset + 1
	sequence, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
	if err != nil {
		return TupleFieldUpdateJournal{}, err
	}
	count, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
	if err != nil {
		return TupleFieldUpdateJournal{}, err
	}
	if count > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: update count %d exceeds %d", ErrTupleFieldUpdateJournalInvalid, count, MaxTupleFieldUpdateJournalUpdates)
	}
	updates := make([]TupleFieldUpdate, int(count))
	seen := make(map[int]struct{}, int(count))
	for index := range updates {
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
		if err != nil {
			return TupleFieldUpdateJournal{}, err
		}
		if fieldIndex > uint64(^uint(0)>>1) {
			return TupleFieldUpdateJournal{}, fmt.Errorf("%w: field index %d is too large", ErrTupleFieldUpdateJournalInvalid, fieldIndex)
		}
		fieldIndexInt := int(fieldIndex)
		if _, exists := seen[fieldIndexInt]; exists {
			return TupleFieldUpdateJournal{}, fmt.Errorf("%w: field %d: %w", ErrTupleFieldUpdateJournalInvalid, fieldIndexInt, ErrTupleFieldUpdateDuplicate)
		}
		seen[fieldIndexInt] = struct{}{}
		if offset >= checksumOffset {
			return TupleFieldUpdateJournal{}, fmt.Errorf("%w: missing operation kind", ErrTupleFieldUpdateJournalInvalid)
		}
		update := TupleFieldUpdate{Index: fieldIndexInt, Kind: TupleFieldUpdateKind(encoded[offset])}
		offset++
		switch update.Kind {
		case TupleFieldSet:
			value, err := readTupleFieldUpdateJournalBytes(encoded[:checksumOffset], &offset)
			if err != nil {
				return TupleFieldUpdateJournal{}, err
			}
			update.Value = value
		case TupleFieldSplice:
			start, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
			if err != nil {
				return TupleFieldUpdateJournal{}, err
			}
			remove, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
			if err != nil {
				return TupleFieldUpdateJournal{}, err
			}
			if start > uint64(^uint(0)>>1) || remove > uint64(^uint(0)>>1) {
				return TupleFieldUpdateJournal{}, fmt.Errorf("%w: splice range is too large", ErrTupleFieldUpdateJournalInvalid)
			}
			insert, err := readTupleFieldUpdateJournalBytes(encoded[:checksumOffset], &offset)
			if err != nil {
				return TupleFieldUpdateJournal{}, err
			}
			update.Start = int(start)
			update.Remove = int(remove)
			update.Insert = insert
		case TupleFieldAddInt64:
			delta, err := readTupleFieldUpdateJournalUvarint(encoded[:checksumOffset], &offset)
			if err != nil {
				return TupleFieldUpdateJournal{}, err
			}
			update.Delta = tupleFieldUpdateJournalDecodeInt64(delta)
		default:
			return TupleFieldUpdateJournal{}, fmt.Errorf("%w: unsupported operation kind %d", ErrTupleFieldUpdateJournalInvalid, update.Kind)
		}
		updates[index] = update
	}
	if offset != checksumOffset {
		return TupleFieldUpdateJournal{}, fmt.Errorf("%w: trailing payload bytes", ErrTupleFieldUpdateJournalInvalid)
	}
	return TupleFieldUpdateJournal{Sequence: sequence, Updates: updates}, nil
}

func readTupleFieldUpdateJournalUvarint(encoded []byte, offset *int) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, fmt.Errorf("%w: truncated varint", ErrTupleFieldUpdateJournalInvalid)
	}
	value, count := binary.Uvarint(encoded[*offset:])
	if count <= 0 {
		return 0, fmt.Errorf("%w: invalid varint", ErrTupleFieldUpdateJournalInvalid)
	}
	*offset += count
	return value, nil
}

func readTupleFieldUpdateJournalBytes(encoded []byte, offset *int) ([]byte, error) {
	length, err := readTupleFieldUpdateJournalUvarint(encoded, offset)
	if err != nil {
		return nil, err
	}
	if length > uint64(len(encoded)-*offset) || length > MaxTupleFieldUpdateJournalBytes {
		return nil, fmt.Errorf("%w: payload length %d", ErrTupleFieldUpdateJournalInvalid, length)
	}
	end := *offset + int(length)
	value := append([]byte(nil), encoded[*offset:end]...)
	*offset = end
	return value, nil
}

func tupleFieldUpdateJournalEncodeInt64(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func tupleFieldUpdateJournalDecodeInt64(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

// Apply replays the validated operation batch through TupleFieldOffsetCache's
// existing all-or-nothing update path.
func (record TupleFieldUpdateJournal) Apply(cache TupleFieldOffsetCache) (TupleFieldOffsetCache, error) {
	return cache.ApplyUpdates(record.Updates)
}
