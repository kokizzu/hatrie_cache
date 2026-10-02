package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// MaxTupleFieldUpdateJournalBytes bounds one encoded journal record.
	MaxTupleFieldUpdateJournalBytes = 1 << 20
	// MaxTupleFieldUpdateJournalUpdates bounds the number of operations in one
	// journal record.
	MaxTupleFieldUpdateJournalUpdates = 1024
	// MaxTupleFieldUpdateJournalKeyBytes bounds the opaque row key in a record.
	MaxTupleFieldUpdateJournalKeyBytes = 64 << 10
	// MaxTupleFieldUpdateJournalValueBytes bounds one set or splice payload.
	MaxTupleFieldUpdateJournalValueBytes      = 256 << 10
	tupleFieldUpdateJournalWireVersion   byte = 1
)

var (
	// ErrTupleFieldUpdateJournalInvalid indicates an invalid record or applier
	// configuration.
	ErrTupleFieldUpdateJournalInvalid = errors.New("hatDataStructure: tuple update journal record is invalid")
	// ErrTupleFieldUpdateJournalWire indicates a malformed or unsupported wire
	// envelope.
	ErrTupleFieldUpdateJournalWire = errors.New("hatDataStructure: tuple update journal wire is invalid")
	// ErrTupleFieldUpdateJournalChecksum indicates a checksum mismatch.
	ErrTupleFieldUpdateJournalChecksum = errors.New("hatDataStructure: tuple update journal checksum mismatch")
	// ErrTupleFieldUpdateJournalLimit indicates a bounded journal field or
	// record exceeded its limit.
	ErrTupleFieldUpdateJournalLimit = errors.New("hatDataStructure: tuple update journal limit exceeded")
	// ErrTupleFieldUpdateJournalVersion indicates a record was applied to a
	// tuple with a different schema version.
	ErrTupleFieldUpdateJournalVersion = errors.New("hatDataStructure: tuple update journal schema version mismatch")
	// ErrTupleFieldUpdateJournalSequence indicates a duplicate or out-of-order
	// record was submitted to an applier.
	ErrTupleFieldUpdateJournalSequence = errors.New("hatDataStructure: tuple update journal sequence is not newer")
	// ErrTupleFieldUpdateJournalSequenceGap indicates a record skipped a
	// sequence after an applier was initialized.
	ErrTupleFieldUpdateJournalSequenceGap = errors.New("hatDataStructure: tuple update journal sequence gap")
)

var tupleFieldUpdateJournalMagic = [4]byte{'T', 'F', 'J', '1'}

var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalRecord is one durable, replayable batch of tuple
// operations. Key is opaque to this package and lets a caller route the
// record to its row; SchemaVersion must match the VersionedTuple being
// updated.
type TupleFieldUpdateJournalRecord struct {
	Sequence      uint64
	SchemaVersion uint64
	Key           []byte
	Updates       []TupleFieldUpdate
}

// TupleFieldUpdateJournalApplier enforces strictly increasing replay order.
// It advances its sequence only after the tuple update succeeds.
type TupleFieldUpdateJournalApplier struct {
	lastSequence uint64
}

// NewTupleFieldUpdateJournalApplier starts an applier after lastSequence. A
// nonzero starting sequence requires subsequent records to be contiguous.
func NewTupleFieldUpdateJournalApplier(lastSequence uint64) (*TupleFieldUpdateJournalApplier, error) {
	if lastSequence == ^uint64(0) {
		return nil, fmt.Errorf("%w: last sequence cannot be the maximum uint64", ErrTupleFieldUpdateJournalInvalid)
	}
	return &TupleFieldUpdateJournalApplier{lastSequence: lastSequence}, nil
}

// LastSequence returns the last successfully applied record sequence.
func (applier *TupleFieldUpdateJournalApplier) LastSequence() uint64 {
	if applier == nil {
		return 0
	}
	return applier.lastSequence
}

// Apply validates and applies one record, advancing the applier only after
// every operation and the destination tuple pass validation.
func (applier *TupleFieldUpdateJournalApplier) Apply(tuple VersionedTuple, format TupleFormat, record TupleFieldUpdateJournalRecord) (VersionedTuple, error) {
	if applier == nil {
		return VersionedTuple{}, ErrTupleFieldUpdateJournalInvalid
	}
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return VersionedTuple{}, err
	}
	if record.Sequence <= applier.lastSequence {
		return VersionedTuple{}, fmt.Errorf("%w: got %d after %d", ErrTupleFieldUpdateJournalSequence, record.Sequence, applier.lastSequence)
	}
	if applier.lastSequence != 0 && record.Sequence != applier.lastSequence+1 {
		return VersionedTuple{}, fmt.Errorf("%w: got %d after %d", ErrTupleFieldUpdateJournalSequenceGap, record.Sequence, applier.lastSequence)
	}
	updated, err := tuple.ApplyTupleFieldUpdateJournal(format, record)
	if err != nil {
		return VersionedTuple{}, err
	}
	applier.lastSequence = record.Sequence
	return updated, nil
}

// ApplyTupleFieldUpdateJournal applies a record without maintaining replay
// order. Use TupleFieldUpdateJournalApplier when records come from a durable
// ordered journal.
func (tuple VersionedTuple) ApplyTupleFieldUpdateJournal(format TupleFormat, record TupleFieldUpdateJournalRecord) (VersionedTuple, error) {
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return VersionedTuple{}, err
	}
	if tuple.Version() != record.SchemaVersion {
		return VersionedTuple{}, fmt.Errorf("%w: tuple has %d, record has %d", ErrTupleFieldUpdateJournalVersion, tuple.Version(), record.SchemaVersion)
	}
	if err := tuple.Validate(format); err != nil {
		return VersionedTuple{}, err
	}
	updated, err := tuple.tuple.ApplyUpdates(record.Updates)
	if err != nil {
		return VersionedTuple{}, err
	}
	if err := format.validateTuple(updated); err != nil {
		return VersionedTuple{}, err
	}
	return VersionedTuple{version: tuple.version, tuple: updated}, nil
}

// MarshalTupleFieldUpdateJournal encodes a bounded TFJ1 record with a CRC32C
// checksum over the complete envelope before the checksum trailer.
func MarshalTupleFieldUpdateJournal(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return nil, err
	}
	capacity, err := tupleFieldUpdateJournalCapacity(record)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, tupleFieldUpdateJournalMagic[:]...)
	encoded = append(encoded, tupleFieldUpdateJournalWireVersion)
	encoded = appendVersionedTupleUvarint(encoded, record.Sequence)
	encoded = appendVersionedTupleUvarint(encoded, record.SchemaVersion)
	encoded = appendTupleFieldUpdateJournalBytes(encoded, record.Key)
	encoded = appendVersionedTupleUvarint(encoded, uint64(len(record.Updates)))
	for _, update := range record.Updates {
		encoded = appendVersionedTupleUvarint(encoded, uint64(update.Index))
		encoded = append(encoded, byte(update.Kind))
		switch update.Kind {
		case TupleFieldSet:
			encoded = appendTupleFieldUpdateJournalBytes(encoded, update.Value)
		case TupleFieldSplice:
			encoded = appendVersionedTupleUvarint(encoded, uint64(update.Start))
			encoded = appendVersionedTupleUvarint(encoded, uint64(update.Remove))
			encoded = appendTupleFieldUpdateJournalBytes(encoded, update.Insert)
		case TupleFieldAddInt64:
			encoded = appendVersionedTupleUvarint(encoded, tupleFieldUpdateJournalEncodeInt64(update.Delta))
		}
	}
	if len(encoded) > MaxTupleFieldUpdateJournalBytes-4 {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	checksum := crc32.Checksum(encoded, tupleFieldUpdateJournalCRCTable)
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], checksum)
	encoded = append(encoded, trailer[:]...)
	return encoded, nil
}

// UnmarshalTupleFieldUpdateJournal strictly decodes and validates one TFJ1
// record before returning owned key and payload slices.
func UnmarshalTupleFieldUpdateJournal(encoded []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(encoded) > MaxTupleFieldUpdateJournalBytes {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	if len(encoded) < len(tupleFieldUpdateJournalMagic)+1+4 {
		return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalWireError("record is truncated")
	}
	if string(encoded[:len(tupleFieldUpdateJournalMagic)]) != string(tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalWireError("magic is not TFJ1")
	}
	if encoded[len(tupleFieldUpdateJournalMagic)] != tupleFieldUpdateJournalWireVersion {
		return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalWireError("wire version %d is unsupported", encoded[len(tupleFieldUpdateJournalMagic)])
	}
	bodyLength := len(encoded) - 4
	expected := binary.LittleEndian.Uint32(encoded[bodyLength:])
	actual := crc32.Checksum(encoded[:bodyLength], tupleFieldUpdateJournalCRCTable)
	if expected != actual {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: expected %08x, got %08x", ErrTupleFieldUpdateJournalChecksum, expected, actual)
	}

	offset := len(tupleFieldUpdateJournalMagic) + 1
	sequence, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	schemaVersion, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	key, err := readTupleFieldUpdateJournalBytes(encoded[:bodyLength], &offset, MaxTupleFieldUpdateJournalKeyBytes)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	updateCount, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	if updateCount > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: update count %d", ErrTupleFieldUpdateJournalLimit, updateCount)
	}
	updates := make([]TupleFieldUpdate, 0, int(updateCount))
	for index := uint64(0); index < updateCount; index++ {
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		if fieldIndex >= MaxTupleFieldOffsetFields {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: field index %d", ErrTupleFieldUpdateIndex, fieldIndex)
		}
		if offset >= bodyLength {
			return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalWireError("update kind is truncated")
		}
		kind := TupleFieldUpdateKind(encoded[offset])
		offset++
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: kind}
		switch kind {
		case TupleFieldSet:
			update.Value, err = readTupleFieldUpdateJournalBytes(encoded[:bodyLength], &offset, MaxTupleFieldUpdateJournalValueBytes)
		case TupleFieldSplice:
			start, startErr := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
			remove, removeErr := readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
			if startErr != nil {
				err = startErr
			} else if removeErr != nil {
				err = removeErr
			} else if start > MaxTupleFieldUpdateJournalValueBytes || remove > MaxTupleFieldUpdateJournalValueBytes {
				err = ErrTupleFieldUpdateJournalLimit
			} else {
				update.Start = int(start)
				update.Remove = int(remove)
				update.Insert, err = readTupleFieldUpdateJournalBytes(encoded[:bodyLength], &offset, MaxTupleFieldUpdateJournalValueBytes)
			}
		case TupleFieldAddInt64:
			var encodedDelta uint64
			encodedDelta, err = readTupleFieldUpdateJournalUvarint(encoded[:bodyLength], &offset)
			if err == nil {
				update.Delta = tupleFieldUpdateJournalDecodeInt64(encodedDelta)
			}
		default:
			err = ErrTupleFieldUpdateKind
		}
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, fmt.Errorf("%w: update %d: %w", ErrTupleFieldUpdateJournalWire, index, err)
		}
		updates = append(updates, update)
	}
	if offset != bodyLength {
		return TupleFieldUpdateJournalRecord{}, tupleFieldUpdateJournalWireError("trailing bytes after updates")
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence:      sequence,
		SchemaVersion: schemaVersion,
		Key:           key,
		Updates:       updates,
	}
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	return record, nil
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) error {
	if record.Sequence == 0 || record.SchemaVersion == 0 {
		return fmt.Errorf("%w: sequence and schema version must be positive", ErrTupleFieldUpdateJournalInvalid)
	}
	if len(record.Key) > MaxTupleFieldUpdateJournalKeyBytes {
		return fmt.Errorf("%w: key length %d", ErrTupleFieldUpdateJournalLimit, len(record.Key))
	}
	if len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return fmt.Errorf("%w: update count %d", ErrTupleFieldUpdateJournalLimit, len(record.Updates))
	}
	seen := make(map[int]struct{}, len(record.Updates))
	for index, update := range record.Updates {
		if update.Index < 0 || update.Index >= MaxTupleFieldOffsetFields {
			return fmt.Errorf("%w: update %d field index %d", ErrTupleFieldUpdateJournalInvalid, index, update.Index)
		}
		if _, exists := seen[update.Index]; exists {
			return fmt.Errorf("%w: field %d", ErrTupleFieldUpdateDuplicate, update.Index)
		}
		seen[update.Index] = struct{}{}
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > MaxTupleFieldUpdateJournalValueBytes {
				return fmt.Errorf("%w: set value length %d", ErrTupleFieldUpdateJournalLimit, len(update.Value))
			}
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 || update.Start > MaxTupleFieldUpdateJournalValueBytes || update.Remove > MaxTupleFieldUpdateJournalValueBytes {
				return fmt.Errorf("%w: splice bounds", ErrTupleFieldUpdateJournalInvalid)
			}
			if len(update.Insert) > MaxTupleFieldUpdateJournalValueBytes {
				return fmt.Errorf("%w: splice insert length %d", ErrTupleFieldUpdateJournalLimit, len(update.Insert))
			}
		case TupleFieldAddInt64:
		default:
			return fmt.Errorf("%w: update %d kind %d", ErrTupleFieldUpdateJournalInvalid, index, update.Kind)
		}
	}
	return nil
}

func tupleFieldUpdateJournalCapacity(record TupleFieldUpdateJournalRecord) (int, error) {
	capacity := len(tupleFieldUpdateJournalMagic) + 1 + versionedTupleUvarintLen(record.Sequence) + versionedTupleUvarintLen(record.SchemaVersion) + versionedTupleUvarintLen(uint64(len(record.Key)+1)) + len(record.Key) + versionedTupleUvarintLen(uint64(len(record.Updates)))
	for _, update := range record.Updates {
		if err := addTupleFieldUpdateJournalSize(&capacity, versionedTupleUvarintLen(uint64(update.Index))+1); err != nil {
			return 0, err
		}
		switch update.Kind {
		case TupleFieldSet:
			if err := addTupleFieldUpdateJournalSize(&capacity, versionedTupleUvarintLen(uint64(len(update.Value)+1))+len(update.Value)); err != nil {
				return 0, err
			}
		case TupleFieldSplice:
			if err := addTupleFieldUpdateJournalSize(&capacity, versionedTupleUvarintLen(uint64(update.Start))+versionedTupleUvarintLen(uint64(update.Remove))+versionedTupleUvarintLen(uint64(len(update.Insert)+1))+len(update.Insert)); err != nil {
				return 0, err
			}
		case TupleFieldAddInt64:
			if err := addTupleFieldUpdateJournalSize(&capacity, versionedTupleUvarintLen(tupleFieldUpdateJournalEncodeInt64(update.Delta))); err != nil {
				return 0, err
			}
		}
	}
	if err := addTupleFieldUpdateJournalSize(&capacity, 4); err != nil {
		return 0, err
	}
	return capacity, nil
}

func addTupleFieldUpdateJournalSize(size *int, addition int) error {
	if addition < 0 || *size > MaxTupleFieldUpdateJournalBytes-addition {
		return ErrTupleFieldUpdateJournalLimit
	}
	*size += addition
	return nil
}

func appendTupleFieldUpdateJournalBytes(destination, value []byte) []byte {
	destination = appendVersionedTupleUvarint(destination, uint64(len(value))+1)
	return append(destination, value...)
}

func readTupleFieldUpdateJournalBytes(encoded []byte, offset *int, maximum int) ([]byte, error) {
	lengthCode, err := readTupleFieldUpdateJournalUvarint(encoded, offset)
	if err != nil {
		return nil, err
	}
	if lengthCode == 0 {
		return nil, tupleFieldUpdateJournalWireError("zero length code")
	}
	length := lengthCode - 1
	if length > uint64(maximum) {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	if length > uint64(len(encoded)-*offset) {
		return nil, tupleFieldUpdateJournalWireError("payload is truncated")
	}
	start := *offset
	*offset += int(length)
	return append([]byte(nil), encoded[start:*offset]...), nil
}

func readTupleFieldUpdateJournalUvarint(encoded []byte, offset *int) (uint64, error) {
	value, err := readVersionedTupleUvarint(encoded, offset)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalWire, err)
	}
	return value, nil
}

func tupleFieldUpdateJournalWireError(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrTupleFieldUpdateJournalWire, fmt.Sprintf(format, args...))
}

func tupleFieldUpdateJournalEncodeInt64(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func tupleFieldUpdateJournalDecodeInt64(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}
