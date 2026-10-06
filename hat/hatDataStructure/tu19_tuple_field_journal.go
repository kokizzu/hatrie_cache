package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// MaxTupleFieldUpdateJournalUpdates bounds operations in one replay record.
	MaxTupleFieldUpdateJournalUpdates = 256
	// MaxTupleFieldUpdateJournalPayloadBytes bounds Set and Splice payloads in
	// one record before the fixed header and checksum are added.
	MaxTupleFieldUpdateJournalPayloadBytes = 1 << 20
	// MaxTupleFieldUpdateJournalBytes bounds the complete HUF1 record.
	MaxTupleFieldUpdateJournalBytes           = 2 << 20
	tupleFieldUpdateJournalMagic              = "HUF1"
	tupleFieldUpdateJournalWireVersion   byte = 1
	tupleFieldUpdateJournalHeaderBytes        = 4 + 1 + 8 + 8 + 4
	tupleFieldUpdateJournalEntryBytes         = 4 + 1 + 4 + 4 + 8 + 4
	tupleFieldUpdateJournalChecksumBytes      = 4
)

var (
	ErrTupleFieldUpdateJournalInvalid  = errors.New("hatDataStructure: tuple update journal record is invalid")
	ErrTupleFieldUpdateJournalCorrupt  = errors.New("hatDataStructure: tuple update journal record is corrupt")
	ErrTupleFieldUpdateJournalVersion  = errors.New("hatDataStructure: tuple update journal format version mismatch")
	ErrTupleFieldUpdateJournalSequence = errors.New("hatDataStructure: tuple update journal sequence is invalid")
	ErrTupleFieldUpdateJournalLimit    = errors.New("hatDataStructure: tuple update journal record exceeds its limit")
)

var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// TupleFieldUpdateJournalRecord is one durable, replayable tuple mutation.
// FormatVersion fences a record against a caller's tuple schema/version.
type TupleFieldUpdateJournalRecord struct {
	Sequence      uint64
	FormatVersion uint64
	Updates       []TupleFieldUpdate
}

// EncodeTupleFieldUpdateJournalRecord encodes one deterministic HUF1 record.
// The payload is checksummed with CRC32C and all variable fields are bounded.
func EncodeTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	payloadBytes, err := validateTupleFieldUpdateJournalRecord(record)
	if err != nil {
		if errors.Is(err, ErrTupleFieldUpdateJournalLimit) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalInvalid, err)
	}
	total := tupleFieldUpdateJournalHeaderBytes + tupleFieldUpdateJournalChecksumBytes
	if len(record.Updates) > (int(^uint32(0))-total)/tupleFieldUpdateJournalEntryBytes {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	total += len(record.Updates) * tupleFieldUpdateJournalEntryBytes
	if payloadBytes > MaxTupleFieldUpdateJournalBytes-total {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	total += payloadBytes
	if total > MaxTupleFieldUpdateJournalBytes {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	encoded := make([]byte, total)
	copy(encoded, tupleFieldUpdateJournalMagic)
	encoded[4] = tupleFieldUpdateJournalWireVersion
	binary.BigEndian.PutUint64(encoded[5:13], record.Sequence)
	binary.BigEndian.PutUint64(encoded[13:21], record.FormatVersion)
	binary.BigEndian.PutUint32(encoded[21:25], uint32(len(record.Updates)))
	offset := tupleFieldUpdateJournalHeaderBytes
	for _, update := range record.Updates {
		binary.BigEndian.PutUint32(encoded[offset:offset+4], uint32(update.Index))
		offset += 4
		encoded[offset] = byte(update.Kind)
		offset++
		binary.BigEndian.PutUint32(encoded[offset:offset+4], uint32(update.Start))
		offset += 4
		binary.BigEndian.PutUint32(encoded[offset:offset+4], uint32(update.Remove))
		offset += 4
		binary.BigEndian.PutUint64(encoded[offset:offset+8], uint64(update.Delta))
		offset += 8
		payload := tupleFieldUpdateJournalPayload(update)
		binary.BigEndian.PutUint32(encoded[offset:offset+4], uint32(len(payload)))
		offset += 4
		copy(encoded[offset:offset+len(payload)], payload)
		offset += len(payload)
	}
	checksum := crc32.Checksum(encoded[:offset], tupleFieldUpdateJournalCRCTable)
	binary.BigEndian.PutUint32(encoded[offset:offset+tupleFieldUpdateJournalChecksumBytes], checksum)
	return encoded, nil
}

// DecodeTupleFieldUpdateJournalRecord strictly decodes and validates one HUF1
// record before returning owned payload bytes.
func DecodeTupleFieldUpdateJournalRecord(encoded []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(encoded) < tupleFieldUpdateJournalHeaderBytes+tupleFieldUpdateJournalChecksumBytes || len(encoded) > MaxTupleFieldUpdateJournalBytes {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	if string(encoded[:len(tupleFieldUpdateJournalMagic)]) != tupleFieldUpdateJournalMagic || encoded[4] != tupleFieldUpdateJournalWireVersion {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	checksumOffset := len(encoded) - tupleFieldUpdateJournalChecksumBytes
	wantChecksum := binary.BigEndian.Uint32(encoded[checksumOffset:])
	if crc32.Checksum(encoded[:checksumOffset], tupleFieldUpdateJournalCRCTable) != wantChecksum {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	record := TupleFieldUpdateJournalRecord{
		Sequence:      binary.BigEndian.Uint64(encoded[5:13]),
		FormatVersion: binary.BigEndian.Uint64(encoded[13:21]),
	}
	count := binary.BigEndian.Uint32(encoded[21:25])
	if count > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	record.Updates = make([]TupleFieldUpdate, 0, int(count))
	offset := tupleFieldUpdateJournalHeaderBytes
	for index := uint32(0); index < count; index++ {
		if checksumOffset-offset < tupleFieldUpdateJournalEntryBytes {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
		}
		update := TupleFieldUpdate{
			Index:  int(binary.BigEndian.Uint32(encoded[offset : offset+4])),
			Kind:   TupleFieldUpdateKind(encoded[offset+4]),
			Start:  int(binary.BigEndian.Uint32(encoded[offset+5 : offset+9])),
			Remove: int(binary.BigEndian.Uint32(encoded[offset+9 : offset+13])),
			Delta:  int64(binary.BigEndian.Uint64(encoded[offset+13 : offset+21])),
		}
		payloadLength := int(binary.BigEndian.Uint32(encoded[offset+21 : offset+25]))
		offset += tupleFieldUpdateJournalEntryBytes
		if payloadLength > checksumOffset-offset {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
		}
		payload := append([]byte(nil), encoded[offset:offset+payloadLength]...)
		switch update.Kind {
		case TupleFieldSet:
			update.Value = payload
		case TupleFieldSplice:
			update.Insert = payload
		case TupleFieldAddInt64:
			if payloadLength != 0 {
				return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
			}
		default:
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
		}
		record.Updates = append(record.Updates, update)
		offset += payloadLength
	}
	if offset != checksumOffset {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	if _, err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalCorrupt
	}
	return record, nil
}

// ApplyTupleFieldUpdateJournalRecord fences the record version and then uses
// the existing atomic TupleFieldOffsetCache update path for replay.
func ApplyTupleFieldUpdateJournalRecord(cache TupleFieldOffsetCache, record TupleFieldUpdateJournalRecord, expectedFormatVersion uint64) (TupleFieldOffsetCache, error) {
	if expectedFormatVersion == 0 || record.FormatVersion != expectedFormatVersion {
		return TupleFieldOffsetCache{}, ErrTupleFieldUpdateJournalVersion
	}
	if _, err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldOffsetCache{}, fmt.Errorf("%w: %v", ErrTupleFieldUpdateJournalInvalid, err)
	}
	return cache.ApplyUpdates(record.Updates)
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) (int, error) {
	if record.Sequence == 0 || record.FormatVersion == 0 {
		return 0, ErrTupleFieldUpdateJournalSequence
	}
	if len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return 0, ErrTupleFieldUpdateJournalInvalid
	}
	seen := make(map[int]struct{}, len(record.Updates))
	payloadBytes := 0
	for _, update := range record.Updates {
		if update.Index < 0 || update.Index >= MaxTupleFieldOffsetFields {
			return 0, ErrTupleFieldUpdateJournalInvalid
		}
		if _, exists := seen[update.Index]; exists {
			return 0, ErrTupleFieldUpdateJournalInvalid
		}
		seen[update.Index] = struct{}{}
		if update.Start < 0 || update.Remove < 0 {
			return 0, ErrTupleFieldUpdateJournalInvalid
		}
		payload := tupleFieldUpdateJournalPayload(update)
		switch update.Kind {
		case TupleFieldSet:
			if update.Start != 0 || update.Remove != 0 || update.Insert != nil || update.Delta != 0 {
				return 0, ErrTupleFieldUpdateJournalInvalid
			}
		case TupleFieldSplice:
			if update.Value != nil || update.Delta != 0 {
				return 0, ErrTupleFieldUpdateJournalInvalid
			}
		case TupleFieldAddInt64:
			if update.Value != nil || update.Insert != nil || update.Start != 0 || update.Remove != 0 {
				return 0, ErrTupleFieldUpdateJournalInvalid
			}
			payload = nil
		default:
			return 0, ErrTupleFieldUpdateJournalInvalid
		}
		if len(payload) > MaxTupleFieldUpdateJournalPayloadBytes-payloadBytes {
			return 0, ErrTupleFieldUpdateJournalLimit
		}
		payloadBytes += len(payload)
	}
	return payloadBytes, nil
}

func tupleFieldUpdateJournalPayload(update TupleFieldUpdate) []byte {
	switch update.Kind {
	case TupleFieldSet:
		return update.Value
	case TupleFieldSplice:
		return update.Insert
	default:
		return nil
	}
}
