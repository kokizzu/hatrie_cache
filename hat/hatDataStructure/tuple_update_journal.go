package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	// MaxTupleFieldUpdateJournalRecordBytes bounds one encoded record, including
	// its framing and checksum.
	MaxTupleFieldUpdateJournalRecordBytes = 4 << 20
	// MaxTupleFieldUpdateJournalKeyBytes bounds the logical tuple key retained
	// in one replay record.
	MaxTupleFieldUpdateJournalKeyBytes = 32 << 10
	// MaxTupleFieldUpdateJournalUpdates bounds the number of field operations in
	// one atomic replay record.
	MaxTupleFieldUpdateJournalUpdates   = 256
	maxTupleFieldUpdateJournalIndex     = 1<<31 - 1
	tupleFieldUpdateJournalHeaderBytes  = 4 + 1 + 4
	tupleFieldUpdateJournalChecksumSize = 4
)

var (
	// ErrTupleFieldUpdateJournalInvalid indicates malformed record fields or
	// an unsupported operation encoding.
	ErrTupleFieldUpdateJournalInvalid = errors.New("hatDataStructure: tuple update journal record is invalid")
	// ErrTupleFieldUpdateJournalVersion indicates an unsupported HTJ1 version.
	ErrTupleFieldUpdateJournalVersion = errors.New("hatDataStructure: tuple update journal version is unsupported")
	// ErrTupleFieldUpdateJournalChecksum indicates a corrupted record.
	ErrTupleFieldUpdateJournalChecksum = errors.New("hatDataStructure: tuple update journal checksum mismatch")
	// ErrTupleFieldUpdateJournalTruncated indicates an incomplete record.
	ErrTupleFieldUpdateJournalTruncated = errors.New("hatDataStructure: tuple update journal record is truncated")
	// ErrTupleFieldUpdateJournalLimit indicates a bounded record limit was
	// exceeded before allocation or replay.
	ErrTupleFieldUpdateJournalLimit = errors.New("hatDataStructure: tuple update journal record exceeds its limit")
)

var tupleFieldUpdateJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

var tupleFieldUpdateJournalMagic = [4]byte{'H', 'T', 'J', '1'}

// TupleFieldUpdateJournalRecord is one durable, replayable atomic tuple-field
// update. The caller supplies Sequence and Key identity; the record codec does
// not retain or interpret external storage state.
type TupleFieldUpdateJournalRecord struct {
	Sequence uint64
	Key      string
	Updates  []TupleFieldUpdate
}

// MarshalTupleFieldUpdateJournalRecord encodes one bounded HTJ1 record. The
// binary format is length-framed, little-endian for fixed-width fields, and
// ends with a CRC32C over the header and payload.
func MarshalTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) ([]byte, error) {
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return nil, err
	}
	payload := make([]byte, 0, len(record.Key)+len(record.Updates)*16+16)
	var fixed [8]byte
	binary.LittleEndian.PutUint64(fixed[:], record.Sequence)
	payload = append(payload, fixed[:]...)
	payload = appendTupleFieldUpdateJournalUvarint(payload, uint64(len(record.Key)))
	payload = append(payload, record.Key...)
	payload = appendTupleFieldUpdateJournalUvarint(payload, uint64(len(record.Updates)))
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
			payload = appendTupleFieldUpdateJournalUvarint(payload, tupleFieldUpdateJournalZigZag(update.Delta))
		}
	}
	if len(payload) > maxTupleFieldUpdateJournalPayloadBytes() {
		return nil, ErrTupleFieldUpdateJournalLimit
	}

	encoded := make([]byte, 0, tupleFieldUpdateJournalHeaderBytes+len(payload)+tupleFieldUpdateJournalChecksumSize)
	encoded = append(encoded, tupleFieldUpdateJournalMagic[:]...)
	encoded = append(encoded, 1)
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(payload)))
	encoded = append(encoded, length[:]...)
	encoded = append(encoded, payload...)
	checksum := crc32.Checksum(encoded, tupleFieldUpdateJournalCRCTable)
	binary.LittleEndian.PutUint32(length[:], checksum)
	encoded = append(encoded, length[:]...)
	return encoded, nil
}

// UnmarshalTupleFieldUpdateJournalRecord validates and decodes exactly one
// HTJ1 record. All variable-length fields are copied out of data before the
// function returns.
func UnmarshalTupleFieldUpdateJournalRecord(data []byte) (TupleFieldUpdateJournalRecord, error) {
	if len(data) < tupleFieldUpdateJournalHeaderBytes+tupleFieldUpdateJournalChecksumSize {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
	}
	if !bytes.Equal(data[:4], tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
	}
	if data[4] != 1 {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalVersion
	}
	payloadLength := int(binary.LittleEndian.Uint32(data[5:9]))
	if payloadLength > maxTupleFieldUpdateJournalPayloadBytes() {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	expectedLength := tupleFieldUpdateJournalHeaderBytes + payloadLength + tupleFieldUpdateJournalChecksumSize
	if len(data) < expectedLength {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
	}
	if len(data) != expectedLength {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
	}
	wantChecksum := binary.LittleEndian.Uint32(data[expectedLength-tupleFieldUpdateJournalChecksumSize:])
	if gotChecksum := crc32.Checksum(data[:expectedLength-tupleFieldUpdateJournalChecksumSize], tupleFieldUpdateJournalCRCTable); gotChecksum != wantChecksum {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalChecksum
	}

	payload := data[tupleFieldUpdateJournalHeaderBytes : tupleFieldUpdateJournalHeaderBytes+payloadLength]
	offset := 0
	sequence, err := readTupleFieldUpdateJournalFixedUint64(payload, &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	keyBytes, err := readTupleFieldUpdateJournalBytes(payload, &offset, MaxTupleFieldUpdateJournalKeyBytes)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	count, err := readTupleFieldUpdateJournalUvarint(payload, &offset)
	if err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	if count == 0 || count > MaxTupleFieldUpdateJournalUpdates {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	updates := make([]TupleFieldUpdate, 0, int(count))
	for index := uint64(0); index < count; index++ {
		fieldIndex, err := readTupleFieldUpdateJournalUvarint(payload, &offset)
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		if fieldIndex > maxTupleFieldUpdateJournalIndex {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
		}
		if offset >= len(payload) {
			return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
		}
		kind := TupleFieldUpdateKind(payload[offset])
		offset++
		update := TupleFieldUpdate{Index: int(fieldIndex), Kind: kind}
		switch kind {
		case TupleFieldSet:
			update.Value, err = readTupleFieldUpdateJournalBytes(payload, &offset, maxTupleFieldUpdateJournalPayloadBytes())
		case TupleFieldSplice:
			start, startErr := readTupleFieldUpdateJournalUvarint(payload, &offset)
			if startErr != nil {
				err = startErr
				break
			}
			remove, removeErr := readTupleFieldUpdateJournalUvarint(payload, &offset)
			if removeErr != nil {
				err = removeErr
				break
			}
			update.Start, err = journalUint64ToInt(start)
			if err != nil {
				break
			}
			update.Remove, err = journalUint64ToInt(remove)
			if err != nil {
				break
			}
			update.Insert, err = readTupleFieldUpdateJournalBytes(payload, &offset, maxTupleFieldUpdateJournalPayloadBytes())
		case TupleFieldAddInt64:
			encodedDelta, deltaErr := readTupleFieldUpdateJournalUvarint(payload, &offset)
			if deltaErr != nil {
				err = deltaErr
				break
			}
			update.Delta = tupleFieldUpdateJournalUnZigZag(encodedDelta)
		default:
			err = ErrTupleFieldUpdateJournalInvalid
		}
		if err != nil {
			return TupleFieldUpdateJournalRecord{}, err
		}
		updates = append(updates, update)
	}
	if offset != len(payload) {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
	}
	record := TupleFieldUpdateJournalRecord{Sequence: sequence, Key: string(keyBytes), Updates: updates}
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return TupleFieldUpdateJournalRecord{}, err
	}
	return record, nil
}

// WriteTupleFieldUpdateJournalRecord appends one encoded record to a writer.
// The writer owns durability and sync policy; short writes are rejected.
func WriteTupleFieldUpdateJournalRecord(writer io.Writer, record TupleFieldUpdateJournalRecord) error {
	if writer == nil {
		return ErrTupleFieldUpdateJournalInvalid
	}
	encoded, err := MarshalTupleFieldUpdateJournalRecord(record)
	if err != nil {
		return err
	}
	for len(encoded) > 0 {
		written, writeErr := writer.Write(encoded)
		if written < 0 || written > len(encoded) {
			return fmt.Errorf("%w: invalid writer count %d", ErrTupleFieldUpdateJournalInvalid, written)
		}
		if written == 0 {
			if writeErr != nil {
				return writeErr
			}
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}

// ReadTupleFieldUpdateJournalRecord reads one framed record from a stream.
// Clean EOF before a new header is returned unchanged; a partial header or
// payload returns an error matching ErrTupleFieldUpdateJournalTruncated.
func ReadTupleFieldUpdateJournalRecord(reader io.Reader) (TupleFieldUpdateJournalRecord, error) {
	if reader == nil {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
	}
	var header [tupleFieldUpdateJournalHeaderBytes]byte
	read, err := io.ReadFull(reader, header[:])
	if err != nil {
		if read == 0 && errors.Is(err, io.EOF) {
			return TupleFieldUpdateJournalRecord{}, io.EOF
		}
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
	}
	if !bytes.Equal(header[:4], tupleFieldUpdateJournalMagic[:]) {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalInvalid
	}
	if header[4] != 1 {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalVersion
	}
	payloadLength := int(binary.LittleEndian.Uint32(header[5:9]))
	if payloadLength > maxTupleFieldUpdateJournalPayloadBytes() {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalLimit
	}
	rest := make([]byte, payloadLength+tupleFieldUpdateJournalChecksumSize)
	if _, err := io.ReadFull(reader, rest); err != nil {
		return TupleFieldUpdateJournalRecord{}, ErrTupleFieldUpdateJournalTruncated
	}
	data := make([]byte, 0, len(header)+len(rest))
	data = append(data, header[:]...)
	data = append(data, rest...)
	return UnmarshalTupleFieldUpdateJournalRecord(data)
}

// Apply replays the record through TupleFieldOffsetCache.ApplyUpdates. The
// underlying update operation validates the complete batch before mutation, so
// an invalid record cannot partially change the returned or source cache.
func (record TupleFieldUpdateJournalRecord) Apply(cache TupleFieldOffsetCache) (TupleFieldOffsetCache, error) {
	if err := validateTupleFieldUpdateJournalRecord(record); err != nil {
		return cache, err
	}
	return cache.ApplyUpdates(record.Updates)
}

func validateTupleFieldUpdateJournalRecord(record TupleFieldUpdateJournalRecord) error {
	if record.Key == "" || len(record.Key) > MaxTupleFieldUpdateJournalKeyBytes || len(record.Updates) == 0 || len(record.Updates) > MaxTupleFieldUpdateJournalUpdates {
		return ErrTupleFieldUpdateJournalInvalid
	}
	seen := make(map[int]struct{}, len(record.Updates))
	for _, update := range record.Updates {
		if update.Index < 0 || update.Index > maxTupleFieldUpdateJournalIndex {
			return ErrTupleFieldUpdateJournalInvalid
		}
		if _, exists := seen[update.Index]; exists {
			return ErrTupleFieldUpdateJournalInvalid
		}
		seen[update.Index] = struct{}{}
		switch update.Kind {
		case TupleFieldSet:
			if len(update.Value) > maxTupleFieldUpdateJournalPayloadBytes() {
				return ErrTupleFieldUpdateJournalLimit
			}
		case TupleFieldSplice:
			if update.Start < 0 || update.Remove < 0 || len(update.Insert) > maxTupleFieldUpdateJournalPayloadBytes() {
				return ErrTupleFieldUpdateJournalInvalid
			}
		case TupleFieldAddInt64:
		default:
			return ErrTupleFieldUpdateJournalInvalid
		}
	}
	return nil
}

func maxTupleFieldUpdateJournalPayloadBytes() int {
	return MaxTupleFieldUpdateJournalRecordBytes - tupleFieldUpdateJournalHeaderBytes - tupleFieldUpdateJournalChecksumSize
}

func appendTupleFieldUpdateJournalUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(destination, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendTupleFieldUpdateJournalBytes(destination, value []byte) []byte {
	destination = appendTupleFieldUpdateJournalUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

func readTupleFieldUpdateJournalFixedUint64(data []byte, offset *int) (uint64, error) {
	if *offset < 0 || len(data)-*offset < 8 {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	value := binary.LittleEndian.Uint64(data[*offset : *offset+8])
	*offset += 8
	return value, nil
}

func readTupleFieldUpdateJournalUvarint(data []byte, offset *int) (uint64, error) {
	if *offset < 0 || *offset >= len(data) {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	value, length := binary.Uvarint(data[*offset:])
	if length == 0 {
		return 0, ErrTupleFieldUpdateJournalTruncated
	}
	if length < 0 {
		return 0, ErrTupleFieldUpdateJournalInvalid
	}
	*offset += length
	return value, nil
}

func readTupleFieldUpdateJournalBytes(data []byte, offset *int, maximum int) ([]byte, error) {
	length, err := readTupleFieldUpdateJournalUvarint(data, offset)
	if err != nil {
		return nil, err
	}
	if length > uint64(maximum) {
		return nil, ErrTupleFieldUpdateJournalLimit
	}
	if length > uint64(len(data)-*offset) {
		return nil, ErrTupleFieldUpdateJournalTruncated
	}
	end := *offset + int(length)
	value := append([]byte(nil), data[*offset:end]...)
	*offset = end
	return value, nil
}

func journalUint64ToInt(value uint64) (int, error) {
	if value > uint64(maxTupleFieldUpdateJournalIndex) {
		return 0, ErrTupleFieldUpdateJournalInvalid
	}
	return int(value), nil
}

func tupleFieldUpdateJournalZigZag(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func tupleFieldUpdateJournalUnZigZag(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}
