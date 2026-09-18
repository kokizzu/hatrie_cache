package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// MaxVersionedTupleWireBytes bounds one encoded versioned tuple before any
	// payload allocation is attempted.
	MaxVersionedTupleWireBytes      = 64 << 20
	versionedTupleWireVersion  byte = 1
)

var (
	// ErrVersionedTupleInvalid indicates an uninitialized or otherwise invalid
	// versioned tuple envelope.
	ErrVersionedTupleInvalid = errors.New("hatDataStructure: versioned tuple is invalid")
	// ErrVersionedTupleVersionMismatch indicates a record was checked against a
	// different schema version.
	ErrVersionedTupleVersionMismatch = errors.New("hatDataStructure: versioned tuple schema version mismatch")
	// ErrVersionedTupleWire indicates malformed or unsupported encoded data.
	ErrVersionedTupleWire = errors.New("hatDataStructure: versioned tuple wire is invalid")
	// ErrVersionedTupleLimit indicates an encoded tuple exceeds a safety bound.
	ErrVersionedTupleLimit = errors.New("hatDataStructure: versioned tuple limit exceeded")
)

var versionedTupleMagic = [4]byte{'H', 'T', 'V', '1'}

// VersionedTuple couples a packed tuple with the TupleFormat version that
// produced it. It is opt-in, so existing TupleFieldOffsetCache values retain
// their original size and behavior.
//
// The tuple data remains borrowed according to TupleFieldOffsetCache. Use
// Clone when the versioned tuple must outlive the source data.
type VersionedTuple struct {
	version uint64
	tuple   TupleFieldOffsetCache
}

// NewVersionedTuple packs values and records the format version without an
// additional copy beyond TupleFormat.Pack.
func NewVersionedTuple(format TupleFormat, values []TupleFieldValue) (VersionedTuple, error) {
	tuple, err := format.Pack(values)
	if err != nil {
		return VersionedTuple{}, err
	}
	return NewVersionedTupleFromCache(format, tuple)
}

// PackVersioned is the format-oriented spelling of NewVersionedTuple.
func (format TupleFormat) PackVersioned(values []TupleFieldValue) (VersionedTuple, error) {
	return NewVersionedTuple(format, values)
}

// NewVersionedTupleFromCache attaches a format version after validating the
// existing tuple. It does not copy tuple data.
func NewVersionedTupleFromCache(format TupleFormat, tuple TupleFieldOffsetCache) (VersionedTuple, error) {
	if err := format.validateDefinition(); err != nil {
		return VersionedTuple{}, err
	}
	if err := format.Validate(tuple); err != nil {
		return VersionedTuple{}, err
	}
	return VersionedTuple{version: format.version, tuple: tuple}, nil
}

// Version returns the schema version recorded in the envelope. Zero means
// the VersionedTuple is uninitialized.
func (tuple VersionedTuple) Version() uint64 { return tuple.version }

// Tuple returns the packed tuple without copying its data.
func (tuple VersionedTuple) Tuple() TupleFieldOffsetCache { return tuple.tuple }

// Clone returns an independently owned versioned tuple.
func (tuple VersionedTuple) Clone() VersionedTuple {
	return VersionedTuple{version: tuple.version, tuple: tuple.tuple.Clone()}
}

// Validate checks the recorded version before validating the tuple's field
// shape and physical encodings. This is the schema-version boundary callers
// should use when accepting rows from storage, replication, or a wire peer.
func (tuple VersionedTuple) Validate(format TupleFormat) error {
	if tuple.version == 0 {
		return ErrVersionedTupleInvalid
	}
	if err := format.validateDefinition(); err != nil {
		return err
	}
	if tuple.version != format.version {
		return fmt.Errorf("%w: expected %d, got %d", ErrVersionedTupleVersionMismatch, format.version, tuple.version)
	}
	return format.validateTuple(tuple.tuple)
}

// ApplyUpdates validates and applies an atomic field update while retaining
// the schema version. The format is required so updated bytes cannot silently
// escape the same validation boundary.
func (tuple VersionedTuple) ApplyUpdates(format TupleFormat, updates []TupleFieldUpdate) (VersionedTuple, error) {
	if err := tuple.Validate(format); err != nil {
		return VersionedTuple{}, err
	}
	updated, err := tuple.tuple.ApplyUpdates(updates)
	if err != nil {
		return VersionedTuple{}, err
	}
	if err := format.validateTuple(updated); err != nil {
		return VersionedTuple{}, err
	}
	return VersionedTuple{version: tuple.version, tuple: updated}, nil
}

// MarshalVersionedTuple encodes a bounded HTV1 envelope. Each field is
// represented by a uvarint length plus one followed by bytes; zero denotes
// SQL NULL, preserving the distinction between NULL and an empty field.
func MarshalVersionedTuple(tuple VersionedTuple) ([]byte, error) {
	if tuple.version == 0 {
		return nil, ErrVersionedTupleInvalid
	}
	fieldCount := tuple.tuple.FieldCount()
	if fieldCount > MaxTupleFieldOffsetFields {
		return nil, fmt.Errorf("%w: field count %d exceeds %d", ErrVersionedTupleLimit, fieldCount, MaxTupleFieldOffsetFields)
	}
	encoded := make([]byte, 0, versionedTupleWireCapacity(tuple))
	encoded = append(encoded, versionedTupleMagic[:]...)
	encoded = append(encoded, versionedTupleWireVersion)
	encoded = appendVersionedTupleUvarint(encoded, tuple.version)
	encoded = appendVersionedTupleUvarint(encoded, uint64(fieldCount))
	for index := 0; index < fieldCount; index++ {
		valid, err := tuple.tuple.FieldValid(index)
		if err != nil {
			return nil, err
		}
		if !valid {
			if len(encoded) >= MaxVersionedTupleWireBytes {
				return nil, ErrVersionedTupleLimit
			}
			encoded = append(encoded, 0)
			continue
		}
		field, err := tuple.tuple.Field(index)
		if err != nil {
			return nil, err
		}
		lengthCode := uint64(len(field)) + 1
		lengthBytes := versionedTupleUvarintLen(lengthCode)
		if len(encoded) > MaxVersionedTupleWireBytes-lengthBytes || len(field) > MaxVersionedTupleWireBytes-len(encoded)-lengthBytes {
			return nil, ErrVersionedTupleLimit
		}
		encoded = appendVersionedTupleUvarint(encoded, lengthCode)
		encoded = append(encoded, field...)
	}
	if len(encoded) > MaxVersionedTupleWireBytes {
		return nil, ErrVersionedTupleLimit
	}
	return encoded, nil
}

// UnmarshalVersionedTuple strictly decodes an HTV1 envelope and bounds every
// length before copying bytes into an owned tuple.
func UnmarshalVersionedTuple(encoded []byte) (VersionedTuple, error) {
	if len(encoded) > MaxVersionedTupleWireBytes || len(encoded) < len(versionedTupleMagic)+1 {
		return VersionedTuple{}, versionedTupleWireError("payload length %d is outside bounds", len(encoded))
	}
	if !bytes.Equal(encoded[:len(versionedTupleMagic)], versionedTupleMagic[:]) {
		return VersionedTuple{}, versionedTupleWireError("magic is not HTV1")
	}
	if encoded[len(versionedTupleMagic)] != versionedTupleWireVersion {
		return VersionedTuple{}, versionedTupleWireError("wire version %d is unsupported", encoded[len(versionedTupleMagic)])
	}

	offset := len(versionedTupleMagic) + 1
	version, err := readVersionedTupleUvarint(encoded, &offset)
	if err != nil {
		return VersionedTuple{}, err
	}
	if version == 0 {
		return VersionedTuple{}, ErrVersionedTupleInvalid
	}
	fieldCount, err := readVersionedTupleUvarint(encoded, &offset)
	if err != nil {
		return VersionedTuple{}, err
	}
	if fieldCount > MaxTupleFieldOffsetFields {
		return VersionedTuple{}, fmt.Errorf("%w: field count %d exceeds %d", ErrVersionedTupleLimit, fieldCount, MaxTupleFieldOffsetFields)
	}
	if fieldCount > uint64(len(encoded)-offset) {
		return VersionedTuple{}, versionedTupleWireError("field count %d exceeds remaining bytes", fieldCount)
	}

	count := int(fieldCount)
	offsets := make([]uint32, count+1)
	var data []byte
	var valid []bool
	for index := 0; index < count; index++ {
		lengthCode, err := readVersionedTupleUvarint(encoded, &offset)
		if err != nil {
			return VersionedTuple{}, err
		}
		if lengthCode == 0 {
			if valid == nil {
				valid = make([]bool, count)
				for previous := 0; previous < index; previous++ {
					valid[previous] = true
				}
			}
			offsets[index+1] = uint32(len(data))
			continue
		}
		fieldLength := lengthCode - 1
		if fieldLength > uint64(^uint32(0)) || fieldLength > uint64(len(encoded)-offset) {
			return VersionedTuple{}, versionedTupleWireError("field %d length %d exceeds remaining bytes", index, fieldLength)
		}
		if uint64(len(data))+fieldLength > uint64(^uint32(0)) {
			return VersionedTuple{}, ErrVersionedTupleLimit
		}
		end := offset + int(fieldLength)
		data = append(data, encoded[offset:end]...)
		offset = end
		offsets[index+1] = uint32(len(data))
		if valid != nil {
			valid[index] = true
		}
	}
	if offset != len(encoded) {
		return VersionedTuple{}, versionedTupleWireError("payload has %d trailing bytes", len(encoded)-offset)
	}
	return VersionedTuple{version: version, tuple: TupleFieldOffsetCache{data: data, offsets: offsets, valid: valid}}, nil
}

func versionedTupleWireCapacity(tuple VersionedTuple) int {
	capacity := len(versionedTupleMagic) + 1 + 2*binary.MaxVarintLen64 + len(tuple.tuple.data)
	if tuple.tuple.FieldCount() > 0 && capacity <= MaxVersionedTupleWireBytes-tuple.tuple.FieldCount()*binary.MaxVarintLen64 {
		capacity += tuple.tuple.FieldCount() * binary.MaxVarintLen64
	}
	if capacity > MaxVersionedTupleWireBytes {
		return MaxVersionedTupleWireBytes
	}
	return capacity
}

func appendVersionedTupleUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

func versionedTupleUvarintLen(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func readVersionedTupleUvarint(data []byte, offset *int) (uint64, error) {
	if *offset >= len(data) {
		return 0, versionedTupleWireError("truncated uvarint")
	}
	value, length := binary.Uvarint(data[*offset:])
	if length == 0 {
		return 0, versionedTupleWireError("truncated uvarint")
	}
	if length < 0 {
		return 0, versionedTupleWireError("uvarint overflows uint64")
	}
	*offset += length
	return value, nil
}

func versionedTupleWireError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrVersionedTupleWire, fmt.Sprintf(format, args...))
}
