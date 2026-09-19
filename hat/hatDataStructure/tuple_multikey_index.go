package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

var errInvalidTupleMultikeyValue = errors.New("hatDataStructure: invalid tuple multikey value")

// TupleMultikeyIndexOptions bounds one item and the total number of indexed
// items. A zero bound means unlimited, matching StringMultikeyIndexOptions.
type TupleMultikeyIndexOptions struct {
	MaxKeysPerItem int
	MaxItems       int
}

// TupleMultikeyIndex indexes typed tuple values while reusing the sorted,
// compact posting lists of StringMultikeyIndex. Values with different tuple
// types never share a posting, even when their textual forms match.
type TupleMultikeyIndex struct {
	index *StringMultikeyIndex
}

// NewTupleMultikeyIndex creates an empty typed multikey index.
func NewTupleMultikeyIndex(options TupleMultikeyIndexOptions) *TupleMultikeyIndex {
	return &TupleMultikeyIndex{
		index: NewStringMultikeyIndex(StringMultikeyIndexOptions{
			MaxKeysPerItem: options.MaxKeysPerItem,
			MaxItems:       options.MaxItems,
		}),
	}
}

// Set replaces all keys for id. Values are encoded before the underlying
// index is changed, so an invalid value or bound violation leaves old keys
// untouched.
func (index *TupleMultikeyIndex) Set(id uint64, keys []TupleFieldValue) error {
	if index == nil || index.index == nil {
		return errors.New("hatDataStructure: nil tuple multikey index")
	}
	encoded := make([]string, len(keys))
	for position, key := range keys {
		value, err := encodeTupleMultikeyKey(key)
		if err != nil {
			return fmt.Errorf("tuple multikey key %d: %w", position, err)
		}
		encoded[position] = value
	}
	return index.index.Set(id, encoded)
}

// Delete removes id and all its typed postings. It reports whether id
// existed.
func (index *TupleMultikeyIndex) Delete(id uint64) bool {
	if index == nil || index.index == nil {
		return false
	}
	return index.index.Delete(id)
}

// Lookup appends sorted matching item IDs to dst. A reusable dst avoids an
// allocation on the caller's hot path.
func (index *TupleMultikeyIndex) Lookup(key TupleFieldValue, dst []uint64) ([]uint64, error) {
	fixed, length, isFixed, err := encodeTupleMultikeyFixedKey(key)
	if err != nil {
		return dst[:0], err
	}
	if index == nil || index.index == nil {
		return dst[:0], nil
	}
	if isFixed {
		return index.index.lookupBytes(fixed[:length], dst), nil
	}
	encoded, err := encodeTupleMultikeyKey(key)
	if err != nil {
		return dst[:0], err
	}
	return index.index.Lookup(encoded, dst), nil
}

// Contains reports whether id is indexed under key.
func (index *TupleMultikeyIndex) Contains(key TupleFieldValue, id uint64) (bool, error) {
	fixed, length, isFixed, err := encodeTupleMultikeyFixedKey(key)
	if err != nil {
		return false, err
	}
	if index == nil || index.index == nil {
		return false, nil
	}
	if isFixed {
		return index.index.containsBytes(fixed[:length], id), nil
	}
	encoded, err := encodeTupleMultikeyKey(key)
	if err != nil {
		return false, err
	}
	return index.index.Contains(encoded, id), nil
}

// Len returns the number of items with at least one indexed key.
func (index *TupleMultikeyIndex) Len() int {
	if index == nil || index.index == nil {
		return 0
	}
	return index.index.Len()
}

// KeyCount returns the number of distinct typed values with at least one
// posting.
func (index *TupleMultikeyIndex) KeyCount() int {
	if index == nil || index.index == nil {
		return 0
	}
	return index.index.KeyCount()
}

const tupleMultikeyNullKey = "\x00"

func encodeTupleMultikeyKey(value TupleFieldValue) (string, error) {
	fixed, length, isFixed, err := encodeTupleMultikeyFixedKey(value)
	if err != nil {
		return "", err
	}
	if isFixed {
		return string(fixed[:length]), nil
	}

	switch value.Kind {
	case TupleFieldString:
		return encodeTupleMultikeyBytes(byte(value.Kind), []byte(value.String))
	case TupleFieldBytes:
		return encodeTupleMultikeyBytes(byte(value.Kind), value.Bytes)
	case TupleFieldInt64:
		return encodeTupleMultikeyUint64(byte(value.Kind), uint64(value.Int64)), nil
	case TupleFieldUint64:
		return encodeTupleMultikeyUint64(byte(value.Kind), value.Uint64), nil
	case TupleFieldFloat64:
		return encodeTupleMultikeyUint64(byte(value.Kind), math.Float64bits(value.Float64)), nil
	case TupleFieldBool:
		if value.Bool {
			return string([]byte{byte(value.Kind), 1}), nil
		}
		return string([]byte{byte(value.Kind), 0}), nil
	case TupleFieldDate:
		return encodeTupleMultikeyUint64(byte(value.Kind), uint64(tupleDateDays(value.Time))), nil
	case TupleFieldTimestamp:
		return encodeTupleMultikeyUint64(byte(value.Kind), uint64(value.Time.UTC().UnixNano())), nil
	default:
		return "", fmt.Errorf("%w: kind %d", errInvalidTupleMultikeyValue, value.Kind)
	}
}

func encodeTupleMultikeyFixedKey(value TupleFieldValue) (encoded [9]byte, length int, fixed bool, err error) {
	if !value.Valid {
		encoded[0] = 0
		return encoded, 1, true, nil
	}
	if !validTupleFieldType(value.Kind) {
		return encoded, 0, false, fmt.Errorf("%w: kind %d", errInvalidTupleMultikeyValue, value.Kind)
	}

	encoded[0] = byte(value.Kind)
	switch value.Kind {
	case TupleFieldString, TupleFieldBytes:
		return encoded, 0, false, nil
	case TupleFieldInt64:
		binary.BigEndian.PutUint64(encoded[1:], uint64(value.Int64))
		return encoded, len(encoded), true, nil
	case TupleFieldUint64:
		binary.BigEndian.PutUint64(encoded[1:], value.Uint64)
		return encoded, len(encoded), true, nil
	case TupleFieldFloat64:
		binary.BigEndian.PutUint64(encoded[1:], math.Float64bits(value.Float64))
		return encoded, len(encoded), true, nil
	case TupleFieldBool:
		if value.Bool {
			encoded[1] = 1
		}
		return encoded, 2, true, nil
	case TupleFieldDate:
		binary.BigEndian.PutUint64(encoded[1:], uint64(tupleDateDays(value.Time)))
		return encoded, len(encoded), true, nil
	case TupleFieldTimestamp:
		binary.BigEndian.PutUint64(encoded[1:], uint64(value.Time.UTC().UnixNano()))
		return encoded, len(encoded), true, nil
	default:
		return encoded, 0, false, fmt.Errorf("%w: kind %d", errInvalidTupleMultikeyValue, value.Kind)
	}
}

func encodeTupleMultikeyBytes(kind byte, value []byte) (string, error) {
	if uint64(len(value)) > math.MaxUint32 {
		return "", fmt.Errorf("%w: byte payload is too large", errInvalidTupleMultikeyValue)
	}
	var builder strings.Builder
	builder.Grow(1 + 4 + len(value))
	_ = builder.WriteByte(kind)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = builder.Write(length[:])
	_, _ = builder.Write(value)
	return builder.String(), nil
}

func encodeTupleMultikeyUint64(kind byte, value uint64) string {
	var encoded [9]byte
	encoded[0] = kind
	binary.BigEndian.PutUint64(encoded[1:], value)
	return string(encoded[:])
}
