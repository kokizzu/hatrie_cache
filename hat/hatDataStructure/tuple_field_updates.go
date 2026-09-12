package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	// ErrTupleFieldUpdateIndex indicates an invalid field index.
	ErrTupleFieldUpdateIndex = errors.New("hatDataStructure: tuple update field index out of range")
	// ErrTupleFieldUpdateDuplicate indicates more than one operation for a field
	// in one atomic batch.
	ErrTupleFieldUpdateDuplicate = errors.New("hatDataStructure: tuple update field appears more than once")
	// ErrTupleFieldUpdateKind indicates an unsupported operation kind.
	ErrTupleFieldUpdateKind = errors.New("hatDataStructure: unsupported tuple update kind")
	// ErrTupleFieldUpdateRange indicates an invalid splice interval.
	ErrTupleFieldUpdateRange = errors.New("hatDataStructure: tuple splice range is invalid")
	// ErrTupleFieldUpdateType indicates that a field cannot be used by the
	// requested typed operation.
	ErrTupleFieldUpdateType = errors.New("hatDataStructure: tuple field has an incompatible update type")
	// ErrTupleFieldUpdateOverflow indicates that an int64 update would overflow.
	ErrTupleFieldUpdateOverflow = errors.New("hatDataStructure: tuple int64 update overflows")
)

// TupleFieldUpdateKind identifies one positional tuple operation.
type TupleFieldUpdateKind uint8

const (
	// TupleFieldSet replaces the complete field with Value.
	TupleFieldSet TupleFieldUpdateKind = iota + 1
	// TupleFieldSplice replaces Remove bytes at Start with Insert.
	TupleFieldSplice
	// TupleFieldAddInt64 adds Delta to an eight-byte big-endian signed integer.
	TupleFieldAddInt64
)

// TupleFieldUpdate is one operation in an atomic TupleFieldOffsetCache batch.
// Only the fields used by Kind need to be populated.
type TupleFieldUpdate struct {
	Index  int
	Kind   TupleFieldUpdateKind
	Value  []byte
	Start  int
	Remove int
	Insert []byte
	Delta  int64
}

// ApplyUpdates returns a new packed tuple after applying one operation per
// field. Validation completes before allocation or output construction, so a
// rejected batch leaves the source cache unchanged.
//
// TupleFieldAddInt64 expects exactly eight bytes in big-endian signed-int64
// representation. Fixed-width batches reuse the source offset table and only
// allocate the replacement data buffer.
func (cache TupleFieldOffsetCache) ApplyUpdates(updates []TupleFieldUpdate) (TupleFieldOffsetCache, error) {
	if len(updates) == 0 {
		return cache, nil
	}
	fieldCount := cache.FieldCount()
	for updateIndex, update := range updates {
		if update.Index < 0 || update.Index >= fieldCount {
			return TupleFieldOffsetCache{}, fmt.Errorf("%w: got %d, field count %d", ErrTupleFieldUpdateIndex, update.Index, fieldCount)
		}
		for priorIndex := 0; priorIndex < updateIndex; priorIndex++ {
			if updates[priorIndex].Index == update.Index {
				return TupleFieldOffsetCache{}, fmt.Errorf("%w: field %d", ErrTupleFieldUpdateDuplicate, update.Index)
			}
		}
		field, err := cache.Field(update.Index)
		if err != nil {
			return TupleFieldOffsetCache{}, err
		}
		if _, err := tupleFieldUpdateLength(field, update); err != nil {
			return TupleFieldOffsetCache{}, fmt.Errorf("field %d: %w", update.Index, err)
		}
	}

	var total uint64
	sameLengths := true
	for index := 0; index < fieldCount; index++ {
		start, end, _ := cache.Offset(index)
		length := int(end - start)
		if update, ok := tupleFieldUpdateAt(updates, index); ok {
			updatedLength, err := tupleFieldUpdateLength(cache.data[start:end], update)
			if err != nil {
				return TupleFieldOffsetCache{}, fmt.Errorf("field %d: %w", index, err)
			}
			if updatedLength != length {
				sameLengths = false
			}
			length = updatedLength
		}
		total += uint64(length)
		if total < uint64(length) || total > maxTupleFieldOffsetUint32 || total > uint64(^uint(0)>>1) {
			return TupleFieldOffsetCache{}, fmt.Errorf("%w: tuple length %d", ErrTupleFieldUpdateRange, total)
		}
	}

	if sameLengths {
		data := make([]byte, len(cache.data))
		copy(data, cache.data)
		valid := tupleFieldValidityCopy(cache)
		for _, update := range updates {
			start, end, _ := cache.Offset(update.Index)
			switch update.Kind {
			case TupleFieldSet:
				copy(data[start:end], update.Value)
			case TupleFieldSplice:
				copy(data[int(start)+update.Start:], update.Insert)
			case TupleFieldAddInt64:
				value := int64(binary.BigEndian.Uint64(data[start:end]))
				binary.BigEndian.PutUint64(data[start:end], uint64(value+update.Delta))
			}
			if valid != nil {
				valid[update.Index] = true
			}
		}
		return TupleFieldOffsetCache{data: data, offsets: cache.offsets, valid: valid}, nil
	}

	data := make([]byte, int(total))
	offsets := cache.offsets
	if !sameLengths {
		offsets = make([]uint32, fieldCount+1)
	}
	valid := tupleFieldValidityCopy(cache)
	var offset uint64
	for index := 0; index < fieldCount; index++ {
		start, end, _ := cache.Offset(index)
		field := cache.data[start:end]
		if update, ok := tupleFieldUpdateAt(updates, index); ok {
			switch update.Kind {
			case TupleFieldSet:
				copy(data[offset:], update.Value)
				offset += uint64(len(update.Value))
			case TupleFieldSplice:
				copy(data[offset:], field[:update.Start])
				offset += uint64(update.Start)
				copy(data[offset:], update.Insert)
				offset += uint64(len(update.Insert))
				copy(data[offset:], field[update.Start+update.Remove:])
				offset += uint64(len(field) - update.Start - update.Remove)
			case TupleFieldAddInt64:
				value := int64(binary.BigEndian.Uint64(field))
				value += update.Delta
				binary.BigEndian.PutUint64(data[offset:], uint64(value))
				offset += 8
			}
		} else {
			copy(data[offset:], field)
			offset += uint64(len(field))
		}
		if valid != nil {
			if _, ok := tupleFieldUpdateAt(updates, index); ok {
				valid[index] = true
			}
		}
		if !sameLengths {
			offsets[index+1] = uint32(offset)
		}
	}
	return TupleFieldOffsetCache{data: data, offsets: offsets, valid: valid}, nil
}

func tupleFieldValidityCopy(cache TupleFieldOffsetCache) []bool {
	if cache.valid == nil {
		return nil
	}
	valid := make([]bool, cache.FieldCount())
	copy(valid, cache.valid)
	return valid
}

func tupleFieldUpdateAt(updates []TupleFieldUpdate, index int) (TupleFieldUpdate, bool) {
	for _, update := range updates {
		if update.Index == index {
			return update, true
		}
	}
	return TupleFieldUpdate{}, false
}

func tupleFieldUpdateLength(field []byte, update TupleFieldUpdate) (int, error) {
	switch update.Kind {
	case TupleFieldSet:
		return len(update.Value), nil
	case TupleFieldSplice:
		if update.Start < 0 || update.Remove < 0 || update.Start > len(field) || update.Remove > len(field)-update.Start {
			return 0, ErrTupleFieldUpdateRange
		}
		remaining := len(field) - update.Start - update.Remove
		if len(update.Insert) > int(^uint(0)>>1)-update.Start-remaining {
			return 0, ErrTupleFieldUpdateRange
		}
		return update.Start + len(update.Insert) + remaining, nil
	case TupleFieldAddInt64:
		if len(field) != 8 {
			return 0, ErrTupleFieldUpdateType
		}
		value := int64(binary.BigEndian.Uint64(field))
		const maxInt64 = int64(^uint64(0) >> 1)
		const minInt64 = -maxInt64 - 1
		if update.Delta > 0 && value > maxInt64-update.Delta {
			return 0, ErrTupleFieldUpdateOverflow
		}
		if update.Delta < 0 && value < minInt64-update.Delta {
			return 0, ErrTupleFieldUpdateOverflow
		}
		return 8, nil
	default:
		return 0, ErrTupleFieldUpdateKind
	}
}
