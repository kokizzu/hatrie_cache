package hatSql

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

const (
	sqlPartialAggregateStateMagic               = "HAGS"
	sqlPartialAggregateStateVersion        byte = 1
	sqlPartialAggregateStateHeaderSize          = len(sqlPartialAggregateStateMagic) + 1
	sqlPartialAggregateStateMaxGroups           = 1 << 20
	sqlPartialAggregateStateMaxScalarBytes      = 64 << 20
)

const (
	sqlPartialAggregateHasSum byte = 1 << iota
	sqlPartialAggregateHasMin
	sqlPartialAggregateHasMax
)

const (
	sqlPartialAggregateNil byte = iota
	sqlPartialAggregateString
	sqlPartialAggregateBytes
	sqlPartialAggregateSigned
	sqlPartialAggregateUnsigned
	sqlPartialAggregateFloat
	sqlPartialAggregateFalse
	sqlPartialAggregateTrue
	sqlPartialAggregateTime
)

// SQLPartialAggregateGroup is one mergeable group produced by a partial
// aggregation worker. Value is the representative group value; Key must be
// the canonical grouping key used by the query collation.
type SQLPartialAggregateGroup struct {
	Key     string
	Value   interface{}
	Ordinal uint64
	Count   int64
	Sum     float64
	HasSum  bool
	Min     interface{}
	HasMin  bool
	Max     interface{}
	HasMax  bool
}

// SQLPartialAggregateState is a versioned, mergeable set of grouped aggregate
// states. It is suitable for transferring partial COUNT/SUM/AVG/MIN/MAX state
// between workers without transferring every input row.
type SQLPartialAggregateState struct {
	Groups []SQLPartialAggregateGroup
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (state SQLPartialAggregateState) MarshalBinary() ([]byte, error) {
	return EncodeSQLPartialAggregateState(state)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler. The receiver is
// unchanged when decoding fails.
func (state *SQLPartialAggregateState) UnmarshalBinary(encoded []byte) error {
	if state == nil {
		return fmt.Errorf("SQL partial aggregate destination is nil")
	}
	decoded, err := DecodeSQLPartialAggregateState(encoded)
	if err != nil {
		return err
	}
	*state = decoded
	return nil
}

// EncodeSQLPartialAggregateState encodes grouped aggregate state into a
// compact binary envelope. The format is self-versioned and supports nil,
// strings, bytes, booleans, signed and unsigned integers, floats, and times.
func EncodeSQLPartialAggregateState(state SQLPartialAggregateState) ([]byte, error) {
	if len(state.Groups) > sqlPartialAggregateStateMaxGroups {
		return nil, fmt.Errorf("SQL partial aggregate group limit exceeded: %d", len(state.Groups))
	}
	encoded := make([]byte, 0, sqlPartialAggregateStateHeaderSize+binary.MaxVarintLen64+len(state.Groups)*32)
	encoded = append(encoded, sqlPartialAggregateStateMagic...)
	encoded = append(encoded, sqlPartialAggregateStateVersion)
	encoded = sqlAppendPartialAggregateUvarint(encoded, uint64(len(state.Groups)))
	for index, group := range state.Groups {
		if err := validateSQLPartialAggregateGroup(group); err != nil {
			return nil, fmt.Errorf("group %d: %w", index, err)
		}
		encoded = sqlAppendPartialAggregateBytes(encoded, []byte(group.Key))
		encoded = sqlAppendPartialAggregateUvarint(encoded, group.Ordinal)
		encoded = sqlAppendPartialAggregateUvarint(encoded, sqlPartialAggregateEncodeInt64(group.Count))
		var flags byte
		if group.HasSum {
			flags |= sqlPartialAggregateHasSum
		}
		if group.HasMin {
			flags |= sqlPartialAggregateHasMin
		}
		if group.HasMax {
			flags |= sqlPartialAggregateHasMax
		}
		encoded = append(encoded, flags)
		if group.HasSum {
			var sum [8]byte
			binary.LittleEndian.PutUint64(sum[:], math.Float64bits(group.Sum))
			encoded = append(encoded, sum[:]...)
		}
		var err error
		encoded, err = sqlAppendPartialAggregateScalar(encoded, group.Value)
		if err != nil {
			return nil, fmt.Errorf("group %d value: %w", index, err)
		}
		if group.HasMin {
			encoded, err = sqlAppendPartialAggregateScalar(encoded, group.Min)
			if err != nil {
				return nil, fmt.Errorf("group %d minimum: %w", index, err)
			}
		}
		if group.HasMax {
			encoded, err = sqlAppendPartialAggregateScalar(encoded, group.Max)
			if err != nil {
				return nil, fmt.Errorf("group %d maximum: %w", index, err)
			}
		}
	}
	return encoded, nil
}

// DecodeSQLPartialAggregateState decodes a state envelope and rejects unknown
// versions, malformed lengths, unknown scalar tags, trailing bytes, and
// group counts large enough to cause an unreasonable allocation.
func DecodeSQLPartialAggregateState(encoded []byte) (SQLPartialAggregateState, error) {
	if len(encoded) < sqlPartialAggregateStateHeaderSize || string(encoded[:len(sqlPartialAggregateStateMagic)]) != sqlPartialAggregateStateMagic {
		return SQLPartialAggregateState{}, fmt.Errorf("invalid SQL partial aggregate state marker")
	}
	if encoded[len(sqlPartialAggregateStateMagic)] != sqlPartialAggregateStateVersion {
		return SQLPartialAggregateState{}, fmt.Errorf("unsupported SQL partial aggregate state version")
	}
	offset := sqlPartialAggregateStateHeaderSize
	groupCount, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
	if err != nil {
		return SQLPartialAggregateState{}, err
	}
	offset = next
	if groupCount > sqlPartialAggregateStateMaxGroups || groupCount > uint64(len(encoded)) {
		return SQLPartialAggregateState{}, fmt.Errorf("SQL partial aggregate group count out of bounds: %d", groupCount)
	}
	state := SQLPartialAggregateState{Groups: make([]SQLPartialAggregateGroup, 0, int(groupCount))}
	for index := uint64(0); index < groupCount; index++ {
		keyBytes, next, err := sqlReadPartialAggregateBytes(encoded, offset, "group key")
		if err != nil {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d: %w", index, err)
		}
		offset = next
		ordinal, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d ordinal: %w", index, err)
		}
		offset = next
		countValue, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d count: %w", index, err)
		}
		offset = next
		if offset >= len(encoded) {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d flags are truncated", index)
		}
		flags := encoded[offset]
		offset++
		if flags & ^(sqlPartialAggregateHasSum|sqlPartialAggregateHasMin|sqlPartialAggregateHasMax) != 0 {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d has unknown flags %#x", index, flags)
		}
		group := SQLPartialAggregateGroup{
			Key:     string(keyBytes),
			Ordinal: ordinal,
			Count:   sqlPartialAggregateDecodeInt64(countValue),
			HasSum:  flags&sqlPartialAggregateHasSum != 0,
			HasMin:  flags&sqlPartialAggregateHasMin != 0,
			HasMax:  flags&sqlPartialAggregateHasMax != 0,
		}
		if group.HasSum {
			if len(encoded)-offset < 8 {
				return SQLPartialAggregateState{}, fmt.Errorf("group %d sum is truncated", index)
			}
			group.Sum = math.Float64frombits(binary.LittleEndian.Uint64(encoded[offset : offset+8]))
			offset += 8
		}
		group.Value, offset, err = sqlReadPartialAggregateScalar(encoded, offset)
		if err != nil {
			return SQLPartialAggregateState{}, fmt.Errorf("group %d value: %w", index, err)
		}
		if group.HasMin {
			group.Min, offset, err = sqlReadPartialAggregateScalar(encoded, offset)
			if err != nil {
				return SQLPartialAggregateState{}, fmt.Errorf("group %d minimum: %w", index, err)
			}
		}
		if group.HasMax {
			group.Max, offset, err = sqlReadPartialAggregateScalar(encoded, offset)
			if err != nil {
				return SQLPartialAggregateState{}, fmt.Errorf("group %d maximum: %w", index, err)
			}
		}
		state.Groups = append(state.Groups, group)
	}
	if offset != len(encoded) {
		return SQLPartialAggregateState{}, fmt.Errorf("SQL partial aggregate state has %d trailing bytes", len(encoded)-offset)
	}
	return state, nil
}

// MergeSQLPartialAggregateState merges src into dst. It preserves the first
// input ordinal and representative value for each key, while combining the
// mergeable aggregate fields. The destination is unchanged if validation or
// count overflow fails.
func MergeSQLPartialAggregateState(dst *SQLPartialAggregateState, src SQLPartialAggregateState) error {
	if dst == nil {
		return fmt.Errorf("SQL partial aggregate destination is nil")
	}
	if len(dst.Groups)+len(src.Groups) > sqlPartialAggregateStateMaxGroups {
		return fmt.Errorf("SQL partial aggregate group limit exceeded")
	}
	for index, group := range dst.Groups {
		if err := validateSQLPartialAggregateGroup(group); err != nil {
			return fmt.Errorf("destination group %d: %w", index, err)
		}
	}
	for index, group := range src.Groups {
		if err := validateSQLPartialAggregateGroup(group); err != nil {
			return fmt.Errorf("source group %d: %w", index, err)
		}
	}
	merged := make([]SQLPartialAggregateGroup, len(dst.Groups), len(dst.Groups)+len(src.Groups))
	copy(merged, dst.Groups)
	indexes := make(map[string]int, len(merged)+len(src.Groups))
	for index, group := range merged {
		if _, exists := indexes[group.Key]; !exists {
			indexes[group.Key] = index
		}
	}
	for _, source := range src.Groups {
		index, exists := indexes[source.Key]
		if !exists {
			indexes[source.Key] = len(merged)
			merged = append(merged, source)
			continue
		}
		target := &merged[index]
		count, ok := sqlPartialAggregateAddInt64(target.Count, source.Count)
		if !ok {
			return fmt.Errorf("count overflow for group %q", source.Key)
		}
		target.Count = count
		if source.HasSum {
			if !target.HasSum {
				target.Sum = source.Sum
				target.HasSum = true
			} else {
				target.Sum += source.Sum
			}
		}
		if source.HasMin && (!target.HasMin || sqlCompare(source.Min, target.Min) < 0) {
			target.Min = source.Min
			target.HasMin = true
		}
		if source.HasMax && (!target.HasMax || sqlCompare(source.Max, target.Max) > 0) {
			target.Max = source.Max
			target.HasMax = true
		}
		if source.Ordinal < target.Ordinal {
			target.Ordinal = source.Ordinal
			target.Value = source.Value
		}
	}
	dst.Groups = merged
	return nil
}

func validateSQLPartialAggregateGroup(group SQLPartialAggregateGroup) error {
	if len(group.Key) > sqlPartialAggregateStateMaxScalarBytes {
		return fmt.Errorf("group key is too large: %d bytes", len(group.Key))
	}
	if err := validateSQLPartialAggregateScalar(group.Value); err != nil {
		return fmt.Errorf("unsupported group value: %w", err)
	}
	if group.HasMin {
		if err := validateSQLPartialAggregateScalar(group.Min); err != nil {
			return fmt.Errorf("unsupported minimum: %w", err)
		}
	}
	if group.HasMax {
		if err := validateSQLPartialAggregateScalar(group.Max); err != nil {
			return fmt.Errorf("unsupported maximum: %w", err)
		}
	}
	return nil
}

func validateSQLPartialAggregateScalar(value interface{}) error {
	switch value := value.(type) {
	case nil, string, []byte, bool, time.Time,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		if bytes, ok := value.([]byte); ok && len(bytes) > sqlPartialAggregateStateMaxScalarBytes {
			return fmt.Errorf("byte scalar is too large: %d bytes", len(bytes))
		}
		if text, ok := value.(string); ok && len(text) > sqlPartialAggregateStateMaxScalarBytes {
			return fmt.Errorf("string scalar is too large: %d bytes", len(text))
		}
		return nil
	default:
		return fmt.Errorf("unsupported scalar type %T", value)
	}
}

func sqlAppendPartialAggregateScalar(encoded []byte, value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return append(encoded, sqlPartialAggregateNil), nil
	case string:
		encoded = append(encoded, sqlPartialAggregateString)
		return sqlAppendPartialAggregateBytes(encoded, []byte(value)), nil
	case []byte:
		encoded = append(encoded, sqlPartialAggregateBytes)
		return sqlAppendPartialAggregateBytes(encoded, value), nil
	case bool:
		if value {
			return append(encoded, sqlPartialAggregateTrue), nil
		}
		return append(encoded, sqlPartialAggregateFalse), nil
	case time.Time:
		marshaled, err := value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, sqlPartialAggregateTime)
		return sqlAppendPartialAggregateBytes(encoded, marshaled), nil
	case int:
		return sqlAppendPartialAggregateSigned(encoded, int64(value)), nil
	case int8:
		return sqlAppendPartialAggregateSigned(encoded, int64(value)), nil
	case int16:
		return sqlAppendPartialAggregateSigned(encoded, int64(value)), nil
	case int32:
		return sqlAppendPartialAggregateSigned(encoded, int64(value)), nil
	case int64:
		return sqlAppendPartialAggregateSigned(encoded, value), nil
	case uint:
		return sqlAppendPartialAggregateUnsigned(encoded, uint64(value)), nil
	case uint8:
		return sqlAppendPartialAggregateUnsigned(encoded, uint64(value)), nil
	case uint16:
		return sqlAppendPartialAggregateUnsigned(encoded, uint64(value)), nil
	case uint32:
		return sqlAppendPartialAggregateUnsigned(encoded, uint64(value)), nil
	case uint64:
		return sqlAppendPartialAggregateUnsigned(encoded, value), nil
	case float32:
		return sqlAppendPartialAggregateFloat(encoded, float64(value)), nil
	case float64:
		return sqlAppendPartialAggregateFloat(encoded, value), nil
	default:
		return nil, fmt.Errorf("unsupported scalar type %T", value)
	}
}

func sqlAppendPartialAggregateSigned(encoded []byte, value int64) []byte {
	encoded = append(encoded, sqlPartialAggregateSigned)
	return sqlAppendPartialAggregateUvarint(encoded, sqlPartialAggregateEncodeInt64(value))
}

func sqlAppendPartialAggregateUnsigned(encoded []byte, value uint64) []byte {
	encoded = append(encoded, sqlPartialAggregateUnsigned)
	return sqlAppendPartialAggregateUvarint(encoded, value)
}

func sqlAppendPartialAggregateFloat(encoded []byte, value float64) []byte {
	var bits [8]byte
	binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
	encoded = append(encoded, sqlPartialAggregateFloat)
	return append(encoded, bits[:]...)
}

func sqlReadPartialAggregateScalar(encoded []byte, offset int) (interface{}, int, error) {
	if offset >= len(encoded) {
		return nil, offset, fmt.Errorf("scalar tag is truncated")
	}
	tag := encoded[offset]
	offset++
	switch tag {
	case sqlPartialAggregateNil:
		return nil, offset, nil
	case sqlPartialAggregateString:
		value, next, err := sqlReadPartialAggregateBytes(encoded, offset, "string scalar")
		return string(value), next, err
	case sqlPartialAggregateBytes:
		value, next, err := sqlReadPartialAggregateBytes(encoded, offset, "byte scalar")
		if err != nil {
			return nil, offset, err
		}
		return append([]byte(nil), value...), next, nil
	case sqlPartialAggregateSigned:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		return sqlPartialAggregateDecodeInt64(value), next, err
	case sqlPartialAggregateUnsigned:
		return sqlReadPartialAggregateUvarint(encoded, offset)
	case sqlPartialAggregateFloat:
		if len(encoded)-offset < 8 {
			return nil, offset, fmt.Errorf("float scalar is truncated")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(encoded[offset : offset+8])), offset + 8, nil
	case sqlPartialAggregateFalse:
		return false, offset, nil
	case sqlPartialAggregateTrue:
		return true, offset, nil
	case sqlPartialAggregateTime:
		value, next, err := sqlReadPartialAggregateBytes(encoded, offset, "time scalar")
		if err != nil {
			return nil, offset, err
		}
		var timestamp time.Time
		if err := timestamp.UnmarshalBinary(value); err != nil {
			return nil, offset, fmt.Errorf("invalid time scalar: %w", err)
		}
		return timestamp, next, nil
	default:
		return nil, offset, fmt.Errorf("unknown scalar tag %#x", tag)
	}
}

func sqlAppendPartialAggregateBytes(encoded, value []byte) []byte {
	encoded = sqlAppendPartialAggregateUvarint(encoded, uint64(len(value)))
	return append(encoded, value...)
}

func sqlReadPartialAggregateBytes(encoded []byte, offset int, label string) ([]byte, int, error) {
	length, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
	if err != nil {
		return nil, offset, fmt.Errorf("%s length: %w", label, err)
	}
	if length > sqlPartialAggregateStateMaxScalarBytes || length > uint64(len(encoded)-next) {
		return nil, offset, fmt.Errorf("%s length out of bounds: %d", label, length)
	}
	end := next + int(length)
	return encoded[next:end], end, nil
}

func sqlAppendPartialAggregateUvarint(encoded []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], value)
	return append(encoded, buffer[:n]...)
}

func sqlReadPartialAggregateUvarint(encoded []byte, offset int) (uint64, int, error) {
	if offset >= len(encoded) {
		return 0, offset, fmt.Errorf("varint is truncated")
	}
	value, size := binary.Uvarint(encoded[offset:])
	if size == 0 {
		return 0, offset, fmt.Errorf("varint is truncated")
	}
	if size < 0 {
		return 0, offset, fmt.Errorf("varint overflows")
	}
	return value, offset + size, nil
}

func sqlPartialAggregateEncodeInt64(value int64) uint64 {
	return uint64(value<<1) ^ uint64(value>>63)
}

func sqlPartialAggregateDecodeInt64(value uint64) int64 {
	return int64(value>>1) ^ -int64(value&1)
}

func sqlPartialAggregateAddInt64(left, right int64) (int64, bool) {
	if right > 0 && left > math.MaxInt64-right {
		return 0, false
	}
	if right < 0 && left < math.MinInt64-right {
		return 0, false
	}
	return left + right, true
}
