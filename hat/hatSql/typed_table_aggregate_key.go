package hatSql

import (
	"encoding/binary"
	"hash/maphash"
	"math"
	"strconv"
	"strings"
)

var typedTableAggregateHashSeed = maphash.MakeSeed()

func typedTableAggregateGroupHash(values []TypedTableValue, groupBy []int) uint64 {
	var hash maphash.Hash
	hash.SetSeed(typedTableAggregateHashSeed)
	var encoded [8]byte
	for _, column := range groupBy {
		if column < 0 || column >= len(values) {
			_ = hash.WriteByte(0xff)
			continue
		}
		value := values[column]
		_ = hash.WriteByte(byte(value.Kind))
		if !value.Valid {
			_ = hash.WriteByte(0)
			continue
		}
		_ = hash.WriteByte(1)
		switch value.Kind {
		case TypedTableString:
			_, _ = hash.WriteString(value.String)
		case TypedTableInt64:
			binary.LittleEndian.PutUint64(encoded[:], uint64(value.Int64))
			_, _ = hash.Write(encoded[:])
		case TypedTableFloat64:
			binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value.Float64))
			_, _ = hash.Write(encoded[:])
		case TypedTableBool:
			if value.Bool {
				_ = hash.WriteByte(1)
			} else {
				_ = hash.WriteByte(0)
			}
		}
	}
	return hash.Sum64()
}

func typedTableAggregateGroupValuesEqual(groupValues, values []TypedTableValue, groupBy []int) bool {
	if len(groupValues) != len(groupBy) {
		return false
	}
	for index, column := range groupBy {
		if column < 0 || column >= len(values) || !typedTableAggregateValueEqual(groupValues[index], values[column]) {
			return false
		}
	}
	return true
}

func typedTableAggregateValueEqual(left, right TypedTableValue) bool {
	if left.Kind != right.Kind || left.Valid != right.Valid {
		return false
	}
	if !left.Valid {
		return true
	}
	switch left.Kind {
	case TypedTableString:
		return left.String == right.String
	case TypedTableInt64:
		return left.Int64 == right.Int64
	case TypedTableFloat64:
		return math.Float64bits(left.Float64) == math.Float64bits(right.Float64)
	case TypedTableBool:
		return left.Bool == right.Bool
	default:
		return true
	}
}

func typedTableAggregateLegacyGroupKey(values []TypedTableValue, groupBy []int) string {
	if len(groupBy) == 0 {
		return "all"
	}
	var builder strings.Builder
	for _, column := range groupBy {
		value := values[column]
		builder.WriteByte(byte(value.Kind))
		if !value.Valid {
			builder.WriteByte('n')
			continue
		}
		switch value.Kind {
		case TypedTableString:
			builder.WriteString(strconv.Itoa(len(value.String)))
			builder.WriteByte(':')
			builder.WriteString(value.String)
		case TypedTableInt64:
			builder.WriteString(strconv.FormatInt(value.Int64, 10))
		case TypedTableFloat64:
			builder.WriteString(strconv.FormatUint(math.Float64bits(value.Float64), 16))
		case TypedTableBool:
			if value.Bool {
				builder.WriteByte('1')
			} else {
				builder.WriteByte('0')
			}
		}
		builder.WriteByte('|')
	}
	return builder.String()
}
