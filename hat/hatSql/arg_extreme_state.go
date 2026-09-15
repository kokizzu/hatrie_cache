package hatSql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	sqlArgExtremeStateVersion  byte = 1
	sqlArgExtremeStateSeen     byte = 1
	sqlArgExtremeStateHeader        = 8
	sqlArgExtremeStateMaxBytes      = sqlPartialAggregateStateMaxScalarBytes*2 + sqlArgExtremeStateHeader + 32
)

type sqlArgExtremeStateKind byte

const (
	sqlArgExtremeStateMax sqlArgExtremeStateKind = 1
	sqlArgExtremeStateMin sqlArgExtremeStateKind = 2
)

const (
	sqlArgExtremeScalarNil byte = iota
	sqlArgExtremeScalarString
	sqlArgExtremeScalarBytes
	sqlArgExtremeScalarFalse
	sqlArgExtremeScalarTrue
	sqlArgExtremeScalarTime
	sqlArgExtremeScalarInt
	sqlArgExtremeScalarInt8
	sqlArgExtremeScalarInt16
	sqlArgExtremeScalarInt32
	sqlArgExtremeScalarInt64
	sqlArgExtremeScalarUint
	sqlArgExtremeScalarUint8
	sqlArgExtremeScalarUint16
	sqlArgExtremeScalarUint32
	sqlArgExtremeScalarUint64
	sqlArgExtremeScalarFloat32
	sqlArgExtremeScalarFloat64
	sqlArgExtremeScalarDate
	sqlArgExtremeScalarDecimal
	sqlArgExtremeScalarUUID
	sqlArgExtremeScalarDuration
	sqlArgExtremeScalarIPv4
	sqlArgExtremeScalarIPv6
)

func sqlArgExtremeStateSpec(name string) (sqlArgExtremeStateKind, bool, bool) {
	switch strings.ToUpper(name) {
	case "ARGMAX_STATE":
		return sqlArgExtremeStateMax, false, true
	case "ARGMIN_STATE":
		return sqlArgExtremeStateMin, false, true
	case "ARGMAX_MERGE":
		return sqlArgExtremeStateMax, true, true
	case "ARGMIN_MERGE":
		return sqlArgExtremeStateMin, true, true
	default:
		return 0, false, false
	}
}

func sqlArgExtremeStateKindName(kind sqlArgExtremeStateKind) string {
	switch kind {
	case sqlArgExtremeStateMax:
		return "ARGMAX"
	case sqlArgExtremeStateMin:
		return "ARGMIN"
	default:
		return "unknown arg-extreme"
	}
}

func sqlArgExtremeStateArity(expr sqlExpr, merge bool) (*sqlExpr, *sqlExpr, error) {
	if merge {
		if len(expr.args) != 1 || expr.args[0].kind == "star" {
			return nil, nil, fmt.Errorf("%s expects exactly one state argument", expr.name)
		}
		return &expr.args[0], nil, nil
	}
	if len(expr.args) != 2 || expr.args[0].kind == "star" || expr.args[1].kind == "star" {
		return nil, nil, fmt.Errorf("%s expects exactly two arguments", expr.name)
	}
	return &expr.args[0], &expr.args[1], nil
}

func validateSQLArgExtremeScalar(value interface{}) error {
	switch value := value.(type) {
	case nil, bool, time.Time,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, SQLIPv4, SQLIPv6:
		return nil
	case string:
		if len(value) > sqlPartialAggregateStateMaxScalarBytes {
			return fmt.Errorf("string scalar is too large: %d bytes", len(value))
		}
		return nil
	case []byte:
		if len(value) > sqlPartialAggregateStateMaxScalarBytes {
			return fmt.Errorf("byte scalar is too large: %d bytes", len(value))
		}
		return nil
	case sqlDate:
		return validateSQLArgExtremeText(string(value), "date")
	case sqlDecimal:
		return validateSQLArgExtremeText(string(value), "decimal")
	case sqlUUID:
		return validateSQLArgExtremeText(string(value), "UUID")
	case sqlDuration:
		return validateSQLArgExtremeText(string(value), "duration")
	default:
		return fmt.Errorf("unsupported arg-extreme scalar type %T", value)
	}
}

func validateSQLArgExtremeText(value, name string) error {
	if len(value) > sqlPartialAggregateStateMaxScalarBytes {
		return fmt.Errorf("%s scalar is too large: %d bytes", name, len(value))
	}
	return nil
}

func sqlAppendArgExtremeBytes(encoded []byte, tag byte, value []byte) []byte {
	encoded = append(encoded, tag)
	return sqlAppendPartialAggregateBytes(encoded, value)
}

func sqlAppendArgExtremeSigned(encoded []byte, tag byte, value int64) []byte {
	encoded = append(encoded, tag)
	return sqlAppendPartialAggregateUvarint(encoded, sqlPartialAggregateEncodeInt64(value))
}

func sqlAppendArgExtremeUnsigned(encoded []byte, tag byte, value uint64) []byte {
	encoded = append(encoded, tag)
	return sqlAppendPartialAggregateUvarint(encoded, value)
}

func sqlAppendArgExtremeScalar(encoded []byte, value interface{}) ([]byte, error) {
	if err := validateSQLArgExtremeScalar(value); err != nil {
		return nil, err
	}
	switch value := value.(type) {
	case nil:
		return append(encoded, sqlArgExtremeScalarNil), nil
	case string:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarString, []byte(value)), nil
	case []byte:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarBytes, value), nil
	case bool:
		if value {
			return append(encoded, sqlArgExtremeScalarTrue), nil
		}
		return append(encoded, sqlArgExtremeScalarFalse), nil
	case time.Time:
		marshaled, err := value.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarTime, marshaled), nil
	case int:
		return sqlAppendArgExtremeSigned(encoded, sqlArgExtremeScalarInt, int64(value)), nil
	case int8:
		return sqlAppendArgExtremeSigned(encoded, sqlArgExtremeScalarInt8, int64(value)), nil
	case int16:
		return sqlAppendArgExtremeSigned(encoded, sqlArgExtremeScalarInt16, int64(value)), nil
	case int32:
		return sqlAppendArgExtremeSigned(encoded, sqlArgExtremeScalarInt32, int64(value)), nil
	case int64:
		return sqlAppendArgExtremeSigned(encoded, sqlArgExtremeScalarInt64, value), nil
	case uint:
		return sqlAppendArgExtremeUnsigned(encoded, sqlArgExtremeScalarUint, uint64(value)), nil
	case uint8:
		return sqlAppendArgExtremeUnsigned(encoded, sqlArgExtremeScalarUint8, uint64(value)), nil
	case uint16:
		return sqlAppendArgExtremeUnsigned(encoded, sqlArgExtremeScalarUint16, uint64(value)), nil
	case uint32:
		return sqlAppendArgExtremeUnsigned(encoded, sqlArgExtremeScalarUint32, uint64(value)), nil
	case uint64:
		return sqlAppendArgExtremeUnsigned(encoded, sqlArgExtremeScalarUint64, value), nil
	case float32:
		var bits [4]byte
		binary.LittleEndian.PutUint32(bits[:], math.Float32bits(value))
		return append(append(encoded, sqlArgExtremeScalarFloat32), bits[:]...), nil
	case float64:
		var bits [8]byte
		binary.LittleEndian.PutUint64(bits[:], math.Float64bits(value))
		return append(append(encoded, sqlArgExtremeScalarFloat64), bits[:]...), nil
	case sqlDate:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarDate, []byte(value)), nil
	case sqlDecimal:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarDecimal, []byte(value)), nil
	case sqlUUID:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarUUID, []byte(value)), nil
	case sqlDuration:
		return sqlAppendArgExtremeBytes(encoded, sqlArgExtremeScalarDuration, []byte(value)), nil
	case SQLIPv4:
		var bytes [4]byte
		binary.BigEndian.PutUint32(bytes[:], uint32(value))
		return append(append(encoded, sqlArgExtremeScalarIPv4), bytes[:]...), nil
	case SQLIPv6:
		return append(append(encoded, sqlArgExtremeScalarIPv6), value[:]...), nil
	default:
		return nil, fmt.Errorf("unsupported arg-extreme scalar type %T", value)
	}
}

func sqlReadArgExtremeBytes(encoded []byte, offset int, name string) ([]byte, int, error) {
	value, next, err := sqlReadPartialAggregateBytes(encoded, offset, name)
	if err != nil {
		return nil, offset, err
	}
	return value, next, nil
}

func sqlReadArgExtremeSigned(encoded []byte, offset int, tag byte, name string) (int64, int, error) {
	value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
	if err != nil {
		return 0, offset, fmt.Errorf("%s: %w", name, err)
	}
	if tag == sqlArgExtremeScalarInt && strconv.IntSize == 32 {
		decoded := sqlPartialAggregateDecodeInt64(value)
		if decoded < -1<<31 || decoded > 1<<31-1 {
			return 0, offset, fmt.Errorf("%s overflows int32", name)
		}
	}
	return sqlPartialAggregateDecodeInt64(value), next, nil
}

func sqlReadArgExtremeScalar(encoded []byte, offset int) (interface{}, int, error) {
	if offset >= len(encoded) {
		return nil, offset, fmt.Errorf("arg-extreme scalar tag is truncated")
	}
	tag := encoded[offset]
	offset++
	switch tag {
	case sqlArgExtremeScalarNil:
		return nil, offset, nil
	case sqlArgExtremeScalarString:
		value, next, err := sqlReadArgExtremeBytes(encoded, offset, "string scalar")
		return string(value), next, err
	case sqlArgExtremeScalarBytes:
		value, next, err := sqlReadArgExtremeBytes(encoded, offset, "byte scalar")
		if err != nil {
			return nil, offset, err
		}
		return append([]byte(nil), value...), next, nil
	case sqlArgExtremeScalarFalse:
		return false, offset, nil
	case sqlArgExtremeScalarTrue:
		return true, offset, nil
	case sqlArgExtremeScalarTime:
		value, next, err := sqlReadArgExtremeBytes(encoded, offset, "time scalar")
		if err != nil {
			return nil, offset, err
		}
		var result time.Time
		if err := result.UnmarshalBinary(value); err != nil {
			return nil, offset, fmt.Errorf("decode time scalar: %w", err)
		}
		return result, next, nil
	case sqlArgExtremeScalarInt:
		value, next, err := sqlReadArgExtremeSigned(encoded, offset, tag, "int scalar")
		return int(value), next, err
	case sqlArgExtremeScalarInt8:
		value, next, err := sqlReadArgExtremeSigned(encoded, offset, tag, "int8 scalar")
		if err != nil || value < -128 || value > 127 {
			if err != nil {
				return nil, offset, err
			}
			return nil, offset, fmt.Errorf("int8 scalar overflows int8")
		}
		return int8(value), next, nil
	case sqlArgExtremeScalarInt16:
		value, next, err := sqlReadArgExtremeSigned(encoded, offset, tag, "int16 scalar")
		if err != nil || value < -32768 || value > 32767 {
			if err != nil {
				return nil, offset, err
			}
			return nil, offset, fmt.Errorf("int16 scalar overflows int16")
		}
		return int16(value), next, nil
	case sqlArgExtremeScalarInt32:
		value, next, err := sqlReadArgExtremeSigned(encoded, offset, tag, "int32 scalar")
		if err != nil || value < -1<<31 || value > 1<<31-1 {
			if err != nil {
				return nil, offset, err
			}
			return nil, offset, fmt.Errorf("int32 scalar overflows int32")
		}
		return int32(value), next, nil
	case sqlArgExtremeScalarInt64:
		value, next, err := sqlReadArgExtremeSigned(encoded, offset, tag, "int64 scalar")
		return value, next, err
	case sqlArgExtremeScalarUint:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("uint scalar: %w", err)
		}
		if strconv.IntSize == 32 && value > 1<<32-1 {
			return nil, offset, fmt.Errorf("uint scalar overflows uint32")
		}
		return uint(value), next, nil
	case sqlArgExtremeScalarUint8:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil || value > 1<<8-1 {
			if err != nil {
				return nil, offset, fmt.Errorf("uint8 scalar: %w", err)
			}
			return nil, offset, fmt.Errorf("uint8 scalar overflows uint8")
		}
		return uint8(value), next, nil
	case sqlArgExtremeScalarUint16:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil || value > 1<<16-1 {
			if err != nil {
				return nil, offset, fmt.Errorf("uint16 scalar: %w", err)
			}
			return nil, offset, fmt.Errorf("uint16 scalar overflows uint16")
		}
		return uint16(value), next, nil
	case sqlArgExtremeScalarUint32:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil || value > 1<<32-1 {
			if err != nil {
				return nil, offset, fmt.Errorf("uint32 scalar: %w", err)
			}
			return nil, offset, fmt.Errorf("uint32 scalar overflows uint32")
		}
		return uint32(value), next, nil
	case sqlArgExtremeScalarUint64:
		value, next, err := sqlReadPartialAggregateUvarint(encoded, offset)
		if err != nil {
			return nil, offset, fmt.Errorf("uint64 scalar: %w", err)
		}
		return value, next, nil
	case sqlArgExtremeScalarFloat32:
		if len(encoded)-offset < 4 {
			return nil, offset, fmt.Errorf("float32 scalar is truncated")
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(encoded[offset:])), offset + 4, nil
	case sqlArgExtremeScalarFloat64:
		if len(encoded)-offset < 8 {
			return nil, offset, fmt.Errorf("float64 scalar is truncated")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(encoded[offset:])), offset + 8, nil
	case sqlArgExtremeScalarDate, sqlArgExtremeScalarDecimal, sqlArgExtremeScalarUUID, sqlArgExtremeScalarDuration:
		value, next, err := sqlReadArgExtremeBytes(encoded, offset, "typed string scalar")
		if err != nil {
			return nil, offset, err
		}
		switch tag {
		case sqlArgExtremeScalarDate:
			return sqlDate(string(value)), next, nil
		case sqlArgExtremeScalarDecimal:
			return sqlDecimal(string(value)), next, nil
		case sqlArgExtremeScalarUUID:
			return sqlUUID(string(value)), next, nil
		default:
			return sqlDuration(string(value)), next, nil
		}
	case sqlArgExtremeScalarIPv4:
		if len(encoded)-offset < 4 {
			return nil, offset, fmt.Errorf("IPv4 scalar is truncated")
		}
		return SQLIPv4(binary.BigEndian.Uint32(encoded[offset:])), offset + 4, nil
	case sqlArgExtremeScalarIPv6:
		if len(encoded)-offset < 16 {
			return nil, offset, fmt.Errorf("IPv6 scalar is truncated")
		}
		var value SQLIPv6
		copy(value[:], encoded[offset:offset+16])
		return value, offset + 16, nil
	default:
		return nil, offset, fmt.Errorf("unsupported arg-extreme scalar tag %d", tag)
	}
}

type sqlDecodedArgExtremeState struct {
	kind      sqlArgExtremeStateKind
	collation SQLCollation
	seen      bool
	selected  interface{}
	extreme   interface{}
}

func sqlEncodeArgExtremeState(kind sqlArgExtremeStateKind, collation SQLCollation, selected, extreme interface{}, seen bool) ([]byte, error) {
	if kind != sqlArgExtremeStateMax && kind != sqlArgExtremeStateMin {
		return nil, fmt.Errorf("unsupported arg-extreme state kind %d", kind)
	}
	collation = collation.normalized()
	if !collation.valid() {
		return nil, fmt.Errorf("unsupported SQL collation %q", collation)
	}
	encoded := make([]byte, 0, 40)
	encoded = append(encoded, 'H', 'A', 'E', 'X', sqlArgExtremeStateVersion, byte(kind), sqlArgExtremeStateCollationTag(collation), 0)
	if !seen {
		return encoded, nil
	}
	if selected == nil || extreme == nil {
		return nil, fmt.Errorf("arg-extreme state cannot encode a NULL winner")
	}
	encoded[7] = sqlArgExtremeStateSeen
	var err error
	encoded, err = sqlAppendArgExtremeScalar(encoded, selected)
	if err != nil {
		return nil, fmt.Errorf("encode selected arg-extreme value: %w", err)
	}
	encoded, err = sqlAppendArgExtremeScalar(encoded, extreme)
	if err != nil {
		return nil, fmt.Errorf("encode arg-extreme ordering value: %w", err)
	}
	return encoded, nil
}

func sqlArgExtremeStateCollationTag(collation SQLCollation) byte {
	if collation.normalized() == SQLCollationUnicodeCI {
		return 1
	}
	return 0
}

func sqlArgExtremeStateCollation(tag byte) (SQLCollation, error) {
	switch tag {
	case 0:
		return SQLCollationBinary, nil
	case 1:
		return SQLCollationUnicodeCI, nil
	default:
		return "", fmt.Errorf("unsupported arg-extreme collation tag %d", tag)
	}
}

func sqlDecodeArgExtremeState(encoded []byte) (sqlDecodedArgExtremeState, error) {
	if len(encoded) > sqlArgExtremeStateMaxBytes {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("arg-extreme state is too large: %d bytes", len(encoded))
	}
	if len(encoded) < sqlArgExtremeStateHeader {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("arg-extreme state header is truncated")
	}
	if encoded[0] != 'H' || encoded[1] != 'A' || encoded[2] != 'E' || encoded[3] != 'X' {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("invalid arg-extreme state magic")
	}
	if encoded[4] != sqlArgExtremeStateVersion {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("unsupported arg-extreme state version %d", encoded[4])
	}
	kind := sqlArgExtremeStateKind(encoded[5])
	if kind != sqlArgExtremeStateMax && kind != sqlArgExtremeStateMin {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("unsupported arg-extreme state kind %d", kind)
	}
	collation, err := sqlArgExtremeStateCollation(encoded[6])
	if err != nil {
		return sqlDecodedArgExtremeState{}, err
	}
	flags := encoded[7]
	if flags&^sqlArgExtremeStateSeen != 0 {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("invalid arg-extreme state flags 0x%x", flags)
	}
	if flags == 0 {
		if len(encoded) != sqlArgExtremeStateHeader {
			return sqlDecodedArgExtremeState{}, fmt.Errorf("empty arg-extreme state has trailing data")
		}
		return sqlDecodedArgExtremeState{kind: kind, collation: collation}, nil
	}
	selected, offset, err := sqlReadArgExtremeScalar(encoded, sqlArgExtremeStateHeader)
	if err != nil {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("decode selected arg-extreme value: %w", err)
	}
	extreme, offset, err := sqlReadArgExtremeScalar(encoded, offset)
	if err != nil {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("decode arg-extreme ordering value: %w", err)
	}
	if selected == nil || extreme == nil {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("arg-extreme state contains a NULL winner")
	}
	if offset != len(encoded) {
		return sqlDecodedArgExtremeState{}, fmt.Errorf("arg-extreme state has %d trailing bytes", len(encoded)-offset)
	}
	return sqlDecodedArgExtremeState{kind: kind, collation: collation, seen: true, selected: selected, extreme: extreme}, nil
}

type sqlArgExtremeStateAccumulator struct {
	kind         sqlArgExtremeStateKind
	collation    SQLCollation
	collationSet bool
	selected     interface{}
	extreme      interface{}
	seen         bool
}

func (state *sqlArgExtremeStateAccumulator) add(selected, extreme interface{}) error {
	if selected == nil || extreme == nil {
		return nil
	}
	if err := validateSQLArgExtremeScalar(selected); err != nil {
		return err
	}
	if err := validateSQLArgExtremeScalar(extreme); err != nil {
		return err
	}
	if !state.collationSet {
		state.collation = SQLCollationBinary
		state.collationSet = true
	}
	if !state.seen {
		state.selected, state.extreme, state.seen = selected, extreme, true
		return nil
	}
	comparison := sqlCompareWithCollation(state.collation, extreme, state.extreme)
	if state.kind == sqlArgExtremeStateMin && comparison < 0 || state.kind == sqlArgExtremeStateMax && comparison > 0 {
		state.selected, state.extreme = selected, extreme
	}
	return nil
}

func (state *sqlArgExtremeStateAccumulator) mergeValue(value interface{}) error {
	if value == nil {
		return nil
	}
	encoded, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("%s_MERGE expects []byte state, got %T", sqlArgExtremeStateKindName(state.kind), value)
	}
	decoded, err := sqlDecodeArgExtremeState(encoded)
	if err != nil {
		return err
	}
	if decoded.kind != state.kind {
		return fmt.Errorf("%s_MERGE received %s state", sqlArgExtremeStateKindName(state.kind), sqlArgExtremeStateKindName(decoded.kind))
	}
	if !decoded.seen {
		return nil
	}
	if !state.collationSet {
		state.collation, state.collationSet = decoded.collation, true
	} else if state.collation.normalized() != decoded.collation.normalized() {
		return fmt.Errorf("%s_MERGE received mixed collations %q and %q", sqlArgExtremeStateKindName(state.kind), state.collation, decoded.collation)
	}
	return state.add(decoded.selected, decoded.extreme)
}

func (state *sqlArgExtremeStateAccumulator) output(serialized bool) (interface{}, error) {
	if !serialized {
		if state.seen {
			return state.selected, nil
		}
		return nil, nil
	}
	return sqlEncodeArgExtremeState(state.kind, state.collation, state.selected, state.extreme, state.seen)
}

func (state *sqlArgExtremeStateAccumulator) result(serialized bool) interface{} {
	value, _ := state.output(serialized)
	return value
}

func evalSQLArgExtremeState(expr sqlExpr, group []sqlExecRow) (interface{}, error) {
	kind, merge, ok := sqlArgExtremeStateSpec(expr.name)
	if !ok {
		return nil, fmt.Errorf("unknown arg-extreme state function %q", expr.name)
	}
	argument, ordering, err := sqlArgExtremeStateArity(expr, merge)
	if err != nil {
		return nil, err
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	state := sqlArgExtremeStateAccumulator{kind: kind}
	if !merge {
		state.collation, state.collationSet = expr.collation.normalized(), true
	}
	for _, row := range rows {
		if merge {
			value := evalSQLExpr(*argument, []sqlExecRow{row}, row)
			if err := sqlExpressionError(value); err != nil {
				return nil, err
			}
			if err := state.mergeValue(value); err != nil {
				return nil, err
			}
			continue
		}
		selected := evalSQLExpr(*argument, []sqlExecRow{row}, row)
		if err := sqlExpressionError(selected); err != nil {
			return nil, err
		}
		extreme := evalSQLExpr(*ordering, []sqlExecRow{row}, row)
		if err := sqlExpressionError(extreme); err != nil {
			return nil, err
		}
		if err := state.add(selected, extreme); err != nil {
			return nil, err
		}
	}
	return state.output(!merge)
}
