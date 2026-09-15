package hatSql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
)

const (
	sqlAggregateStateVersion byte = 1
	sqlAggregateStateSeen    byte = 1
)

var sqlAggregateStateMagic = [...]byte{'H', 'A', 'S', 'T'}

type sqlAggregateStateKind byte

const (
	sqlAggregateStateCount sqlAggregateStateKind = iota + 1
	sqlAggregateStateSum
	sqlAggregateStateAverage
	sqlAggregateStateMinimum
	sqlAggregateStateMaximum
)

type sqlAggregateStateAccumulator struct {
	kind  sqlAggregateStateKind
	count int64
	sum   float64
	value float64
	seen  bool
}

func sqlAggregateStateSpec(name string) (sqlAggregateStateKind, bool, bool) {
	switch strings.ToUpper(name) {
	case "COUNT_STATE":
		return sqlAggregateStateCount, false, true
	case "SUM_STATE":
		return sqlAggregateStateSum, false, true
	case "AVG_STATE":
		return sqlAggregateStateAverage, false, true
	case "MIN_STATE":
		return sqlAggregateStateMinimum, false, true
	case "MAX_STATE":
		return sqlAggregateStateMaximum, false, true
	case "COUNT_MERGE":
		return sqlAggregateStateCount, true, true
	case "SUM_MERGE":
		return sqlAggregateStateSum, true, true
	case "AVG_MERGE":
		return sqlAggregateStateAverage, true, true
	case "MIN_MERGE":
		return sqlAggregateStateMinimum, true, true
	case "MAX_MERGE":
		return sqlAggregateStateMaximum, true, true
	default:
		return 0, false, false
	}
}

func (state *sqlAggregateStateAccumulator) addCount() error {
	if state.kind != sqlAggregateStateCount {
		return fmt.Errorf("%s aggregate state cannot count rows", sqlAggregateStateKindName(state.kind))
	}
	if state.count == math.MaxInt64 {
		return fmt.Errorf("SQL aggregate state count overflow")
	}
	state.count++
	state.seen = state.count != 0
	return nil
}

func (state *sqlAggregateStateAccumulator) addValue(value interface{}) error {
	if state.kind == sqlAggregateStateCount {
		if value == nil {
			return nil
		}
		return state.addCount()
	}
	number, ok := sqlNumber(value)
	if !ok {
		return nil
	}
	if !state.seen {
		state.count = 1
		state.sum = number
		state.value = number
		state.seen = true
		return nil
	}
	if state.count == math.MaxInt64 {
		return fmt.Errorf("SQL aggregate state count overflow")
	}
	state.count++
	switch state.kind {
	case sqlAggregateStateSum, sqlAggregateStateAverage:
		state.sum += number
	case sqlAggregateStateMinimum:
		if number < state.value {
			state.value = number
		}
	case sqlAggregateStateMaximum:
		if number > state.value {
			state.value = number
		}
	}
	return nil
}

func (state *sqlAggregateStateAccumulator) mergeValue(value interface{}) error {
	if value == nil {
		return nil
	}
	serialized, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("aggregate merge expects a serialized state, got %s", sqlLiteralTypeName(value))
	}
	other, err := decodeSQLAggregateState(serialized)
	if err != nil {
		return err
	}
	if other.kind != state.kind {
		return fmt.Errorf("aggregate merge state kind mismatch: expected %s, got %s", sqlAggregateStateKindName(state.kind), sqlAggregateStateKindName(other.kind))
	}
	if !other.seen {
		return nil
	}
	if other.count > math.MaxInt64-state.count {
		return fmt.Errorf("SQL aggregate state count overflow")
	}
	if !state.seen {
		*state = other
		return nil
	}
	state.count += other.count
	switch state.kind {
	case sqlAggregateStateCount:
	case sqlAggregateStateSum, sqlAggregateStateAverage:
		state.sum += other.sum
	case sqlAggregateStateMinimum:
		if other.value < state.value {
			state.value = other.value
		}
	case sqlAggregateStateMaximum:
		if other.value > state.value {
			state.value = other.value
		}
	}
	return nil
}

func sqlAggregateStateKindName(kind sqlAggregateStateKind) string {
	switch kind {
	case sqlAggregateStateCount:
		return "COUNT"
	case sqlAggregateStateSum:
		return "SUM"
	case sqlAggregateStateAverage:
		return "AVG"
	case sqlAggregateStateMinimum:
		return "MIN"
	case sqlAggregateStateMaximum:
		return "MAX"
	default:
		return "unknown"
	}
}

func (state sqlAggregateStateAccumulator) result(serialized bool) interface{} {
	if serialized {
		return encodeSQLAggregateState(state)
	}
	if state.kind == sqlAggregateStateCount {
		return state.count
	}
	if !state.seen {
		return nil
	}
	switch state.kind {
	case sqlAggregateStateSum:
		return state.sum
	case sqlAggregateStateAverage:
		return state.sum / float64(state.count)
	case sqlAggregateStateMinimum, sqlAggregateStateMaximum:
		return state.value
	default:
		return nil
	}
}

func encodeSQLAggregateState(state sqlAggregateStateAccumulator) []byte {
	flags := byte(0)
	if state.seen {
		flags |= sqlAggregateStateSeen
	}
	serialized := make([]byte, 0, 4+1+1+1+binary.MaxVarintLen64+8)
	serialized = append(serialized, sqlAggregateStateMagic[:]...)
	serialized = append(serialized, sqlAggregateStateVersion, byte(state.kind), flags)
	var count [binary.MaxVarintLen64]byte
	countLength := binary.PutUvarint(count[:], uint64(state.count))
	serialized = append(serialized, count[:countLength]...)
	if state.kind != sqlAggregateStateCount {
		number := state.value
		if state.kind == sqlAggregateStateSum || state.kind == sqlAggregateStateAverage {
			number = state.sum
		}
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(number))
		serialized = append(serialized, encoded[:]...)
	}
	return serialized
}

func decodeSQLAggregateState(serialized []byte) (sqlAggregateStateAccumulator, error) {
	if len(serialized) < len(sqlAggregateStateMagic)+3+1 {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: truncated header")
	}
	if serialized[0] != sqlAggregateStateMagic[0] || serialized[1] != sqlAggregateStateMagic[1] || serialized[2] != sqlAggregateStateMagic[2] || serialized[3] != sqlAggregateStateMagic[3] {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: invalid magic")
	}
	if serialized[4] != sqlAggregateStateVersion {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: unsupported version %d", serialized[4])
	}
	kind := sqlAggregateStateKind(serialized[5])
	if kind < sqlAggregateStateCount || kind > sqlAggregateStateMaximum {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: invalid kind %d", serialized[5])
	}
	flags := serialized[6]
	if flags&^sqlAggregateStateSeen != 0 {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: invalid flags %d", flags)
	}
	count, countLength := binary.Uvarint(serialized[7:])
	if countLength <= 0 || count > uint64(math.MaxInt64) {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: invalid count")
	}
	state := sqlAggregateStateAccumulator{kind: kind, count: int64(count), seen: flags&sqlAggregateStateSeen != 0}
	if !state.seen && state.count != 0 {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: unseen state has a nonzero count")
	}
	if state.seen && state.count == 0 {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: seen state has a zero count")
	}
	position := 7 + countLength
	if kind == sqlAggregateStateCount {
		if position != len(serialized) {
			return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: unexpected payload")
		}
		return state, nil
	}
	if len(serialized)-position != 8 {
		return sqlAggregateStateAccumulator{}, fmt.Errorf("malformed aggregate state: numeric payload has invalid length")
	}
	number := math.Float64frombits(binary.LittleEndian.Uint64(serialized[position:]))
	if kind == sqlAggregateStateSum || kind == sqlAggregateStateAverage {
		state.sum = number
	} else {
		state.value = number
	}
	return state, nil
}

func sqlAggregateStateArity(expr sqlExpr, merge bool, kind sqlAggregateStateKind) (*sqlExpr, error) {
	if merge {
		if len(expr.args) != 1 || expr.args[0].kind == "star" {
			return nil, fmt.Errorf("%s expects exactly one state argument", expr.name)
		}
		return &expr.args[0], nil
	}
	if kind == sqlAggregateStateCount {
		if len(expr.args) > 1 {
			return nil, fmt.Errorf("%s expects zero or one argument", expr.name)
		}
		if len(expr.args) == 0 || expr.args[0].kind == "star" {
			return nil, nil
		}
		return &expr.args[0], nil
	}
	if len(expr.args) != 1 || expr.args[0].kind == "star" {
		return nil, fmt.Errorf("%s expects exactly one argument", expr.name)
	}
	return &expr.args[0], nil
}

func evalSQLAggregateState(expr sqlExpr, group []sqlExecRow) (interface{}, error) {
	kind, merge, ok := sqlAggregateStateSpec(expr.name)
	if !ok {
		return nil, fmt.Errorf("unknown aggregate state function %q", expr.name)
	}
	argument, err := sqlAggregateStateArity(expr, merge, kind)
	if err != nil {
		return nil, err
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	state := sqlAggregateStateAccumulator{kind: kind}
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
		if argument == nil {
			if err := state.addCount(); err != nil {
				return nil, err
			}
			continue
		}
		value := evalSQLExpr(*argument, []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		if err := state.addValue(value); err != nil {
			return nil, err
		}
	}
	return state.result(!merge), nil
}
