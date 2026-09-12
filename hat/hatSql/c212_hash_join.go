package hatSql

import (
	"math"
)

type sqlJoinProbeKind uint8

const (
	sqlJoinProbeNumber sqlJoinProbeKind = iota + 1
	sqlJoinProbeString
	sqlJoinProbeBoolean
)

// sqlJoinProbeKey keeps the typed join value beside a precomputed numeric
// probe hash. Strings retain their original bytes so the runtime's optimized
// native string map can handle them without a canonical copy.
type sqlJoinProbeKey struct {
	hash        uint64
	value       uint64
	stringValue string
	kind        sqlJoinProbeKind
}

func newSQLJoinProbeKey(value interface{}) (sqlJoinProbeKey, bool) {
	if number, ok := sqlNumber(value); ok {
		bits := math.Float64bits(number)
		if math.IsNaN(number) {
			bits = 0x7ff8000000000000
		}
		return sqlJoinProbeKey{
			hash:  bits ^ 0x9e3779b97f4a7c15,
			value: bits,
			kind:  sqlJoinProbeNumber,
		}, true
	}
	switch value := value.(type) {
	case string:
		return sqlJoinProbeKey{
			stringValue: value,
			kind:        sqlJoinProbeString,
		}, true
	case bool:
		hash := uint64(0x13198a2e03707344)
		if value {
			hash++
		}
		var bit uint64
		if value {
			bit = 1
		}
		return sqlJoinProbeKey{hash: hash, value: bit, kind: sqlJoinProbeBoolean}, true
	default:
		return sqlJoinProbeKey{}, false
	}
}

func (key sqlJoinProbeKey) equal(other sqlJoinProbeKey) bool {
	if key.kind != other.kind {
		return false
	}
	switch key.kind {
	case sqlJoinProbeNumber:
		if math.IsNaN(math.Float64frombits(key.value)) {
			return math.IsNaN(math.Float64frombits(other.value))
		}
		return key.value == other.value
	case sqlJoinProbeString:
		return key.stringValue == other.stringValue
	case sqlJoinProbeBoolean:
		return key.value == other.value
	default:
		return false
	}
}

// sqlJoinHashIndex stores numeric payload bits directly, native strings
// without a type-prefix copy, and booleans in a two-slot table.
type sqlJoinHashIndex struct {
	capacity int
	numbers  map[uint64][]int
	strings  map[string][]int
	booleans [2][]int
}

func newSQLJoinHashIndex(capacity int) *sqlJoinHashIndex {
	if capacity < 0 {
		capacity = 0
	}
	return &sqlJoinHashIndex{capacity: capacity}
}

func (index *sqlJoinHashIndex) Add(value interface{}, row int) bool {
	key, ok := newSQLJoinProbeKey(value)
	if !ok {
		return false
	}
	index.addKey(key, row)
	return true
}

func (index *sqlJoinHashIndex) Lookup(value interface{}) []int {
	key, ok := newSQLJoinProbeKey(value)
	if !ok {
		return nil
	}
	return index.lookupKey(key)
}

func (index *sqlJoinHashIndex) addKey(key sqlJoinProbeKey, row int) {
	switch key.kind {
	case sqlJoinProbeNumber:
		if index.numbers == nil {
			index.numbers = make(map[uint64][]int, index.capacity)
		}
		index.numbers[key.hash] = append(index.numbers[key.hash], row)
	case sqlJoinProbeString:
		if index.strings == nil {
			index.strings = make(map[string][]int, index.capacity)
		}
		index.strings[key.stringValue] = append(index.strings[key.stringValue], row)
	case sqlJoinProbeBoolean:
		if key.value < uint64(len(index.booleans)) {
			index.booleans[key.value] = append(index.booleans[key.value], row)
		}
	}
}

func (index *sqlJoinHashIndex) lookupKey(key sqlJoinProbeKey) []int {
	switch key.kind {
	case sqlJoinProbeNumber:
		return index.numbers[key.hash]
	case sqlJoinProbeString:
		return index.strings[key.stringValue]
	case sqlJoinProbeBoolean:
		if key.value < uint64(len(index.booleans)) {
			return index.booleans[key.value]
		}
	}
	return nil
}
