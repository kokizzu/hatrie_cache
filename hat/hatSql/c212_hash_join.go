package hatSql

import (
	"math"
)

const (
	sqlJoinRuntimeBloomMinCapacity = 1024
	sqlJoinRuntimeBloomSampleSize  = 64
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
	capacity    int
	numbers     map[uint64][]int
	strings     map[string][]int
	booleans    [2][]int
	bloom       *sqlJoinRuntimeBloom
	bloomState  uint8
	bloomProbes uint32
	bloomMisses uint32
}

// sqlJoinRuntimeBloom is a query-local negative lookup filter. It is
// deliberately adaptive: a join pays the exact existing map lookup while it
// samples its probe mix, then enables the filter only for miss-heavy probes.
// This keeps hit-heavy joins from carrying a permanent extra hash/bit-test.
type sqlJoinRuntimeBloom struct {
	words []uint64
	mask  uint64
}

const (
	sqlJoinRuntimeBloomSampling uint8 = iota
	sqlJoinRuntimeBloomActive
	sqlJoinRuntimeBloomDisabled
)

func newSQLJoinRuntimeBloom(capacity int) *sqlJoinRuntimeBloom {
	if capacity < sqlJoinRuntimeBloomMinCapacity {
		return nil
	}
	bits := uint64(capacity) * 8
	if bits < 64 {
		bits = 64
	}
	bitCount := uint64(64)
	for bitCount < bits {
		bitCount <<= 1
	}
	return &sqlJoinRuntimeBloom{
		words: make([]uint64, bitCount/64),
		mask:  bitCount - 1,
	}
}

func (filter *sqlJoinRuntimeBloom) add(hash uint64) {
	if filter == nil {
		return
	}
	first, second, third := sqlJoinRuntimeBloomPositions(hash, filter.mask)
	filter.words[first>>6] |= uint64(1) << (first & 63)
	filter.words[second>>6] |= uint64(1) << (second & 63)
	filter.words[third>>6] |= uint64(1) << (third & 63)
}

func (filter *sqlJoinRuntimeBloom) mayContain(hash uint64) bool {
	if filter == nil {
		return true
	}
	first, second, third := sqlJoinRuntimeBloomPositions(hash, filter.mask)
	return filter.words[first>>6]&(uint64(1)<<(first&63)) != 0 &&
		filter.words[second>>6]&(uint64(1)<<(second&63)) != 0 &&
		filter.words[third>>6]&(uint64(1)<<(third&63)) != 0
}

func sqlJoinRuntimeBloomPositions(hash, mask uint64) (uint64, uint64, uint64) {
	first := sqlJoinRuntimeBloomMix(hash)
	step := sqlJoinRuntimeBloomMix(hash^0x9e3779b97f4a7c15) | 1
	return first & mask, (first + step) & mask, (first + step*2) & mask
}

func sqlJoinRuntimeBloomMix(hash uint64) uint64 {
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	hash *= 0x94d049bb133111eb
	return hash ^ (hash >> 31)
}

func newSQLJoinHashIndex(capacity int) *sqlJoinHashIndex {
	if capacity < 0 {
		capacity = 0
	}
	index := &sqlJoinHashIndex{capacity: capacity}
	if capacity < sqlJoinRuntimeBloomMinCapacity {
		index.bloomState = sqlJoinRuntimeBloomDisabled
	}
	return index
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

func (index *sqlJoinHashIndex) activateRuntimeBloom() {
	filter := newSQLJoinRuntimeBloom(index.capacity)
	if filter == nil {
		index.bloomState = sqlJoinRuntimeBloomDisabled
		return
	}
	for hash := range index.numbers {
		filter.add(hash)
	}
	for value, rows := range index.booleans {
		if len(rows) == 0 {
			continue
		}
		key, _ := newSQLJoinProbeKey(value == 1)
		filter.add(key.hash)
	}
	index.bloom = filter
	index.bloomState = sqlJoinRuntimeBloomActive
}

func (index *sqlJoinHashIndex) lookupKey(key sqlJoinProbeKey) []int {
	if index.bloom != nil && key.kind != sqlJoinProbeString {
		if index.bloomState == sqlJoinRuntimeBloomActive && !index.bloom.mayContain(key.hash) {
			return nil
		}
	}
	var rows []int
	switch key.kind {
	case sqlJoinProbeNumber:
		rows = index.numbers[key.hash]
	case sqlJoinProbeString:
		rows = index.strings[key.stringValue]
	case sqlJoinProbeBoolean:
		if key.value < uint64(len(index.booleans)) {
			rows = index.booleans[key.value]
		}
	}
	if key.kind != sqlJoinProbeString && index.bloomState == sqlJoinRuntimeBloomSampling {
		index.bloomProbes++
		if len(rows) == 0 {
			index.bloomMisses++
		}
		if index.bloomProbes >= sqlJoinRuntimeBloomSampleSize {
			if index.bloomMisses*4 >= index.bloomProbes*3 {
				index.activateRuntimeBloom()
			} else {
				index.bloomState = sqlJoinRuntimeBloomDisabled
			}
		}
	}
	return rows
}
