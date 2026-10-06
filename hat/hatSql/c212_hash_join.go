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

const sqlJoinDuplicateBucketBit = uint64(1) << 63

// sqlJoinBucket stores one row index inline. Only keys with duplicate rows
// receive an entry in duplicateRows, keeping the common unique-key case free
// of one-element backing slices.
type sqlJoinBucket uint64

func (bucket sqlJoinBucket) isDuplicate() bool {
	return uint64(bucket)&sqlJoinDuplicateBucketBit != 0
}

func (bucket sqlJoinBucket) row() int {
	return int(uint64(bucket))
}

func (bucket sqlJoinBucket) duplicateIndex() int {
	return int(uint64(bucket) &^ sqlJoinDuplicateBucketBit)
}

// sqlJoinHashIndex stores numeric payload bits directly, native strings
// without a type-prefix copy, and booleans in a two-slot table.
type sqlJoinHashIndex struct {
	capacity       int
	numbers        map[uint64]sqlJoinBucket
	strings        map[string]sqlJoinBucket
	booleans       [2]sqlJoinBucket
	booleanPresent [2]bool
	duplicateRows  [][]int
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
			index.numbers = make(map[uint64]sqlJoinBucket, index.capacity)
		}
		bucket, found := index.numbers[key.hash]
		if !found {
			index.numbers[key.hash] = sqlJoinBucket(uint64(row))
			return
		}
		index.numbers[key.hash] = index.growBucket(bucket, row)
	case sqlJoinProbeString:
		if index.strings == nil {
			index.strings = make(map[string]sqlJoinBucket, index.capacity)
		}
		bucket, found := index.strings[key.stringValue]
		if !found {
			index.strings[key.stringValue] = sqlJoinBucket(uint64(row))
			return
		}
		index.strings[key.stringValue] = index.growBucket(bucket, row)
	case sqlJoinProbeBoolean:
		if key.value < uint64(len(index.booleans)) {
			booleanIndex := int(key.value)
			if !index.booleanPresent[booleanIndex] {
				index.booleans[booleanIndex] = sqlJoinBucket(uint64(row))
				index.booleanPresent[booleanIndex] = true
				return
			}
			index.booleans[booleanIndex] = index.growBucket(index.booleans[booleanIndex], row)
		}
	}
}

func (index *sqlJoinHashIndex) growBucket(bucket sqlJoinBucket, row int) sqlJoinBucket {
	if !bucket.isDuplicate() {
		duplicateIndex := len(index.duplicateRows)
		index.duplicateRows = append(index.duplicateRows, []int{bucket.row(), row})
		return sqlJoinBucket(sqlJoinDuplicateBucketBit | uint64(duplicateIndex))
	}
	duplicateIndex := bucket.duplicateIndex()
	index.duplicateRows[duplicateIndex] = append(index.duplicateRows[duplicateIndex], row)
	return bucket
}

func (index *sqlJoinHashIndex) lookupBucket(key sqlJoinProbeKey) (sqlJoinBucket, bool) {
	switch key.kind {
	case sqlJoinProbeNumber:
		bucket, found := index.numbers[key.hash]
		return bucket, found
	case sqlJoinProbeString:
		bucket, found := index.strings[key.stringValue]
		return bucket, found
	case sqlJoinProbeBoolean:
		if key.value < uint64(len(index.booleans)) {
			booleanIndex := int(key.value)
			return index.booleans[booleanIndex], index.booleanPresent[booleanIndex]
		}
	}
	return sqlJoinBucket(0), false
}

func (index *sqlJoinHashIndex) duplicateRowsFor(bucket sqlJoinBucket) []int {
	if !bucket.isDuplicate() {
		return nil
	}
	return index.duplicateRows[bucket.duplicateIndex()]
}

func (index *sqlJoinHashIndex) lookupKey(key sqlJoinProbeKey) []int {
	bucket, found := index.lookupBucket(key)
	if !found {
		return nil
	}
	if rows := index.duplicateRowsFor(bucket); rows != nil {
		return rows
	}
	return []int{bucket.row()}
}
