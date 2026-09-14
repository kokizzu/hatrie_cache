package hatDataStructure

import "math/bits"

// BitmapIndexInfo describes the shape of a typed bitmap index. IndexedRows is
// the number of value-to-row memberships, so a row indexed under multiple
// values is counted once per value.
type BitmapIndexInfo struct {
	DistinctValues uint64
	IndexedRows    uint64
	EncodedBytes   uint64
}

// BitmapIndex maps comparable typed values to exact row IDs. Each value owns a
// RoaringBitmap, so low-cardinality columns use compact sparse or dense
// containers instead of one pointer-bearing entry per row.
//
// BitmapIndex is derived state: callers should rebuild it from the authoritative
// table after restore. A zero value is ready for use. It is not safe for
// concurrent mutation or concurrent mutation and reads without external
// synchronization.
type BitmapIndex[K comparable] struct {
	buckets     map[K]RoaringBitmap
	indexedRows uint64
}

// NewBitmapIndex creates an empty typed bitmap index.
func NewBitmapIndex[K comparable]() *BitmapIndex[K] {
	return &BitmapIndex[K]{buckets: make(map[K]RoaringBitmap)}
}

// Add associates key with row and reports whether the membership was new.
func (index *BitmapIndex[K]) Add(key K, row uint32) bool {
	if index == nil {
		return false
	}
	if index.buckets == nil {
		index.buckets = make(map[K]RoaringBitmap)
	}
	bitmap := index.buckets[key]
	if bitmap.Add(row) == 0 {
		return false
	}
	index.buckets[key] = bitmap
	index.indexedRows++
	return true
}

// Remove removes the key-to-row membership and reports whether it existed.
func (index *BitmapIndex[K]) Remove(key K, row uint32) bool {
	if index == nil || index.buckets == nil {
		return false
	}
	bitmap, ok := index.buckets[key]
	if !ok || bitmap.Remove(row) == 0 {
		return false
	}
	index.indexedRows--
	if bitmap.Count() == 0 {
		delete(index.buckets, key)
	} else {
		index.buckets[key] = bitmap
	}
	return true
}

// Contains reports whether row is associated with key.
func (index *BitmapIndex[K]) Contains(key K, row uint32) bool {
	if index == nil || index.buckets == nil {
		return false
	}
	bitmap, ok := index.buckets[key]
	return ok && bitmap.Contains(row)
}

// Rows returns sorted row IDs for key. The returned slice is newly allocated
// and can be retained or modified by the caller.
func (index *BitmapIndex[K]) Rows(key K) []uint32 {
	if index == nil || index.buckets == nil {
		return nil
	}
	bitmap, ok := index.buckets[key]
	if !ok || bitmap.Count() == 0 {
		return nil
	}
	return bitmap.Values()
}

// Visit calls visit for each sorted row ID associated with key. It returns
// false when the callback stops iteration or is nil, and true after a complete
// visit, including a missing key.
func (index *BitmapIndex[K]) Visit(key K, visit func(uint32) bool) bool {
	if visit == nil {
		return false
	}
	if index == nil || index.buckets == nil {
		return true
	}
	bitmap, ok := index.buckets[key]
	if !ok {
		return true
	}
	return visitRoaringBitmapRows(bitmap, visit)
}

// Intersect returns rows that are associated with every supplied key. It
// starts with the smallest bitmap and tests membership in the remaining ones.
// The result owns its bitmap storage and can be modified independently.
func (index *BitmapIndex[K]) Intersect(keys ...K) RoaringBitmap {
	result := NewRoaringBitmap()
	if index == nil || index.buckets == nil || len(keys) == 0 {
		return result
	}
	smallest, ok := index.buckets[keys[0]]
	if !ok {
		return result
	}
	for _, key := range keys[1:] {
		candidate, found := index.buckets[key]
		if !found {
			return result
		}
		if candidate.Count() < smallest.Count() {
			smallest = candidate
		}
	}
	visitRoaringBitmapRows(smallest, func(row uint32) bool {
		for _, key := range keys {
			bitmap := index.buckets[key]
			if !bitmap.Contains(row) {
				return true
			}
		}
		result.Add(row)
		return true
	})
	return result
}

// Union returns rows associated with at least one supplied key. The result
// owns its bitmap storage and can be modified independently.
func (index *BitmapIndex[K]) Union(keys ...K) RoaringBitmap {
	result := NewRoaringBitmap()
	if index == nil || index.buckets == nil {
		return result
	}
	for _, key := range keys {
		bitmap, ok := index.buckets[key]
		if !ok {
			continue
		}
		visitRoaringBitmapRows(bitmap, func(row uint32) bool {
			result.Add(row)
			return true
		})
	}
	return result
}

// ValueCount returns the number of values with at least one indexed row.
func (index *BitmapIndex[K]) ValueCount() int {
	if index == nil {
		return 0
	}
	return len(index.buckets)
}

// IndexedRows returns the number of value-to-row memberships.
func (index *BitmapIndex[K]) IndexedRows() uint64 {
	if index == nil {
		return 0
	}
	return index.indexedRows
}

// Info reports the index's distinct-value, membership, and encoded bitmap
// footprint totals.
func (index *BitmapIndex[K]) Info() BitmapIndexInfo {
	if index == nil {
		return BitmapIndexInfo{}
	}
	info := BitmapIndexInfo{
		DistinctValues: uint64(len(index.buckets)),
		IndexedRows:    index.indexedRows,
	}
	for _, bitmap := range index.buckets {
		info.EncodedBytes += uint64(bitmap.EncodedSize())
	}
	return info
}

func visitRoaringBitmapRows(bitmap RoaringBitmap, visit func(uint32) bool) bool {
	completed := true
	bitmap.VisitContainers(func(key uint16, _ uint32, values []uint16, bitset []uint64) bool {
		for _, value := range values {
			if !visit(uint32(key)<<16 | uint32(value)) {
				completed = false
				return false
			}
		}
		for wordIndex, word := range bitset {
			for word != 0 {
				value := wordIndex*64 + bits.TrailingZeros64(word)
				if !visit(uint32(key)<<16 | uint32(value)) {
					completed = false
					return false
				}
				word &= word - 1
			}
		}
		return true
	})
	return completed
}
