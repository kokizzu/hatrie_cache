package hatriecache

import (
	"math/bits"
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"
	"unsafe"
)

var (
	benchmarkRoaringBitmapLayoutContainerSink roaringBitmapContainer
	benchmarkRoaringBitmapLayoutDataSink      roaringBitmapData
	benchmarkRoaringBitmapSliceControlSink    roaringBitmapSliceBackingControl
	benchmarkRoaringBitmapCompactControlSink  roaringBitmapCompactFieldOrderControl
)

type roaringBitmapCompactFieldOrderControl struct {
	values      []uint16
	bits        *[roaringBitmapBitmapWords]uint64
	key         uint16
	cardinality uint32
}

func TestRoaringBitmapFixedBitmapBackingPreservesTransitions(t *testing.T) {
	var container roaringBitmapContainer
	for value := 0; value <= roaringBitmapArrayMaxSize; value++ {
		if !container.add(uint16(value)) {
			t.Fatalf("add(%d) = false, want true", value)
		}
	}
	if !container.isBitmap() || len(container.bits) != roaringBitmapBitmapWords {
		t.Fatalf("dense representation = bitmap %v words %d, want bitmap with %d words", container.isBitmap(), len(container.bits), roaringBitmapBitmapWords)
	}
	for _, value := range []uint16{0, 1, roaringBitmapArrayMaxSize} {
		if !container.contains(value) {
			t.Fatalf("contains(%d) = false after bitmap conversion", value)
		}
	}
	wantSnapshot := container.Snapshot()
	restored, err := newRoaringBitmapContainerFromSnapshot(wantSnapshot)
	if err != nil {
		t.Fatalf("newRoaringBitmapContainerFromSnapshot() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatal("snapshot changed across dense container restore")
	}

	for value := roaringBitmapArrayMaxSize; value >= roaringBitmapArrayShrinkSize; value-- {
		if !container.remove(uint16(value)) {
			t.Fatalf("remove(%d) = false, want true", value)
		}
	}
	if container.isBitmap() || len(container.values) != roaringBitmapArrayShrinkSize {
		t.Fatalf("shrunk representation = bitmap %v values %d, want %d-value array", container.isBitmap(), len(container.values), roaringBitmapArrayShrinkSize)
	}
}

func TestRoaringBitmapContainerHeaderFitsOneCacheLine(t *testing.T) {
	if got := unsafe.Sizeof(roaringBitmapContainer{}); got != 48 {
		t.Fatalf("sizeof(roaringBitmapContainer) = %d, want 48", got)
	}
	if got := unsafe.Sizeof(roaringBitmapCompactFieldOrderControl{}); got != 40 {
		t.Fatalf("sizeof(roaringBitmapCompactFieldOrderControl) = %d, want 40", got)
	}
}

func BenchmarkRoaringBitmapFixedBitmapBacking(b *testing.B) {
	b.Run("Build4097", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			var container roaringBitmapContainer
			for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
				container.add(uint16(value))
			}
			benchmarkRoaringBitmapLayoutContainerSink = container
		}
	})

	var container roaringBitmapContainer
	for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
		container.add(uint16(value))
	}
	b.Run("ContainsBitmap", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkBoolSink = container.contains(uint16(iteration & roaringBitmapArrayMaxSize))
		}
	})
	b.Run("RemoveAddBitmap", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			value := uint16(iteration & roaringBitmapArrayMaxSize)
			if !container.remove(value) || !container.add(value) {
				b.Fatal("bitmap remove/add failed")
			}
		}
	})
}

func BenchmarkRoaringBitmapContainerRetained50k(b *testing.B) {
	const items = 50000
	var retainedBytes, retainedObjects uint64
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()
		bitmap := newRoaringBitmapData()
		for item := 0; item < items; item++ {
			bitmap.Add(uint32(item) << roaringBitmapContainerBits)
		}
		b.StopTimer()
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		if after.HeapAlloc > before.HeapAlloc {
			retainedBytes += after.HeapAlloc - before.HeapAlloc
		}
		if after.HeapObjects > before.HeapObjects {
			retainedObjects += after.HeapObjects - before.HeapObjects
		}
		runtime.KeepAlive(bitmap)
		benchmarkRoaringBitmapLayoutDataSink = bitmap
		b.StartTimer()
	}
	b.StopTimer()
	operations := float64(b.N * items)
	b.ReportMetric(float64(retainedBytes)/operations, "retained-B/container")
	b.ReportMetric(float64(retainedObjects)/operations, "retained-objects/container")
}

func BenchmarkRoaringBitmapBackingLayoutAlternating(b *testing.B) {
	const accesses = 1 << 16

	b.Run("Build4097", func(b *testing.B) {
		var sliceDuration, pointerDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			pointerFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if pointerFirst == (pass == 0) {
					var container roaringBitmapContainer
					for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkRoaringBitmapLayoutContainerSink = container
					pointerDuration += time.Since(started)
				} else {
					var container roaringBitmapSliceBackingControl
					for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkRoaringBitmapSliceControlSink = container
					sliceDuration += time.Since(started)
				}
			}
		}
		b.ReportMetric(float64(sliceDuration.Nanoseconds())/float64(b.N), "slice-ns/build")
		b.ReportMetric(float64(pointerDuration.Nanoseconds())/float64(b.N), "pointer-ns/build")
	})

	var control roaringBitmapSliceBackingControl
	var candidate roaringBitmapContainer
	for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
		control.add(uint16(value))
		candidate.add(uint16(value))
	}
	b.Run("Contains", func(b *testing.B) {
		var sliceDuration, pointerDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			pointerFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if pointerFirst == (pass == 0) {
					for access := 0; access < accesses; access++ {
						benchmarkBoolSink = candidate.contains(uint16(access % (roaringBitmapArrayMaxSize + 1)))
					}
					pointerDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						benchmarkBoolSink = control.contains(uint16(access % (roaringBitmapArrayMaxSize + 1)))
					}
					sliceDuration += time.Since(started)
				}
			}
		}
		operations := float64(b.N * accesses)
		b.ReportMetric(float64(sliceDuration.Nanoseconds())/operations, "slice-ns/access")
		b.ReportMetric(float64(pointerDuration.Nanoseconds())/operations, "pointer-ns/access")
	})

	b.Run("RemoveAdd", func(b *testing.B) {
		var sliceDuration, pointerDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			pointerFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if pointerFirst == (pass == 0) {
					for access := 0; access < accesses; access++ {
						value := uint16(access % (roaringBitmapArrayMaxSize + 1))
						if !candidate.remove(value) || !candidate.add(value) {
							b.Fatal("pointer-backed remove/add failed")
						}
					}
					pointerDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						value := uint16(access % (roaringBitmapArrayMaxSize + 1))
						if !control.remove(value) || !control.add(value) {
							b.Fatal("slice-backed remove/add failed")
						}
					}
					sliceDuration += time.Since(started)
				}
			}
		}
		operations := float64(b.N * accesses)
		b.ReportMetric(float64(sliceDuration.Nanoseconds())/operations, "slice-ns/access")
		b.ReportMetric(float64(pointerDuration.Nanoseconds())/operations, "pointer-ns/access")
	})
}

func roaringBitmapCompactFieldOrderControlContains(container roaringBitmapCompactFieldOrderControl, value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		return bitmap[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	return idx < len(container.values) && container.values[idx] == value
}

func (container *roaringBitmapCompactFieldOrderControl) add(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		if bitmap[word]&mask != 0 {
			return false
		}
		bitmap[word] |= mask
		container.cardinality++
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	if idx < len(container.values) && container.values[idx] == value {
		return false
	}
	container.values = append(container.values, 0)
	copy(container.values[idx+1:], container.values[idx:])
	container.values[idx] = value
	container.cardinality++
	if len(container.values) > roaringBitmapArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func (container *roaringBitmapCompactFieldOrderControl) convertToBitmap() {
	if container.bits != nil {
		return
	}
	next := new([roaringBitmapBitmapWords]uint64)
	for _, value := range container.values {
		word, mask := roaringBitmapBit(value)
		next[word] |= mask
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.bits = next
}

func roaringBitmapCurrentDenseRemoveAdd(container *roaringBitmapContainer, value uint16) bool {
	bitmap := container.bits
	word, mask := roaringBitmapBit(value)
	if bitmap[word]&mask == 0 {
		return false
	}
	bitmap[word] &^= mask
	container.cardinality--
	if bitmap[word]&mask != 0 {
		return false
	}
	bitmap[word] |= mask
	container.cardinality++
	return true
}

func roaringBitmapCompactFieldOrderControlDenseRemoveAdd(container *roaringBitmapCompactFieldOrderControl, value uint16) bool {
	bitmap := container.bits
	word, mask := roaringBitmapBit(value)
	if bitmap[word]&mask == 0 {
		return false
	}
	bitmap[word] &^= mask
	container.cardinality--
	if bitmap[word]&mask != 0 {
		return false
	}
	bitmap[word] |= mask
	container.cardinality++
	return true
}

func BenchmarkRoaringBitmapFieldOrderAlternating(b *testing.B) {
	const accesses = 1 << 16

	b.Run("Build4097", func(b *testing.B) {
		var currentDuration, candidateDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			candidateFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if candidateFirst == (pass == 0) {
					var container roaringBitmapCompactFieldOrderControl
					for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkRoaringBitmapCompactControlSink = container
					candidateDuration += time.Since(started)
				} else {
					var container roaringBitmapContainer
					for value := roaringBitmapArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkRoaringBitmapLayoutContainerSink = container
					currentDuration += time.Since(started)
				}
			}
		}
		b.ReportMetric(float64(currentDuration.Nanoseconds())/float64(b.N), "current-ns/build")
		b.ReportMetric(float64(candidateDuration.Nanoseconds())/float64(b.N), "candidate-ns/build")
	})

	current := roaringBitmapContainer{
		bits:        new([roaringBitmapBitmapWords]uint64),
		cardinality: roaringBitmapArrayMaxSize + 1,
	}
	candidate := roaringBitmapCompactFieldOrderControl{
		bits:        new([roaringBitmapBitmapWords]uint64),
		cardinality: roaringBitmapArrayMaxSize + 1,
	}
	for value := 0; value <= roaringBitmapArrayMaxSize; value++ {
		word, mask := roaringBitmapBit(uint16(value))
		current.bits[word] |= mask
		candidate.bits[word] |= mask
	}

	b.Run("Contains", func(b *testing.B) {
		var currentDuration, candidateDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			candidateFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if candidateFirst == (pass == 0) {
					for access := 0; access < accesses; access++ {
						benchmarkBoolSink = roaringBitmapCompactFieldOrderControlContains(candidate, uint16(access&roaringBitmapArrayMaxSize))
					}
					candidateDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						benchmarkBoolSink = current.contains(uint16(access & roaringBitmapArrayMaxSize))
					}
					currentDuration += time.Since(started)
				}
			}
		}
		operations := float64(b.N * accesses)
		b.ReportMetric(float64(currentDuration.Nanoseconds())/operations, "current-ns/access")
		b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/access")
	})

	b.Run("RemoveAdd", func(b *testing.B) {
		var currentDuration, candidateDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			candidateFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if candidateFirst == (pass == 0) {
					for access := 0; access < accesses; access++ {
						if !roaringBitmapCompactFieldOrderControlDenseRemoveAdd(&candidate, uint16(access&roaringBitmapArrayMaxSize)) {
							b.Fatal("candidate dense remove/add failed")
						}
					}
					candidateDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						if !roaringBitmapCurrentDenseRemoveAdd(&current, uint16(access&roaringBitmapArrayMaxSize)) {
							b.Fatal("current dense remove/add failed")
						}
					}
					currentDuration += time.Since(started)
				}
			}
		}
		operations := float64(b.N * accesses)
		b.ReportMetric(float64(currentDuration.Nanoseconds())/operations, "current-ns/access")
		b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/access")
	})
}

type roaringBitmapSliceBackingControl struct {
	values      []uint16
	bits        []uint64
	cardinality uint32
}

func (container *roaringBitmapSliceBackingControl) add(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		if bitmap[word]&mask != 0 {
			return false
		}
		bitmap[word] |= mask
		container.cardinality++
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	if idx < len(container.values) && container.values[idx] == value {
		return false
	}
	container.values = append(container.values, 0)
	copy(container.values[idx+1:], container.values[idx:])
	container.values[idx] = value
	container.cardinality++
	if len(container.values) > roaringBitmapArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func (container *roaringBitmapSliceBackingControl) remove(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		if bitmap[word]&mask == 0 {
			return false
		}
		bitmap[word] &^= mask
		container.cardinality--
		if container.cardinality <= roaringBitmapArrayShrinkSize {
			container.convertToArray()
		}
		return true
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	if idx >= len(container.values) || container.values[idx] != value {
		return false
	}
	copy(container.values[idx:], container.values[idx+1:])
	container.values[len(container.values)-1] = 0
	container.values = container.values[:len(container.values)-1]
	container.cardinality--
	if cap(container.values) > 16 && len(container.values)*4 < cap(container.values) {
		next := make([]uint16, len(container.values))
		copy(next, container.values)
		container.values = next
	}
	return true
}

func (container roaringBitmapSliceBackingControl) contains(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := roaringBitmapBit(value)
		return bitmap[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	return idx < len(container.values) && container.values[idx] == value
}

func (container *roaringBitmapSliceBackingControl) convertToBitmap() {
	if container.bits != nil {
		return
	}
	next := make([]uint64, roaringBitmapBitmapWords)
	for _, value := range container.values {
		word, mask := roaringBitmapBit(value)
		next[word] |= mask
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.bits = next
}

func (container *roaringBitmapSliceBackingControl) convertToArray() {
	if container.bits == nil {
		return
	}
	values := make([]uint16, 0, container.cardinality)
	for wordIdx, word := range container.bits {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			values = append(values, uint16(wordIdx*64+bit))
			word &^= uint64(1) << uint(bit)
		}
	}
	for idx := range container.bits {
		container.bits[idx] = 0
	}
	container.bits = nil
	container.values = values
}
