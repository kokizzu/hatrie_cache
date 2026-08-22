package hatDataStructure

import (
	"math/bits"
	"reflect"
	"runtime"
	"testing"
	"time"
)

var (
	benchmarkSparseBitsetLayoutContainerSink sparseBitsetContainer
	benchmarkSparseBitsetLayoutDataSink      sparseBitsetData
	benchmarkSparseBitsetSliceControlSink    sparseBitsetSliceBackingControl
)

func TestSparseBitsetFixedBitmapBackingPreservesTransitions(t *testing.T) {
	container := sparseBitsetContainer{key: 42}
	for value := 0; value <= sparseBitsetArrayMaxSize; value++ {
		if !container.add(uint16(value)) {
			t.Fatalf("add(%d) = false, want true", value)
		}
	}
	if !container.isBitmap() || len(container.bits) != sparseBitsetBitmapWords {
		t.Fatalf("dense representation = bitmap %v words %d, want bitmap with %d words", container.isBitmap(), len(container.bits), sparseBitsetBitmapWords)
	}
	for _, value := range []uint16{0, 1, sparseBitsetArrayMaxSize} {
		if !container.contains(value) {
			t.Fatalf("contains(%d) = false after bitmap conversion", value)
		}
	}
	wantSnapshot := container.Snapshot()
	restored, err := newSparseBitsetContainerFromSnapshot(wantSnapshot)
	if err != nil {
		t.Fatalf("newSparseBitsetContainerFromSnapshot() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatal("snapshot changed across dense container restore")
	}

	for value := sparseBitsetArrayMaxSize; value >= sparseBitsetArrayShrinkSize; value-- {
		if !container.remove(uint16(value)) {
			t.Fatalf("remove(%d) = false, want true", value)
		}
	}
	if container.isBitmap() || len(container.values) != sparseBitsetArrayShrinkSize {
		t.Fatalf("shrunk representation = bitmap %v values %d, want %d-value array", container.isBitmap(), len(container.values), sparseBitsetArrayShrinkSize)
	}
}

func BenchmarkSparseBitsetFixedBitmapBacking(b *testing.B) {
	b.Run("Build4097", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			var container sparseBitsetContainer
			for value := sparseBitsetArrayMaxSize; value >= 0; value-- {
				container.add(uint16(value))
			}
			benchmarkSparseBitsetLayoutContainerSink = container
		}
	})

	var container sparseBitsetContainer
	for value := sparseBitsetArrayMaxSize; value >= 0; value-- {
		container.add(uint16(value))
	}
	b.Run("ContainsBitmap", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			benchmarkBoolSink = container.contains(uint16(iteration % (sparseBitsetArrayMaxSize + 1)))
		}
	})
	b.Run("RemoveAddBitmap", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			value := uint16(iteration % (sparseBitsetArrayMaxSize + 1))
			if !container.remove(value) || !container.add(value) {
				b.Fatal("bitmap remove/add failed")
			}
		}
	})
}

func BenchmarkSparseBitsetCompactHeaderRetained100k(b *testing.B) {
	const items = 100000
	var retainedBytes, retainedObjects uint64
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		b.StartTimer()
		bitset := newSparseBitsetData()
		for item := 0; item < items; item++ {
			bitset.Add(uint64(item) << sparseBitsetContainerBits)
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
		runtime.KeepAlive(bitset)
		benchmarkSparseBitsetLayoutDataSink = bitset
		b.StartTimer()
	}
	b.StopTimer()
	operations := float64(b.N * items)
	b.ReportMetric(float64(retainedBytes)/operations, "retained-B/container")
	b.ReportMetric(float64(retainedObjects)/operations, "retained-objects/container")
}

func BenchmarkSparseBitsetBackingLayoutAlternating(b *testing.B) {
	const accesses = 1 << 16

	b.Run("Build4097", func(b *testing.B) {
		var sliceDuration, pointerDuration time.Duration
		for iteration := 0; iteration < b.N; iteration++ {
			pointerFirst := iteration&1 != 0
			for pass := 0; pass < 2; pass++ {
				started := time.Now()
				if pointerFirst == (pass == 0) {
					var container sparseBitsetContainer
					for value := sparseBitsetArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkSparseBitsetLayoutContainerSink = container
					pointerDuration += time.Since(started)
				} else {
					var container sparseBitsetSliceBackingControl
					for value := sparseBitsetArrayMaxSize; value >= 0; value-- {
						container.add(uint16(value))
					}
					benchmarkSparseBitsetSliceControlSink = container
					sliceDuration += time.Since(started)
				}
			}
		}
		b.ReportMetric(float64(sliceDuration.Nanoseconds())/float64(b.N), "slice-ns/build")
		b.ReportMetric(float64(pointerDuration.Nanoseconds())/float64(b.N), "pointer-ns/build")
	})

	var control sparseBitsetSliceBackingControl
	var candidate sparseBitsetContainer
	for value := sparseBitsetArrayMaxSize; value >= 0; value-- {
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
						benchmarkBoolSink = candidate.contains(uint16(access % (sparseBitsetArrayMaxSize + 1)))
					}
					pointerDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						benchmarkBoolSink = control.contains(uint16(access % (sparseBitsetArrayMaxSize + 1)))
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
						value := uint16(access % (sparseBitsetArrayMaxSize + 1))
						if !candidate.remove(value) || !candidate.add(value) {
							b.Fatal("pointer-backed remove/add failed")
						}
					}
					pointerDuration += time.Since(started)
				} else {
					for access := 0; access < accesses; access++ {
						value := uint16(access % (sparseBitsetArrayMaxSize + 1))
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

type sparseBitsetSliceBackingControl struct {
	key         uint64
	values      []uint16
	bits        []uint64
	cardinality uint32
	inline      [2]uint16
}

func (container *sparseBitsetSliceBackingControl) add(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := sparseBitsetBit(value)
		if bitmap[word]&mask != 0 {
			return false
		}
		bitmap[word] |= mask
		container.cardinality++
		return true
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			container.inline[0] = value
			container.cardinality = 1
			return true
		case 1:
			if container.inline[0] == value {
				return false
			}
			if value < container.inline[0] {
				container.inline[1] = container.inline[0]
				container.inline[0] = value
			} else {
				container.inline[1] = value
			}
			container.cardinality = 2
			return true
		case 2:
			if container.inline[0] == value || container.inline[1] == value {
				return false
			}
			container.values = make([]uint16, 3)
			switch {
			case value < container.inline[0]:
				container.values[0] = value
				copy(container.values[1:], container.inline[:])
			case value < container.inline[1]:
				container.values[0] = container.inline[0]
				container.values[1] = value
				container.values[2] = container.inline[1]
			default:
				copy(container.values, container.inline[:])
				container.values[2] = value
			}
			container.inline = [2]uint16{}
			container.cardinality = 3
			return true
		}
	}
	idx := sparseBitsetSearch(container.values, value)
	if idx < len(container.values) && container.values[idx] == value {
		return false
	}
	container.values = append(container.values, 0)
	copy(container.values[idx+1:], container.values[idx:])
	container.values[idx] = value
	container.cardinality++
	if len(container.values) > sparseBitsetArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func (container *sparseBitsetSliceBackingControl) remove(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := sparseBitsetBit(value)
		if bitmap[word]&mask == 0 {
			return false
		}
		bitmap[word] &^= mask
		container.cardinality--
		if container.cardinality <= sparseBitsetArrayShrinkSize {
			container.convertToArray()
		}
		return true
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			return false
		case 1:
			if container.inline[0] != value {
				return false
			}
			container.inline[0] = 0
			container.cardinality = 0
			return true
		case 2:
			switch value {
			case container.inline[0]:
				container.inline[0] = container.inline[1]
			case container.inline[1]:
			default:
				return false
			}
			container.inline[1] = 0
			container.cardinality = 1
			return true
		}
	}
	idx := sparseBitsetSearch(container.values, value)
	if idx >= len(container.values) || container.values[idx] != value {
		return false
	}
	copy(container.values[idx:], container.values[idx+1:])
	container.values[len(container.values)-1] = 0
	container.values = container.values[:len(container.values)-1]
	container.cardinality--
	if len(container.values) <= len(container.inline) {
		copy(container.inline[:], container.values)
		for idx := range container.values {
			container.values[idx] = 0
		}
		container.values = nil
		return true
	}
	if cap(container.values) > 16 && len(container.values)*4 < cap(container.values) {
		next := make([]uint16, len(container.values))
		copy(next, container.values)
		container.values = next
	}
	return true
}

func (container sparseBitsetSliceBackingControl) contains(value uint16) bool {
	if bitmap := container.bits; bitmap != nil {
		word, mask := sparseBitsetBit(value)
		return bitmap[word]&mask != 0
	}
	if container.values == nil {
		switch container.cardinality {
		case 0:
			return false
		case 1:
			return container.inline[0] == value
		default:
			return container.inline[0] == value || container.inline[1] == value
		}
	}
	idx := sparseBitsetSearch(container.values, value)
	return idx < len(container.values) && container.values[idx] == value
}

func (container *sparseBitsetSliceBackingControl) convertToBitmap() {
	if container.bits != nil {
		return
	}
	next := make([]uint64, sparseBitsetBitmapWords)
	if container.values == nil {
		for idx := 0; idx < int(container.cardinality); idx++ {
			word, mask := sparseBitsetBit(container.inline[idx])
			next[word] |= mask
		}
	} else {
		for _, value := range container.values {
			word, mask := sparseBitsetBit(value)
			next[word] |= mask
		}
	}
	for idx := range container.values {
		container.values[idx] = 0
	}
	container.values = nil
	container.inline = [2]uint16{}
	container.bits = next
}

func (container *sparseBitsetSliceBackingControl) convertToArray() {
	if container.bits == nil {
		return
	}
	if container.cardinality <= uint32(len(container.inline)) {
		idx := 0
		for wordIdx, word := range container.bits {
			for word != 0 {
				bit := bits.TrailingZeros64(word)
				container.inline[idx] = uint16(wordIdx*64 + bit)
				idx++
				word &^= uint64(1) << uint(bit)
			}
		}
		for idx := range container.bits {
			container.bits[idx] = 0
		}
		container.bits = nil
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
