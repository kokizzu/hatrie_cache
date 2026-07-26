package hatriecache

import (
	"reflect"
	"runtime"
	"sort"
	"testing"
	"time"
	"unsafe"
)

var (
	benchmarkSparseBitsetContainerSink sparseBitsetContainer
	benchmarkSparseBitsetDataSink      sparseBitsetData
)

func TestSparseBitsetSmallContainerTransitionsPreserveBehavior(t *testing.T) {
	var container sparseBitsetContainer
	for _, value := range []uint16{9, 3, 7} {
		if !container.add(value) {
			t.Fatalf("add(%d) = false, want true", value)
		}
	}
	if len(container.values) != 3 || container.inline != [2]uint16{} {
		t.Fatalf("three-value representation = values %#v inline %#v, want promoted slice", container.values, container.inline)
	}
	if container.add(7) {
		t.Fatal("duplicate add(7) = true, want false")
	}
	for _, value := range []uint16{3, 7, 9} {
		if !container.contains(value) {
			t.Fatalf("contains(%d) = false, want true", value)
		}
	}
	if container.contains(8) {
		t.Fatal("contains(8) = true, want false")
	}
	if got := container.appendValues(nil); !reflect.DeepEqual(got, []uint64{3, 7, 9}) {
		t.Fatalf("appendValues() = %#v, want sorted values", got)
	}
	if got := container.EncodedSize(); got != 6 {
		t.Fatalf("EncodedSize() = %d, want 6", got)
	}
	wantSnapshot := container.Snapshot()

	if !container.remove(7) {
		t.Fatal("remove(7) = false, want true")
	}
	if container.values != nil || container.inline != [2]uint16{3, 9} {
		t.Fatalf("two-value representation = values %#v inline %#v, want inline [3 9]", container.values, container.inline)
	}
	if !container.remove(9) {
		t.Fatal("remove(9) = false, want true")
	}
	if got := container.appendValues(nil); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("appendValues() after removals = %#v, want [3]", got)
	}
	if !container.remove(3) || !container.empty() {
		t.Fatal("final remove did not empty container")
	}

	restored, err := newSparseBitsetContainerFromSnapshot(wantSnapshot)
	if err != nil {
		t.Fatalf("newSparseBitsetContainerFromSnapshot() error = %v", err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, wantSnapshot)
	}

	for count := 1; count <= 2; count++ {
		var small sparseBitsetContainer
		for value := 1; value <= count; value++ {
			small.add(uint16(value))
		}
		restored, err := newSparseBitsetContainerFromSnapshot(small.Snapshot())
		if err != nil {
			t.Fatalf("newSparseBitsetContainerFromSnapshot(%d) error = %v", count, err)
		}
		if restored.values != nil || restored.cardinality != uint32(count) {
			t.Fatalf("restored %d-value representation = values %#v cardinality %d, want inline", count, restored.values, restored.cardinality)
		}
	}
}

func TestSparseBitsetContainerHasFourPaddingBytes(t *testing.T) {
	if got := unsafe.Sizeof(sparseBitsetContainer{}); got != 64 {
		t.Fatalf("sizeof(sparseBitsetContainer) = %d, want 64", got)
	}
}

func BenchmarkSparseBitsetSmallContainer(b *testing.B) {
	for _, count := range []int{1, 2, 3, 16} {
		b.Run("Build"+smallBenchmarkCountName(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var container sparseBitsetContainer
				for value := count; value > 0; value-- {
					container.add(uint16(value))
				}
				benchmarkSparseBitsetContainerSink = container
			}
		})

		var container sparseBitsetContainer
		for value := count; value > 0; value-- {
			container.add(uint16(value))
		}
		b.Run("Contains"+smallBenchmarkCountName(count), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkBoolSink = container.contains(uint16(i%count + 1))
			}
		})
	}
}

func BenchmarkSparseBitsetDistinctSmallContainers(b *testing.B) {
	const items = 1 << 14
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bitset := newSparseBitsetData()
		for item := 0; item < items; item++ {
			bitset.Add(uint64(item) << sparseBitsetContainerBits)
		}
		benchmarkSparseBitsetDataSink = bitset
	}
}

func BenchmarkSparseBitsetInlineLayout(b *testing.B) {
	for _, count := range []int{1, 2, 3, 16, 4096, 4097} {
		for _, layout := range []struct {
			name     string
			add      func(*sparseBitsetContainer, uint16) bool
			contains func(sparseBitsetContainer, uint16) bool
		}{
			{name: "SliceControl", add: sparseBitsetContainerAddSliceControl, contains: sparseBitsetContainerContainsSliceControl},
			{name: "Inline", add: (*sparseBitsetContainer).add, contains: sparseBitsetContainer.contains},
		} {
			b.Run("Build"+smallBenchmarkCountName(count)+"/"+layout.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					var container sparseBitsetContainer
					for value := count; value > 0; value-- {
						layout.add(&container, uint16(value))
					}
					benchmarkSparseBitsetContainerSink = container
				}
			})

			var container sparseBitsetContainer
			for value := count; value > 0; value-- {
				layout.add(&container, uint16(value))
			}
			b.Run("Contains"+smallBenchmarkCountName(count)+"/"+layout.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchmarkBoolSink = layout.contains(container, uint16(i%count+1))
				}
			})
		}
	}

	for _, layout := range []struct {
		name string
		add  func(*sparseBitsetData, uint64) bool
	}{
		{name: "SliceControl", add: sparseBitsetAddSliceControl},
		{name: "Inline", add: (*sparseBitsetData).Add},
	} {
		b.Run("Distinct16384/"+layout.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				bitset := newSparseBitsetData()
				for item := 0; item < 1<<14; item++ {
					layout.add(&bitset, uint64(item)<<sparseBitsetContainerBits)
				}
				benchmarkSparseBitsetDataSink = bitset
			}
		})
	}
}

func BenchmarkSparseBitsetDistinctSmallContainersAlternating(b *testing.B) {
	const items = 1 << 14
	var sliceDuration, inlineDuration time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		layouts := []struct {
			inline bool
			add    func(*sparseBitsetData, uint64) bool
		}{
			{add: sparseBitsetAddSliceControl},
			{inline: true, add: (*sparseBitsetData).Add},
		}
		if iteration&1 != 0 {
			layouts[0], layouts[1] = layouts[1], layouts[0]
		}
		for _, layout := range layouts {
			started := time.Now()
			bitset := newSparseBitsetData()
			for item := 0; item < items; item++ {
				layout.add(&bitset, uint64(item)<<sparseBitsetContainerBits)
			}
			elapsed := time.Since(started)
			if layout.inline {
				inlineDuration += elapsed
			} else {
				sliceDuration += elapsed
			}
			benchmarkSparseBitsetDataSink = bitset
		}
	}
	b.ReportMetric(float64(sliceDuration.Nanoseconds())/float64(b.N), "slice-ns/op")
	b.ReportMetric(float64(inlineDuration.Nanoseconds())/float64(b.N), "inline-ns/op")
}

func BenchmarkSparseBitsetInlineRetained100k(b *testing.B) {
	const items = 100000
	for _, layout := range []struct {
		name string
		add  func(*sparseBitsetData, uint64) bool
	}{
		{name: "SliceControl", add: sparseBitsetAddSliceControl},
		{name: "Inline", add: (*sparseBitsetData).Add},
	} {
		b.Run(layout.name, func(b *testing.B) {
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
					layout.add(&bitset, uint64(item)<<sparseBitsetContainerBits)
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
				b.StartTimer()
			}
			b.StopTimer()
			operations := float64(b.N * items)
			b.ReportMetric(float64(retainedBytes)/operations, "retained-B/container")
			b.ReportMetric(float64(retainedObjects)/operations, "retained-objects/container")
		})
	}
}

func sparseBitsetAddSliceControl(bitset *sparseBitsetData, value uint64) bool {
	key, low := sparseBitsetSplit(value)
	idx, found := bitset.findContainer(key)
	if !found {
		container := sparseBitsetContainer{key: key}
		sparseBitsetContainerAddSliceControl(&container, low)
		bitset.containers = insertSparseBitsetContainer(bitset.containers, idx, container)
		bitset.count++
		return true
	}
	if sparseBitsetContainerAddSliceControl(&bitset.containers[idx], low) {
		bitset.count++
		return true
	}
	return false
}

func sparseBitsetContainerAddSliceControl(container *sparseBitsetContainer, value uint16) bool {
	if container.isBitmap() {
		word, mask := sparseBitsetBit(value)
		if container.bits[word]&mask != 0 {
			return false
		}
		container.bits[word] |= mask
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
	if len(container.values) > sparseBitsetArrayMaxSize {
		container.convertToBitmap()
	}
	return true
}

func sparseBitsetContainerContainsSliceControl(container sparseBitsetContainer, value uint16) bool {
	if container.isBitmap() {
		word, mask := sparseBitsetBit(value)
		return container.bits[word]&mask != 0
	}
	idx := sort.Search(len(container.values), func(idx int) bool {
		return container.values[idx] >= value
	})
	return idx < len(container.values) && container.values[idx] == value
}

func smallBenchmarkCountName(count int) string {
	switch count {
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3"
	case 16:
		return "16"
	case 4096:
		return "4096"
	case 4097:
		return "4097"
	default:
		return "N"
	}
}
