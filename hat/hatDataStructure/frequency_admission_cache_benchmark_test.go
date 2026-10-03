package hatDataStructure

import "testing"

type round77BaselineLRUEntry struct {
	key      uint64
	value    uint64
	previous int
	next     int
	used     bool
}

type round77BaselineLRU struct {
	entries []round77BaselineLRUEntry
	indexes map[uint64]int
	front   int
	back    int
	length  int
}

func newRound77BaselineLRU(capacity int) *round77BaselineLRU {
	cache := &round77BaselineLRU{
		entries: make([]round77BaselineLRUEntry, capacity),
		indexes: make(map[uint64]int, capacity),
		front:   -1,
		back:    -1,
	}
	for index := range cache.entries {
		cache.entries[index].previous = -1
		cache.entries[index].next = -1
	}
	return cache
}

func (cache *round77BaselineLRU) get(key uint64) (uint64, bool) {
	index, ok := cache.indexes[key]
	if !ok {
		return 0, false
	}
	cache.moveFront(index)
	return cache.entries[index].value, true
}

func (cache *round77BaselineLRU) set(key, value uint64) {
	if index, ok := cache.indexes[key]; ok {
		cache.entries[index].value = value
		cache.moveFront(index)
		return
	}
	index := -1
	if cache.length == len(cache.entries) {
		index = cache.back
		delete(cache.indexes, cache.entries[index].key)
		cache.unlink(index)
	} else {
		for candidate := range cache.entries {
			if !cache.entries[candidate].used {
				index = candidate
				break
			}
		}
	}
	if index >= 0 {
		cache.entries[index] = round77BaselineLRUEntry{key: key, value: value, previous: -1, next: -1, used: true}
		cache.indexes[key] = index
		cache.linkFront(index)
		if cache.length < len(cache.entries) {
			cache.length++
		}
	}
}

func (cache *round77BaselineLRU) moveFront(index int) {
	if cache.front == index {
		return
	}
	cache.unlink(index)
	cache.linkFront(index)
}

func (cache *round77BaselineLRU) unlink(index int) {
	entry := &cache.entries[index]
	if entry.previous >= 0 {
		cache.entries[entry.previous].next = entry.next
	} else if cache.front == index {
		cache.front = entry.next
	}
	if entry.next >= 0 {
		cache.entries[entry.next].previous = entry.previous
	} else if cache.back == index {
		cache.back = entry.previous
	}
	entry.previous = -1
	entry.next = -1
}

func (cache *round77BaselineLRU) linkFront(index int) {
	entry := &cache.entries[index]
	entry.previous = -1
	entry.next = cache.front
	if cache.front >= 0 {
		cache.entries[cache.front].previous = index
	} else {
		cache.back = index
	}
	cache.front = index
}

func round77WorkloadKey(operation int) uint64 {
	phase := operation % 80
	if phase < 16 {
		return uint64(phase % 4)
	}
	return 1_000_000 + uint64((operation/80)*64+phase-16)
}

func BenchmarkRound77BaselineLRU(b *testing.B) {
	cache := newRound77BaselineLRU(8)
	var checksum uint64
	hits := 0
	b.ResetTimer()
	for operation := 0; operation < b.N; operation++ {
		key := round77WorkloadKey(operation)
		if value, ok := cache.get(key); ok {
			hits++
			checksum += value + 1
			continue
		}
		cache.set(key, key)
		checksum += key + 1
	}
	b.StopTimer()
	if checksum == 0 {
		b.Fatal("unexpected zero checksum")
	}
	b.ReportMetric(float64(hits)/float64(b.N), "hit-ratio")
}

func BenchmarkRound77FrequencyAdmission(b *testing.B) {
	cache, err := NewFrequencyAdmissionCache[uint64, uint64](FrequencyAdmissionCacheOptions[uint64]{
		Capacity:     8,
		Hash:         round77AdmissionHash,
		CounterCount: 4096,
		SampleWindow: 1 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	var checksum uint64
	b.ResetTimer()
	for operation := 0; operation < b.N; operation++ {
		key := round77WorkloadKey(operation)
		if value, ok := cache.Get(key); ok {
			checksum += value + 1
			continue
		}
		if cache.Set(key, key) {
			checksum += key + 1
		}
	}
	b.StopTimer()
	if checksum == 0 {
		b.Fatal("unexpected zero checksum")
	}
	stats := cache.Stats()
	b.ReportMetric(float64(stats.Hits)/float64(b.N), "hit-ratio")
	b.ReportMetric(float64(stats.Rejected)/float64(b.N), "rejects/op")
	b.ReportMetric(float64(stats.CounterBytes), "counter-bytes")
}

func BenchmarkRound77FrequencyAdmissionHit(b *testing.B) {
	cache, err := NewFrequencyAdmissionCache[uint64, uint64](FrequencyAdmissionCacheOptions[uint64]{
		Capacity: 8,
		Hash:     round77AdmissionHash,
	})
	if err != nil {
		b.Fatal(err)
	}
	if !cache.Set(7, 7) {
		b.Fatal("initial Set() rejected")
	}
	var checksum uint64
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := cache.Get(7)
		if !ok {
			b.Fatal("resident key disappeared")
		}
		checksum += value
	}
	b.StopTimer()
	if checksum == 0 {
		b.Fatal("unexpected zero checksum")
	}
}
