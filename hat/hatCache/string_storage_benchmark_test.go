package hatCache

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func BenchmarkStringStorageLayout100k(b *testing.B) {
	keys := stringStorageBenchmarkInt("HATRIE_STRING_STORAGE_KEYS", 100000)
	keyValues := make([]string, keys)
	values := make([]string, keys)
	for index := 0; index < keys; index++ {
		keyValues[index] = fmt.Sprintf("string-layout:%09d", index)
		prefix := fmt.Sprintf("%09d:", index)
		values[index] = prefix + strings.Repeat("x", 256-len(prefix))
	}

	b.Run("Insert256", func(b *testing.B) {
		var retained uint64
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			trie := CreateHatTrie()
			b.StartTimer()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], values[index])
			}
			b.StopTimer()
			runtime.GC()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			if after.HeapAlloc > before.HeapAlloc {
				retained += after.HeapAlloc - before.HeapAlloc
			}
			if trie.Size() != keys || trie.GetString(keyValues[keys-1]) != values[keys-1] {
				trie.Destroy()
				b.Fatal("inserted string state mismatch")
			}
			trie.Destroy()
			b.StartTimer()
		}
		b.StopTimer()
		b.ReportMetric(float64(keys), "keys/op")
		b.ReportMetric(float64(retained)/float64(b.N*keys), "retained_B/key")
	})

	b.Run("Replace256", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			b.StopTimer()
			trie := CreateHatTrie()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], "old")
			}
			b.StartTimer()
			for index := range keyValues {
				trie.UpsertString(keyValues[index], values[index])
			}
			b.StopTimer()
			if trie.GetString(keyValues[keys-1]) != values[keys-1] {
				trie.Destroy()
				b.Fatal("replaced string state mismatch")
			}
			trie.Destroy()
			b.StartTimer()
		}
		b.StopTimer()
		b.ReportMetric(float64(keys), "keys/op")
	})
}

func BenchmarkStringUpsertCheckedPaths(b *testing.B) {
	for _, benchmark := range []struct {
		name      string
		replacing bool
	}{
		{name: "Duplicate"},
		{name: "Replacement", replacing: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			trie.UpsertString("key", "value")
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				value := "value"
				if benchmark.replacing && iteration&1 != 0 {
					value = "replacement"
				}
				if err := trie.UpsertStringChecked("key", value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStringStorageReplaceActiveAlternating(b *testing.B) {
	const puts = 1 << 18
	for _, benchmark := range []struct {
		name      string
		replacing bool
	}{
		{name: "Duplicate"},
		{name: "Replacement", replacing: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := CreateStringStorage()
			control := CreateStringStorage()
			candidateIndex := candidate.Add("value")
			controlIndex := control.Add("value")
			var candidateDuration, controlDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if candidateFirst == (pass == 0) {
						for put := 0; put < puts; put++ {
							value := "value"
							if benchmark.replacing && put&1 != 0 {
								value = "replacement"
							}
							candidate.replaceActive(candidateIndex, value)
						}
						candidateDuration += time.Since(started)
					} else {
						for put := 0; put < puts; put++ {
							value := "value"
							if benchmark.replacing && put&1 != 0 {
								value = "replacement"
							}
							control.Put(controlIndex, value)
						}
						controlDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * puts)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/put")
			b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/put")
		})
	}
}

func BenchmarkStringUpsertCheckedAlternating(b *testing.B) {
	const puts = 1 << 16
	for _, benchmark := range []struct {
		name      string
		replacing bool
	}{
		{name: "Duplicate"},
		{name: "Replacement", replacing: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			candidate := CreateHatTrie()
			control := CreateHatTrie()
			b.Cleanup(candidate.Destroy)
			b.Cleanup(control.Destroy)
			candidate.UpsertString("key", "value")
			control.UpsertString("key", "value")
			var candidateDuration, controlDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				candidateFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if candidateFirst == (pass == 0) {
						for put := 0; put < puts; put++ {
							value := "value"
							if benchmark.replacing && put&1 != 0 {
								value = "replacement"
							}
							if err := candidate.UpsertStringChecked("key", value); err != nil {
								b.Fatal(err)
							}
						}
						candidateDuration += time.Since(started)
					} else {
						for put := 0; put < puts; put++ {
							value := "value"
							if benchmark.replacing && put&1 != 0 {
								value = "replacement"
							}
							if err := benchmarkLegacyUpsertStringChecked(control, "key", value); err != nil {
								b.Fatal(err)
							}
						}
						controlDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * puts)
			b.ReportMetric(float64(candidateDuration.Nanoseconds())/operations, "candidate-ns/put")
			b.ReportMetric(float64(controlDuration.Nanoseconds())/operations, "control-ns/put")
		})
	}
}

func benchmarkLegacyUpsertStringChecked(trie *HatTrie, key string, value string) error {
	if trie == nil {
		return ErrNilHatTrie
	}
	if partition := trie.localPartitionForKey(key); partition != nil {
		return benchmarkLegacyUpsertStringChecked(partition, key, value)
	}
	trie.mu.Lock()
	defer trie.mu.Unlock()

	rawPtr, hval, err := trie.upsertReplacementLocation(key)
	if err != nil {
		return err
	}
	if hval.IsStringAtRaws() {
		trie.strings.Put(hval.Index, value)
		trie.clearExpirationLocked(key)
		hval.Flags &^= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
		trie.recordWriteLocked(key)
		return nil
	}

	trie.returnStorage(hval)
	trie.clearExpirationLocked(key)
	idx := trie.strings.Add(value)
	*rawPtr = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
	trie.recordWriteLocked(key)
	return nil
}

func BenchmarkStringCompaction100k(b *testing.B) {
	keys := stringStorageBenchmarkInt("HATRIE_STRING_STORAGE_KEYS", 100000)
	keyValues := make([]string, keys)
	for index := range keyValues {
		keyValues[index] = fmt.Sprintf("string-compaction:%09d", index)
	}

	var retainedHeapBefore uint64
	var retainedHeapAfter uint64
	var retainedObjectsBefore uint64
	var retainedObjectsAfter uint64
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		runtime.GC()
		var baseline runtime.MemStats
		runtime.ReadMemStats(&baseline)
		trie := CreateHatTrie()
		for index, key := range keyValues {
			prefix := fmt.Sprintf("%09d:", index)
			valueBytes := 33 + index%480
			trie.UpsertString(key, prefix+strings.Repeat("x", valueBytes-len(prefix)))
		}
		runtime.GC()
		var loaded runtime.MemStats
		runtime.ReadMemStats(&loaded)
		if loaded.HeapAlloc > baseline.HeapAlloc {
			retainedHeapBefore += loaded.HeapAlloc - baseline.HeapAlloc
		}
		if loaded.HeapObjects > baseline.HeapObjects {
			retainedObjectsBefore += loaded.HeapObjects - baseline.HeapObjects
		}
		b.StartTimer()
		if _, err := trie.CompactMemory(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		runtime.GC()
		var compacted runtime.MemStats
		runtime.ReadMemStats(&compacted)
		if compacted.HeapAlloc > baseline.HeapAlloc {
			retainedHeapAfter += compacted.HeapAlloc - baseline.HeapAlloc
		}
		if compacted.HeapObjects > baseline.HeapObjects {
			retainedObjectsAfter += compacted.HeapObjects - baseline.HeapObjects
		}
		if trie.Size() != keys || trie.GetString(keyValues[keys-1]) == "" {
			trie.Destroy()
			b.Fatal("compacted string state mismatch")
		}
		trie.Destroy()
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(keys), "keys/op")
	b.ReportMetric(float64(retainedHeapBefore)/float64(b.N), "retained_before_B/op")
	b.ReportMetric(float64(retainedHeapAfter)/float64(b.N), "retained_after_B/op")
	b.ReportMetric(float64(retainedObjectsBefore)/float64(b.N), "objects_before/op")
	b.ReportMetric(float64(retainedObjectsAfter)/float64(b.N), "objects_after/op")
}

func BenchmarkStringCompactionPostGC100k(b *testing.B) {
	keys := stringStorageBenchmarkInt("HATRIE_STRING_STORAGE_KEYS", 100000)
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	for index := 0; index < keys; index++ {
		prefix := fmt.Sprintf("%09d:", index)
		valueBytes := 33 + index%480
		trie.UpsertString(fmt.Sprintf("string-compaction-gc:%09d", index), prefix+strings.Repeat("x", valueBytes-len(prefix)))
	}
	if _, err := trie.CompactMemory(); err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		runtime.GC()
	}
}

func stringStorageBenchmarkInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
