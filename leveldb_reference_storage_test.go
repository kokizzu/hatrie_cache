package hatriecache

import (
	"reflect"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func TestLevelDBReferenceStorageUsesCompactSharedStoreRecords(t *testing.T) {
	if got, limit := unsafe.Sizeof(levelDBReferenceRecord{}), uintptr(64); got > limit {
		t.Fatalf("compact reference record size = %d, want <= %d bytes", got, limit)
	}
	if compact, public := unsafe.Sizeof(levelDBReferenceRecord{}), unsafe.Sizeof(LevelDBReference{}); compact >= public {
		t.Fatalf("compact/public reference sizes = %d/%d, want compact smaller", compact, public)
	}

	store := &LevelDBStore{}
	expiresAt := time.Unix(1_700_000_000, 123_456_789).UTC()
	stats := &KeyStats{Reads: 7, Hits: 5, LastHit: expiresAt}
	storage := CreateLevelDBReferenceStorage()
	first := LevelDBReference{
		Key:            "first",
		Type:           "string",
		Store:          store,
		ExpiresAt:      &expiresAt,
		Stats:          stats,
		RecordBytes:    123,
		RecordChecksum: 456,
		Token:          789,
	}
	firstIndex := storage.Add(first)
	secondIndex := storage.Add(LevelDBReference{Key: "second", Type: "counter", Store: store})
	if got := len(storage.stores); got != 1 {
		t.Fatalf("shared store handles = %d, want 1", got)
	}
	if got, ok := storage.Get(firstIndex); !ok || !reflect.DeepEqual(got, first) {
		t.Fatalf("Get(first) = %#v/%v, want %#v/true", got, ok, first)
	}

	replacement := LevelDBReference{Key: "updated", Type: "bytes", Store: store, RecordBytes: 9, Token: 10}
	if !storage.Set(firstIndex, replacement) {
		t.Fatal("Set(first) = false, want true")
	}
	if got, ok := storage.Get(firstIndex); !ok || !reflect.DeepEqual(got, replacement) {
		t.Fatalf("Get(updated) = %#v/%v, want %#v/true", got, ok, replacement)
	}
	storage.Del(firstIndex)
	if _, ok := storage.Get(firstIndex); ok {
		t.Fatal("Get(deleted) ok = true, want false")
	}
	storage.Del(secondIndex)
	if got := len(storage.array); got != 0 {
		t.Fatalf("trimmed compact records = %d, want 0", got)
	}
}

func BenchmarkLevelDBReferenceRetainedMemory100k(b *testing.B) {
	const references = 100_000
	store := &LevelDBStore{}
	for _, mode := range []string{"LegacyStruct", "CompactSlab"} {
		b.Run(mode, func(b *testing.B) {
			var retained uint64
			for iteration := 0; iteration < b.N; iteration++ {
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				var value any
				if mode == "LegacyStruct" {
					records := make([]LevelDBReference, 0)
					for index := 0; index < references; index++ {
						records = append(records, LevelDBReference{Key: "shared-key", Type: "string", Store: store, RecordBytes: 64, Token: uint64(index + 1)})
					}
					value = records
				} else {
					storage := CreateLevelDBReferenceStorage()
					for index := 0; index < references; index++ {
						storage.Add(LevelDBReference{Key: "shared-key", Type: "string", Store: store, RecordBytes: 64, Token: uint64(index + 1)})
					}
					value = storage
				}
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > before.HeapAlloc {
					retained += after.HeapAlloc - before.HeapAlloc
				}
				runtime.KeepAlive(value)
			}
			b.ReportMetric(float64(retained)/float64(b.N*references), "retained_B/ref")
		})
	}
}
