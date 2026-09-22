//go:build t215

package hatDataStructure

import (
	"fmt"
	"testing"
)

func BenchmarkT215BaselineMapGet(b *testing.B) {
	rows := t215BenchmarkMapRows()
	keys := t215BenchmarkKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value := append([]byte(nil), rows[keys[index%len(keys)]]...)
		t215BenchmarkSink = uint64(len(value))
	}
}

func BenchmarkT215MemtxGet(b *testing.B) {
	space := t215BenchmarkMemtxSpace(b)
	keys := t215BenchmarkKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, found, err := space.Get(keys[index%len(keys)])
		if err != nil || !found {
			b.Fatal(err)
		}
		t215BenchmarkSink = uint64(len(value))
	}
}

func BenchmarkT215OnDiskColdGet(b *testing.B) {
	space, err := NewStorageSpace(StorageSpaceOptions{
		Name:             "cold-benchmark",
		Mode:             StorageSpaceOnDisk,
		Directory:        b.TempDir(),
		MemoryLimitBytes: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer space.Close()
	for index := 0; index < 1024; index++ {
		if err := space.Set(t215BenchmarkKey(index), []byte("benchmark-value")); err != nil {
			b.Fatal(err)
		}
	}
	if err := space.Flush(); err != nil {
		b.Fatal(err)
	}
	keys := t215BenchmarkKeys()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, found, err := space.Get(keys[index%len(keys)])
		if err != nil || !found {
			b.Fatal(err)
		}
		t215BenchmarkSink = uint64(len(value))
	}
}

var t215BenchmarkSink uint64

func t215BenchmarkMapRows() map[string][]byte {
	rows := make(map[string][]byte, 1024)
	for index := 0; index < 1024; index++ {
		rows[t215BenchmarkKey(index)] = []byte("benchmark-value")
	}
	return rows
}

func t215BenchmarkKeys() []string {
	keys := make([]string, 1024)
	for index := range keys {
		keys[index] = t215BenchmarkKey(index)
	}
	return keys
}

func t215BenchmarkMemtxSpace(b *testing.B) *StorageSpace {
	b.Helper()
	space, err := NewStorageSpace(StorageSpaceOptions{Name: "memtx-benchmark", Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		if err := space.Set(t215BenchmarkKey(index), []byte("benchmark-value")); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() { _ = space.Close() })
	return space
}

func t215BenchmarkKey(index int) string {
	return fmt.Sprintf("key-%04d", index%1024)
}
