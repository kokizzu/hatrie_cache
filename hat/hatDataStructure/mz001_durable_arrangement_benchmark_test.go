package hatDataStructure_test

import (
	"fmt"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkMZ01RebuildFromSnapshot(b *testing.B) {
	_, rows, cleanup := mz001BenchmarkSegment(b)
	defer cleanup()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		arrangement, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
			MemoryLimitBytes: 64 << 20,
			MaxDiskBytes:     64 << 20,
		})
		if err != nil {
			b.Fatal(err)
		}
		for _, row := range rows {
			if err := arrangement.Set(row.Key, row.Value); err != nil {
				b.Fatal(err)
			}
		}
		if err := arrangement.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ01OpenDurableSegment(b *testing.B) {
	path, _, cleanup := mz001BenchmarkSegment(b)
	defer cleanup()
	options := hatDataStructure.SpillableArrangementOptions{
		MemoryLimitBytes: 64 << 20,
		MaxDiskBytes:     64 << 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		arrangement, err := hatDataStructure.OpenSpillableArrangement(path, options)
		if err != nil {
			b.Fatal(err)
		}
		if err := arrangement.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func mz001BenchmarkSegment(b *testing.B) (string, []hatDataStructure.SpillableArrangementEntry, func()) {
	b.Helper()
	source, err := hatDataStructure.NewSpillableArrangement(hatDataStructure.SpillableArrangementOptions{
		Directory:        b.TempDir(),
		MemoryLimitBytes: 1,
		MaxDiskBytes:     64 << 20,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := source.Set(fmt.Sprintf("key-%04d", index), []byte("durable-value")); err != nil {
			b.Fatal(err)
		}
	}
	if err := source.Flush(); err != nil {
		b.Fatal(err)
	}
	rows, err := source.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	path := source.SpillPath()
	if err := source.Close(); err != nil {
		b.Fatal(err)
	}
	return path, rows, func() {}
}
