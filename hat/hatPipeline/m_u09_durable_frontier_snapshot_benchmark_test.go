package hatPipeline

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkFrontierRegistryDurableSnapshotSave(b *testing.B) {
	registry := benchmarkDurableFrontierRegistry(b)
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{
		Path: filepath.Join(b.TempDir(), "frontiers.bin"),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.SaveDurableSnapshot(context.Background(), store); err != nil {
		b.Fatal(err)
	}
	payload, err := registry.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.SaveDurableSnapshot(context.Background(), store); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistryDurableSnapshotRestore(b *testing.B) {
	registry := benchmarkDurableFrontierRegistry(b)
	store, err := NewFrontierSnapshotFileStore(FrontierSnapshotFileStoreOptions{
		Path: filepath.Join(b.TempDir(), "frontiers.bin"),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.SaveDurableSnapshot(context.Background(), store); err != nil {
		b.Fatal(err)
	}
	payload, err := registry.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		restored, err := NewFrontierRegistry(FrontierRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		found, err := restored.RestoreDurableSnapshot(context.Background(), store)
		if err != nil || !found {
			b.Fatalf("RestoreDurableSnapshot() = found %v, error %v", found, err)
		}
	}
}

func benchmarkDurableFrontierRegistry(b *testing.B) *FrontierRegistry {
	b.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 128})
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		id := benchmarkFrontierID(i)
		if err := registry.Register(id); err != nil {
			b.Fatal(err)
		}
		if err := registry.Advance(id, uint64(i), uint64(i+100)); err != nil {
			b.Fatal(err)
		}
	}
	return registry
}

func benchmarkFrontierID(index int) string {
	return "frontier-" + benchmarkFrontierDigits(index)
}

func benchmarkFrontierDigits(index int) string {
	if index < 10 {
		return "00" + string(rune('0'+index))
	}
	if index < 100 {
		return "0" + string(rune('0'+index/10)) + string(rune('0'+index%10))
	}
	return string(rune('0'+index/100)) + string(rune('0'+(index/10)%10)) + string(rune('0'+index%10))
}
