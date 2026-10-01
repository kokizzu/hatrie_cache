package hatMetrics

import (
	"strconv"
	"testing"
)

func BenchmarkSourceHealthRegistrySnapshot(b *testing.B) {
	registry := NewSourceHealthRegistry(1024)
	for index := 0; index < 1024; index++ {
		if err := registry.Record("source-"+strconv.Itoa(index), SourceHealthHealthy, uint64(index), ""); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := registry.Snapshot(2048)
		if len(rows) != 1024 {
			b.Fatalf("snapshot length = %d, want 1024", len(rows))
		}
	}
}

func BenchmarkSourceFrontierRegistrySnapshot(b *testing.B) {
	registry := NewSourceFrontierRegistry()
	for index := 0; index < 1024; index++ {
		if err := registry.Advance("source-"+strconv.Itoa(index), uint64(index)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows := registry.Snapshot(2048)
		if len(rows) != 1024 {
			b.Fatalf("snapshot length = %d, want 1024", len(rows))
		}
	}
}

func BenchmarkSourceHealthRegistryRecord(b *testing.B) {
	registry := NewSourceHealthRegistry(1)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := registry.Record("source", SourceHealthHealthy, uint64(index), ""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSourceFrontierRegistryAdvance(b *testing.B) {
	registry := NewSourceFrontierRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := registry.Advance("source", uint64(index)); err != nil {
			b.Fatal(err)
		}
	}
}
