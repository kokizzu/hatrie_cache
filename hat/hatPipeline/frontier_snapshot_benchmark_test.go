package hatPipeline

import (
	"encoding/json"
	"fmt"
	"testing"
)

func BenchmarkFrontierRegistryMarshalSnapshot(b *testing.B) {
	registry := benchmarkFrontierRegistry(b)
	encoded, err := registry.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))
	b.ReportMetric(float64(len(encoded)), "snapshot-bytes")
	for index := 0; index < b.N; index++ {
		if _, err := registry.MarshalSnapshot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistryMarshalJSONSnapshot(b *testing.B) {
	registry := benchmarkFrontierRegistry(b)
	snapshots := registry.SnapshotAll()
	encoded, err := json.Marshal(snapshots)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))
	b.ReportMetric(float64(len(encoded)), "snapshot-bytes")
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(snapshots); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistryDecodeSnapshot(b *testing.B) {
	registry := benchmarkFrontierRegistry(b)
	encoded, err := registry.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))
	b.ReportMetric(float64(len(encoded)), "snapshot-bytes")
	for index := 0; index < b.N; index++ {
		if _, err := decodeFrontierSnapshot(encoded, 128); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFrontierRegistryDecodeJSONSnapshot(b *testing.B) {
	registry := benchmarkFrontierRegistry(b)
	snapshots := registry.SnapshotAll()
	encoded, err := json.Marshal(snapshots)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))
	b.ReportMetric(float64(len(encoded)), "snapshot-bytes")
	for index := 0; index < b.N; index++ {
		var decoded []FrontierSnapshot
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkFrontierRegistry(b *testing.B) *FrontierRegistry {
	b.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: 128})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		id := fmt.Sprintf("region-%03d", index)
		if err := registry.Register(id); err != nil {
			b.Fatal(err)
		}
		if err := registry.Advance(id, uint64(index), uint64(index+4)); err != nil {
			b.Fatal(err)
		}
	}
	return registry
}
