package hatMetrics

import (
	"fmt"
	"testing"
)

func benchmarkFrontierNames() []string {
	names := make([]string, 128)
	for i := range names {
		names[i] = fmt.Sprintf("operator-%03d", i)
	}
	return names
}

func BenchmarkMZ043SourceFrontierRegistryAdvance(b *testing.B) {
	names := benchmarkFrontierNames()
	registry := NewSourceFrontierRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Advance(names[i%len(names)], uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ043OperatorFrontierRegistryAdvance(b *testing.B) {
	names := benchmarkFrontierNames()
	registry := NewOperatorFrontierRegistry()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := registry.Advance(names[i%len(names)], uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMZ043SourceFrontierRegistrySnapshot(b *testing.B) {
	names := benchmarkFrontierNames()
	registry := NewSourceFrontierRegistry()
	for i, name := range names {
		if err := registry.Advance(name, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.Snapshot(uint64(len(names) + i))
	}
}

func BenchmarkMZ043OperatorFrontierRegistrySnapshot(b *testing.B) {
	names := benchmarkFrontierNames()
	registry := NewOperatorFrontierRegistry()
	for i, name := range names {
		if err := registry.Advance(name, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = registry.Snapshot(uint64(len(names) + i))
	}
}
