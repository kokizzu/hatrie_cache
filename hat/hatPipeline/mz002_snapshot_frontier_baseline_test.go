package hatPipeline

import (
	"context"
	"testing"
)

func benchmarkMZ002ManualSnapshot(b *testing.B, sourceCount int) {
	b.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: sourceCount})
	if err != nil {
		b.Fatal(err)
	}
	defer registry.Close()

	const target uint64 = 100
	ids := make([]string, sourceCount)
	for index := range ids {
		ids[index] = "source-" + formatMZ002Index(index)
		if err := registry.Register(ids[index]); err != nil {
			b.Fatal(err)
		}
		if err := registry.Advance(ids[index], target, target); err != nil {
			b.Fatal(err)
		}
	}

	ctx := context.Background()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		for _, id := range ids {
			if err := registry.WaitUntil(ctx, id, target); err != nil {
				b.Fatal(err)
			}
		}
		snapshots := make([]FrontierSnapshot, 0, len(ids))
		for _, id := range ids {
			snapshot, ok := registry.Snapshot(id)
			if !ok {
				b.Fatalf("frontier %q disappeared", id)
			}
			snapshots = append(snapshots, snapshot)
		}
		if len(snapshots) != sourceCount {
			b.Fatalf("got %d snapshots, want %d", len(snapshots), sourceCount)
		}
	}
}

func formatMZ002Index(index int) string {
	const digits = "0123456789"
	if index == 0 {
		return "0"
	}
	var reversed [20]byte
	position := len(reversed)
	for index > 0 {
		position--
		reversed[position] = digits[index%10]
		index /= 10
	}
	return string(reversed[position:])
}

func BenchmarkMZ002ManualSnapshot1(b *testing.B) {
	benchmarkMZ002ManualSnapshot(b, 1)
}

func BenchmarkMZ002ManualSnapshot8(b *testing.B) {
	benchmarkMZ002ManualSnapshot(b, 8)
}

func BenchmarkMZ002ManualSnapshot64(b *testing.B) {
	benchmarkMZ002ManualSnapshot(b, 64)
}

func benchmarkMZ002SnapshotGate(b *testing.B, sourceCount int) {
	b.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: sourceCount})
	if err != nil {
		b.Fatal(err)
	}
	defer registry.Close()

	const target uint64 = 100
	ids := make([]string, sourceCount)
	for index := range ids {
		ids[index] = "source-" + formatMZ002Index(index)
		if err := registry.Register(ids[index]); err != nil {
			b.Fatal(err)
		}
		if err := registry.Advance(ids[index], target, target); err != nil {
			b.Fatal(err)
		}
	}
	gate, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: ids,
	})
	if err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		snapshot, err := gate.WaitUntil(ctx, target)
		if err != nil {
			b.Fatal(err)
		}
		if len(snapshot.Sources) != sourceCount {
			b.Fatalf("got %d snapshots, want %d", len(snapshot.Sources), sourceCount)
		}
	}
}

func BenchmarkMZ002SnapshotGate1(b *testing.B) {
	benchmarkMZ002SnapshotGate(b, 1)
}

func BenchmarkMZ002SnapshotGate8(b *testing.B) {
	benchmarkMZ002SnapshotGate(b, 8)
}

func BenchmarkMZ002SnapshotGate64(b *testing.B) {
	benchmarkMZ002SnapshotGate(b, 64)
}

func benchmarkMZ002SnapshotGateConstructor(b *testing.B, sourceCount int) {
	b.Helper()
	registry, err := NewFrontierRegistry(FrontierRegistryOptions{MaxObjects: sourceCount})
	if err != nil {
		b.Fatal(err)
	}
	defer registry.Close()

	ids := make([]string, sourceCount)
	for index := range ids {
		ids[index] = "source-" + formatMZ002Index(index)
		if err := registry.Register(ids[index]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		gate, err := NewSnapshotFrontierGate(SnapshotFrontierGateOptions{
			Registry:  registry,
			SourceIDs: ids,
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(gate.sourceIDs) != sourceCount {
			b.Fatalf("got %d source IDs, want %d", len(gate.sourceIDs), sourceCount)
		}
	}
}

func BenchmarkMZ002SnapshotGateConstructor1(b *testing.B) {
	benchmarkMZ002SnapshotGateConstructor(b, 1)
}

func BenchmarkMZ002SnapshotGateConstructor8(b *testing.B) {
	benchmarkMZ002SnapshotGateConstructor(b, 8)
}

func BenchmarkMZ002SnapshotGateConstructor64(b *testing.B) {
	benchmarkMZ002SnapshotGateConstructor(b, 64)
}
