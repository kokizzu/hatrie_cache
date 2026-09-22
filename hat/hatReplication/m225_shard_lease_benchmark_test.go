package hatReplication

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func BenchmarkShardLeaseAcquireRelease(b *testing.B) {
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 1})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		lease, err := registry.Acquire("region-a", "node-a", time.Minute, now)
		if err != nil {
			b.Fatal(err)
		}
		if err := registry.Release(lease, now); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardLeaseSnapshotMarshalBinary(b *testing.B) {
	snapshot := benchmarkShardLeaseSnapshot(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(mustMarshalShardLeaseBinary(b, snapshot))))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := snapshot.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardLeaseSnapshotMarshalJSONControl(b *testing.B) {
	snapshot := benchmarkShardLeaseSnapshot(b)
	encoded := mustMarshalShardLeaseJSON(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardLeaseSnapshotUnmarshalBinary(b *testing.B) {
	snapshot := benchmarkShardLeaseSnapshot(b)
	encoded := mustMarshalShardLeaseBinary(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded ShardLeaseRegistrySnapshot
		if err := decoded.UnmarshalBinary(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardLeaseSnapshotUnmarshalJSONControl(b *testing.B) {
	snapshot := benchmarkShardLeaseSnapshot(b)
	encoded := mustMarshalShardLeaseJSON(b, snapshot)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded ShardLeaseRegistrySnapshot
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func TestShardLeaseSnapshotBinaryIsSmallerThanJSON(t *testing.T) {
	snapshot := benchmarkShardLeaseSnapshot(t)
	binarySnapshot := mustMarshalShardLeaseBinary(t, snapshot)
	jsonSnapshot := mustMarshalShardLeaseJSON(t, snapshot)
	if len(binarySnapshot) >= len(jsonSnapshot) {
		t.Fatalf("binary snapshot should be smaller: binary=%d json=%d", len(binarySnapshot), len(jsonSnapshot))
	}
	t.Logf("snapshot bytes: binary=%d json=%d ratio=%.2fx", len(binarySnapshot), len(jsonSnapshot), float64(len(jsonSnapshot))/float64(len(binarySnapshot)))
}

func benchmarkShardLeaseSnapshot(tb testing.TB) ShardLeaseRegistrySnapshot {
	tb.Helper()
	registry, err := NewShardLeaseRegistry(ShardLeaseRegistryOptions{MaxLeases: 256})
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	for index := 0; index < 256; index++ {
		if _, err := registry.Acquire(fmt.Sprintf("region-%03d", index), "node-a", time.Hour, now); err != nil {
			tb.Fatal(err)
		}
	}
	return registry.Snapshot()
}

func mustMarshalShardLeaseBinary(tb testing.TB, snapshot ShardLeaseRegistrySnapshot) []byte {
	tb.Helper()
	encoded, err := snapshot.MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}

func mustMarshalShardLeaseJSON(tb testing.TB, snapshot ShardLeaseRegistrySnapshot) []byte {
	tb.Helper()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		tb.Fatal(err)
	}
	return encoded
}
