package hatTopology_test

import (
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func BenchmarkM226DurablePartitionOwnershipCommit(b *testing.B) {
	path := filepath.Join(b.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		b.Fatal(err)
	}
	decision := m226OwnershipDecision(11, "node-a", "topology-1")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := store.Commit(uint64(index+1), uint64(index+1), decision); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM226DurablePartitionOwnershipSnapshot(b *testing.B) {
	path := filepath.Join(b.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := store.Commit(1, 1, m226OwnershipDecision(11, "node-a", "topology-1")); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, ok := store.Snapshot(); !ok {
			b.Fatal("Snapshot() returned no record")
		}
	}
}

func BenchmarkM226DurablePartitionOwnershipOpen(b *testing.B) {
	path := filepath.Join(b.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := store.Commit(1, 1, m226OwnershipDecision(11, "node-a", "topology-1")); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		opened, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
		if err != nil {
			b.Fatal(err)
		}
		if _, ok := opened.Snapshot(); !ok {
			b.Fatal("reopened Snapshot() returned no record")
		}
	}
}
