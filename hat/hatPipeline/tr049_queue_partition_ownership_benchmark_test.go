package hatPipeline

import "testing"

var tr049OwnerBenchmarkSink string

func BenchmarkTR049BaselineSliceOwnerLookup(b *testing.B) {
	owners := make([]string, 64)
	for index := range owners {
		owners[index] = "node-a"
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tr049OwnerBenchmarkSink = owners[index&(len(owners)-1)]
	}
}

func BenchmarkTR049OwnershipOwnerLookup(b *testing.B) {
	owners := make([]string, 64)
	for index := range owners {
		owners[index] = "node-a"
	}
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 64,
		InitialOwners:  owners,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		owner, err := ownership.Owner(index & 63)
		if err != nil {
			b.Fatal(err)
		}
		tr049OwnerBenchmarkSink = owner
	}
}

func BenchmarkTR049OwnershipSnapshotOwnerLookup(b *testing.B) {
	owners := make([]string, 64)
	for index := range owners {
		owners[index] = "node-a"
	}
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 64,
		InitialOwners:  owners,
	})
	if err != nil {
		b.Fatal(err)
	}
	view := ownership.Snapshot()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		owner, err := view.Owner(index & 63)
		if err != nil {
			b.Fatal(err)
		}
		tr049OwnerBenchmarkSink = owner
	}
}

func BenchmarkTR049OwnershipSnapshotOwnerUncheckedLookup(b *testing.B) {
	owners := make([]string, 64)
	for index := range owners {
		owners[index] = "node-a"
	}
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 64,
		InitialOwners:  owners,
	})
	if err != nil {
		b.Fatal(err)
	}
	view := ownership.Snapshot()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tr049OwnerBenchmarkSink = view.OwnerUnchecked(index & 63)
	}
}

func BenchmarkTR049OwnershipMigration(b *testing.B) {
	owners := make([]string, 64)
	for index := range owners {
		owners[index] = "node-a"
	}
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: 64,
		InitialOwners:  owners,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		partition := index & 63
		source, err := ownership.Owner(partition)
		if err != nil {
			b.Fatal(err)
		}
		target := "node-a"
		if source == target {
			target = "node-b"
		}
		migration, err := ownership.BeginMigration(partition, target, uint64(index+1))
		if err != nil {
			b.Fatal(err)
		}
		if _, err := ownership.AcknowledgeCatchUp(migration, migration.Fence); err != nil {
			b.Fatal(err)
		}
		if _, err := ownership.Cutover(migration); err != nil {
			b.Fatal(err)
		}
	}
}
