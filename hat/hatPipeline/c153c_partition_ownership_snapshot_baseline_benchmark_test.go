package hatPipeline

import (
	"encoding/json"
	"testing"
)

func c153cOwnershipBenchmarkFixture(b *testing.B) *QueuePartitionOwnership {
	b.Helper()
	owners := make([]string, 256)
	for partition := range owners {
		if partition%2 == 0 {
			owners[partition] = "node-a"
		} else {
			owners[partition] = "node-b"
		}
	}
	ownership, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{
		PartitionCount: len(owners),
		InitialOwners:  owners,
	})
	if err != nil {
		b.Fatalf("NewQueuePartitionOwnership() error = %v", err)
	}
	return ownership
}

func BenchmarkC153cOwnershipJSONSnapshotBaseline(b *testing.B) {
	ownership := c153cOwnershipBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := json.Marshal(ownership.Assignments())
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
}

func BenchmarkC153cOwnershipBinarySnapshot(b *testing.B) {
	ownership := c153cOwnershipBenchmarkFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := ownership.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(payload)))
	}
}

func BenchmarkC153cOwnershipJSONRestoreBaseline(b *testing.B) {
	ownership := c153cOwnershipBenchmarkFixture(b)
	payload, err := json.Marshal(ownership.Assignments())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var assignments []QueuePartitionAssignment
		if err := json.Unmarshal(payload, &assignments); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC153cOwnershipBinaryRestore(b *testing.B) {
	ownership := c153cOwnershipBenchmarkFixture(b)
	payload, err := ownership.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	target, err := NewQueuePartitionOwnership(QueuePartitionOwnershipOptions{PartitionCount: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := target.RestoreSnapshot(payload); err != nil {
			b.Fatal(err)
		}
	}
}
