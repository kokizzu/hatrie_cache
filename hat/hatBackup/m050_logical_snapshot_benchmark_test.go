package hatBackup

import (
	"testing"
	"time"
)

var m050ManifestBenchmarkSink uint64

func BenchmarkM050MarshalAndPlan(b *testing.B) {
	manifest := LogicalSnapshotManifest{
		Version:           LogicalSnapshotManifestVersion,
		SnapshotID:        "snapshot-benchmark",
		CreatedAt:         time.Unix(1_700_000_000, 0).UTC(),
		BundleBackupID:    "backup-benchmark",
		StorageGeneration: 7,
		JournalSequence:   100,
		SourceOffsets: []SourceOffsetCheckpoint{
			{SourceID: "orders", Partition: "0", Offset: 91, Epoch: 3},
			{SourceID: "orders", Partition: "1", Offset: 92, Epoch: 3},
		},
		Frontiers: []FrontierCheckpoint{
			{ID: "orders", Lower: 98, Upper: 100, Generation: 8},
			{ID: "users", Lower: 99, Upper: 100, Generation: 9},
		},
		Subscriptions: []SubscriptionCheckpoint{
			{ID: "orders-sub", FrontierID: "orders", AsOf: 98, AckedSequence: 100},
		},
	}
	coverage := LogicalSnapshotJournalCoverage{HasEntries: true, FirstSequence: 101, LastSequence: 104}
	b.ReportAllocs()
	encoded, err := manifest.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := manifest.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		plan, err := PlanLogicalSnapshotRestore(manifest, coverage)
		if err != nil {
			b.Fatal(err)
		}
		m050ManifestBenchmarkSink ^= uint64(len(encoded)) + uint64(len(plan.Steps))
	}
	b.StopTimer()
	b.ReportMetric(float64(len(encoded)), "bytes/manifest")
}
