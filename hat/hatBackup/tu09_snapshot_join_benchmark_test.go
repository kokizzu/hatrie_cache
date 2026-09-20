package hatBackup

import (
	"context"
	"testing"
)

func BenchmarkSnapshotJoinWALBatch(b *testing.B) {
	manifest := snapshotJoinTestManifest()
	batch := []SnapshotJoinRecord{{Sequence: 11, Payload: []byte("one")}, {Sequence: 12, Payload: []byte("two")}}
	b.ReportAllocs()
	for range b.N {
		bootstrap, err := NewSnapshotJoinBootstrap(manifest, SnapshotJoinOptions{MaxBatchRecords: 2})
		if err != nil {
			b.Fatal(err)
		}
		if err := bootstrap.ApplySnapshot(context.Background(), manifest.SnapshotChecksum, func() error { return nil }); err != nil {
			b.Fatal(err)
		}
		if err := bootstrap.ApplyWAL(context.Background(), batch, func([]SnapshotJoinRecord) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSnapshotJoinCheckpoint(b *testing.B) {
	checkpoint := SnapshotJoinCheckpoint{
		Manifest:        snapshotJoinTestManifest(),
		Phase:           SnapshotJoinPhaseSnapshotApplied,
		AppliedSequence: 11,
	}
	b.ReportAllocs()
	for range b.N {
		encoded, err := checkpoint.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := UnmarshalSnapshotJoinCheckpoint(encoded); err != nil {
			b.Fatal(err)
		}
	}
}
