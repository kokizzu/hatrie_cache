package hatReplication

import (
	"sync/atomic"
	"testing"
)

var tu09BaselineSequenceSink uint64
var tu09BootstrapStateSink SnapshotWALBootstrapState

func BenchmarkTU09BaselineSequenceAdmission(b *testing.B) {
	applied := uint64(100)
	target := uint64(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if applied < target {
			applied++
		}
		atomic.StoreUint64(&tu09BaselineSequenceSink, applied)
	}
}

func BenchmarkTU09BootstrapAdvanceWAL(b *testing.B) {
	coordinator := newTU09BenchmarkCoordinator(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		state, err := coordinator.AdvanceWAL(100, 9)
		if err != nil {
			b.Fatal(err)
		}
		tu09BootstrapStateSink = state
	}
}

func BenchmarkTU09BootstrapSnapshot(b *testing.B) {
	coordinator := newTU09BenchmarkCoordinator(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tu09BootstrapStateSink = coordinator.Snapshot()
	}
}

func newTU09BenchmarkCoordinator(b testing.TB) *SnapshotWALBootstrapCoordinator {
	b.Helper()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Begin(SnapshotWALBootstrapPlan{
		JoinerID:                "node-b",
		SourceID:                "node-a",
		SnapshotID:              "snapshot-1",
		StorageGeneration:       1,
		SnapshotJournalSequence: 100,
		TargetJournalSequence:   101,
		FencingToken:            9,
	}); err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.InstallSnapshot("snapshot-1", 1, 100, 9); err != nil {
		b.Fatal(err)
	}
	return coordinator
}
