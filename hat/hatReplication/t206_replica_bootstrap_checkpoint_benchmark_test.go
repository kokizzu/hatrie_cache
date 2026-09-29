package hatReplication

import (
	"path/filepath"
	"testing"
)

var t206BootstrapStateSink SnapshotWALBootstrapState
var t206BootstrapPayloadSink []byte

func BenchmarkT206BootstrapMarshalSnapshot(b *testing.B) {
	coordinator := newT206BenchmarkCoordinator(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payload, err := coordinator.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		t206BootstrapPayloadSink = payload
	}
	if len(t206BootstrapPayloadSink) == 0 {
		b.Fatal("MarshalSnapshot() returned an empty checkpoint")
	}
	b.ReportMetric(float64(len(t206BootstrapPayloadSink)), "wire-bytes/op")
}

func BenchmarkT206BootstrapRestoreSnapshot(b *testing.B) {
	coordinator := newT206BenchmarkCoordinator(b)
	payload, err := coordinator.MarshalSnapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		restored, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := restored.RestoreSnapshot(payload); err != nil {
			b.Fatal(err)
		}
		t206BootstrapStateSink = restored.Snapshot()
	}
}

func BenchmarkT206BootstrapSave(b *testing.B) {
	coordinator := newT206BenchmarkCoordinator(b)
	path := filepath.Join(b.TempDir(), "bootstrap.checkpoint")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := coordinator.Save(path); err != nil {
			b.Fatal(err)
		}
	}
}

func newT206BenchmarkCoordinator(b testing.TB) *SnapshotWALBootstrapCoordinator {
	b.Helper()
	coordinator, err := NewSnapshotWALBootstrapCoordinator(SnapshotWALBootstrapOptions{})
	if err != nil {
		b.Fatal(err)
	}
	plan := t206BootstrapPlan()
	if _, err := coordinator.Begin(plan); err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.InstallSnapshot(plan.SnapshotID, plan.StorageGeneration, plan.SnapshotJournalSequence, plan.FencingToken); err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.AdvanceWAL(102, plan.FencingToken); err != nil {
		b.Fatal(err)
	}
	return coordinator
}
