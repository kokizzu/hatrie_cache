package hatPipeline_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

var mu035SnapshotStatusSink hatPipeline.SnapshotCutoverStatus

func newMU035CommittedSnapshot(b testing.TB) (*hatPipeline.SnapshotCutoverCoordinator, string) {
	b.Helper()
	coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(hatPipeline.SnapshotCutoverOptions{MaxCutovers: 2, MaxSources: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{ID: "ready", Timestamp: 1, Sources: []hatPipeline.SnapshotCutoverSource{{ID: "source", Generation: 1}}}); err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Acknowledge("ready", hatPipeline.SnapshotCutoverAcknowledgement{SourceID: "source", Generation: 1, Lower: 1, Upper: 1}); err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.Commit("ready"); err != nil {
		b.Fatal(err)
	}
	return coordinator, "ready"
}

func BenchmarkMU035SnapshotCutoverStatusReady(b *testing.B) {
	coordinator, id := newMU035CommittedSnapshot(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, ok := coordinator.Status(id)
		if !ok {
			b.Fatal("Status() did not find committed snapshot")
		}
		mu035SnapshotStatusSink = status
	}
}

func BenchmarkMU035SnapshotCutoverWaitReady(b *testing.B) {
	coordinator, id := newMU035CommittedSnapshot(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, err := coordinator.Wait(context.Background(), id)
		if err != nil {
			b.Fatal(err)
		}
		mu035SnapshotStatusSink = status
	}
}
