package hatReplication

import "testing"

var joinBootstrapBenchmarkSink uint64

func BenchmarkJoinBootstrapLegacy(b *testing.B) {
	var sink uint64
	for i := 0; i < b.N; i++ {
		snapshotSequence := uint64(i + 7)
		appliedSequence := snapshotSequence + 2
		fencingToken := snapshotSequence + 4
		phase := 0
		appliedThrough := uint64(0)
		if snapshotSequence != uint64(i+7) {
			b.Fatal("snapshot sequence mismatch")
		}
		phase = 1
		appliedThrough = snapshotSequence
		if appliedThrough < snapshotSequence || appliedSequence < appliedThrough {
			b.Fatal("journal catch-up regressed")
		}
		phase = 2
		appliedThrough = appliedSequence
		if phase != 2 || fencingToken != snapshotSequence+4 {
			b.Fatal("activation fence mismatch")
		}
		phase = 3
		if phase != 3 || appliedThrough != appliedSequence {
			b.Fatal("legacy join bootstrap failed")
		}
		sink += appliedThrough + fencingToken + uint64(phase)
	}
	joinBootstrapBenchmarkSink = sink
}

func BenchmarkJoinBootstrapValidated(b *testing.B) {
	var sink uint64
	for i := 0; i < b.N; i++ {
		snapshotSequence := uint64(i + 7)
		appliedSequence := snapshotSequence + 2
		fencingToken := snapshotSequence + 4
		bootstrap, err := NewJoinBootstrap(JoinBootstrapOptions{
			SourceNode:       "source",
			TargetNode:       "target",
			SnapshotSequence: snapshotSequence,
			FencingToken:     fencingToken,
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := bootstrap.SnapshotInstalled(snapshotSequence); err != nil {
			b.Fatal(err)
		}
		if err := bootstrap.CatchUp(appliedSequence); err != nil {
			b.Fatal(err)
		}
		state, err := bootstrap.Activate(appliedSequence, fencingToken)
		if err != nil {
			b.Fatal(err)
		}
		sink += state.AppliedThrough + state.FencingToken + uint64(len(state.SourceNode))
	}
	joinBootstrapBenchmarkSink = sink
}
