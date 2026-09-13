package hatPipeline_test

import (
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

var mz04FrontierRetentionSnapshotSink hatPipeline.FrontierRetentionSnapshot

func BenchmarkMZ04FrontierRetentionSnapshot(b *testing.B) {
	frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = frontiers.Close() })
	if err := frontiers.Register("events"); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 100, 100); err != nil {
		b.Fatal(err)
	}
	retention, err := hatPipeline.NewFrontierRetentionRegistry(frontiers, hatPipeline.FrontierRetentionOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = retention.Close() })
	if _, err := retention.Acquire("events", 100); err != nil {
		b.Fatal(err)
	}
	if err := frontiers.Advance("events", 120, 130); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for range b.N {
		snapshot, err := retention.Snapshot("events")
		if err != nil {
			b.Fatal(err)
		}
		mz04FrontierRetentionSnapshotSink = snapshot
	}
}
