package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLMultiSourceSnapshotReadiness(b *testing.B) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
	}, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.Run("baseline_view", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if coordinator.View() == nil {
				b.Fatal("View returned nil")
			}
		}
	})
	b.Run("wait_ready_fast_path", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := coordinator.WaitReady(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}
