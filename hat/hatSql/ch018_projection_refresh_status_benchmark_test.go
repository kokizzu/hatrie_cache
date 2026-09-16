package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var ch018ProjectionRefreshStatusSink hatSql.ProjectionRefreshStatus
var ch018ProjectionCheckpointSink uint64

func BenchmarkCH018ProjectionRefreshStatus(b *testing.B) {
	views, resolver := newIncrementalProjectionBenchmarkViews(b)
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "events", Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ch018ProjectionRefreshStatusSink = runner.Status()
	}
}

func BenchmarkCH018ProjectionCheckpointControl(b *testing.B) {
	views, resolver := newIncrementalProjectionBenchmarkViews(b)
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "events", Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		ch018ProjectionCheckpointSink = runner.Checkpoint()
	}
}

func BenchmarkCH018ProjectionRefreshStatusApplyControl(b *testing.B) {
	views, resolver := newIncrementalProjectionBenchmarkViews(b)
	runner, err := hatSql.NewIncrementalProjectionRunner(views, resolver, hatSql.QueryOptions{}, hatSql.IncrementalProjectionRunnerOptions{Name: "events", Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	changes := []hatSql.ProjectionChange{{Sequence: 1, Dependency: "events"}}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		changes[0].Sequence = uint64(iteration + 1)
		if _, err := runner.Apply(context.Background(), changes); err != nil {
			b.Fatal(err)
		}
	}
}
