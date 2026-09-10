package hatSql

import (
	"context"
	"testing"
)

type frontierSnapshotBenchmarkResolver struct{}

var frontierSnapshotBenchmarkSink SQLSourceResolver

func (frontierSnapshotBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (frontierSnapshotBenchmarkResolver) BeginSQLSnapshot(context.Context) (SQLSourceResolver, func(), error) {
	return frontierSnapshotBenchmarkResolver{}, nil, nil
}

func (frontierSnapshotBenchmarkResolver) BeginSQLSnapshotAt(context.Context, uint64) (SQLSourceResolver, func(), error) {
	return frontierSnapshotBenchmarkResolver{}, nil, nil
}

func BenchmarkSQLFrontierSnapshotProviderBaseline(b *testing.B) {
	var resolver SQLSourceResolver = frontierSnapshotBenchmarkResolver{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, release, err := beginSQLSnapshot(ctx, resolver)
		if err != nil {
			b.Fatal(err)
		}
		frontierSnapshotBenchmarkSink = snapshot
		if release != nil {
			release()
		}
	}
}

func BenchmarkSQLFrontierSnapshotProvider(b *testing.B) {
	tracker, err := NewSQLSourceFrontierTracker([]SQLSourceFrontierPartition{{
		Source:    "source",
		Partition: "0",
	}})
	if err != nil {
		b.Fatal(err)
	}
	barrier, err := NewSQLSourceFrontierBarrier(tracker)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "source", Partition: "0", Frontier: 1}); err != nil {
		b.Fatal(err)
	}
	resolver := frontierSnapshotBenchmarkResolver{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snapshot, release, err := BeginSQLFrontierSnapshot(ctx, resolver, barrier, 1)
		if err != nil {
			b.Fatal(err)
		}
		frontierSnapshotBenchmarkSink = snapshot
		if release != nil {
			release()
		}
	}
}
