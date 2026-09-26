package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

var m032SnapshotProviderSink []hatSql.Row
var m032SnapshotProviderResolver hatSql.SQLSourceResolver

func BenchmarkM032MultiSourceSnapshotLiveResolveBaseline(b *testing.B) {
	coordinator := m032SnapshotProviderCoordinator(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := coordinator.ResolveSQLSource("CDC", "orders")
		if err != nil {
			b.Fatal(err)
		}
		m032SnapshotProviderSink = rows
	}
}

func BenchmarkM032MultiSourceSnapshotBeginAndResolve(b *testing.B) {
	coordinator := m032SnapshotProviderCoordinator(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		resolver, release, err := coordinator.BeginSQLSnapshot(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		rows, err := resolver.ResolveSQLSource("CDC", "orders")
		release()
		if err != nil {
			b.Fatal(err)
		}
		m032SnapshotProviderSink = rows
	}
}

func BenchmarkM032MultiSourceSnapshotPinnedResolve(b *testing.B) {
	coordinator := m032SnapshotProviderCoordinator(b)
	resolver, release, err := coordinator.BeginSQLSnapshot(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer release()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := resolver.ResolveSQLSource("CDC", "orders")
		if err != nil {
			b.Fatal(err)
		}
		m032SnapshotProviderSink = rows
	}
}

func m032SnapshotProviderCoordinator(b *testing.B) *hatSql.SQLMultiSourceSnapshotCoordinator {
	b.Helper()
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		b.Fatal(err)
	}
	requests := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "orders-source", Key: "orders", Kind: "CDC", Provider: mU04Provider("orders-source", "orders", "orders-1")},
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), requests, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
		b.Fatal(err)
	}
	return coordinator
}
