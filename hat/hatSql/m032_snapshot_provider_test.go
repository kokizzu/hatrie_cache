package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM032MultiSourceSnapshotCoordinatorPinsPublishedGenerationForSQL(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	requests := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "orders-source", Key: "orders", Kind: "CDC", Provider: mU04Provider("orders-source", "orders", "orders-1")},
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), requests, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
		t.Fatal(err)
	}

	pinned, release, err := coordinator.BeginSQLSnapshot(context.Background())
	if err != nil {
		t.Fatalf("BeginSQLSnapshot() error = %v", err)
	}
	if release == nil {
		t.Fatal("BeginSQLSnapshot() release = nil")
	}
	defer release()
	pinnedView, ok := pinned.(*hatSql.SQLMultiSourceSnapshotView)
	if !ok {
		t.Fatalf("BeginSQLSnapshot() resolver = %T, want *SQLMultiSourceSnapshotView", pinned)
	}
	if got := pinnedView.Generation(); got != 1 {
		t.Fatalf("pinned generation = %d, want 1", got)
	}

	updated := []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "orders-source", Key: "orders", Kind: "CDC", Provider: mU04Provider("orders-source", "orders", "orders-2")},
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), updated, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{RequireSnapshotIDs: true}); err != nil {
		t.Fatal(err)
	}
	if got := coordinator.View().Generation(); got != 2 {
		t.Fatalf("current generation = %d, want 2", got)
	}
	if got := pinnedView.Sources()[0].Metadata.SnapshotID; got != "orders-1" {
		t.Fatalf("pinned snapshot ID = %q, want orders-1", got)
	}
	if got := coordinator.View().Sources()[0].Metadata.SnapshotID; got != "orders-2" {
		t.Fatalf("current snapshot ID = %q, want orders-2", got)
	}
}

func TestM032MultiSourceSnapshotCoordinatorBeginRejectsUnavailableOrCanceled(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	if resolver, release, err := coordinator.BeginSQLSnapshot(context.Background()); !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotUnavailable) || resolver != nil || release != nil {
		t.Fatalf("unpublished BeginSQLSnapshot() = %T, release-nil=%t, %v; want unavailable", resolver, release == nil, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if resolver, release, err := coordinator.BeginSQLSnapshot(canceled); !errors.Is(err, context.Canceled) || resolver != nil || release != nil {
		t.Fatalf("canceled BeginSQLSnapshot() = %T, release-nil=%t, %v; want context.Canceled", resolver, release == nil, err)
	}
	var nilCoordinator *hatSql.SQLMultiSourceSnapshotCoordinator
	if resolver, release, err := nilCoordinator.BeginSQLSnapshot(context.Background()); !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotNil) || resolver != nil || release != nil {
		t.Fatalf("nil BeginSQLSnapshot() = %T, release-nil=%t, %v; want nil error", resolver, release == nil, err)
	}
}
