package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLMultiSourceSnapshotCoordinatorWaitReadyBlocksUntilPublication(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		view *hatSql.SQLMultiSourceSnapshotView
		err  error
	}, 1)
	go func() {
		view, waitErr := coordinator.WaitReady(ctx)
		result <- struct {
			view *hatSql.SQLMultiSourceSnapshotView
			err  error
		}{view: view, err: waitErr}
	}()
	select {
	case got := <-result:
		t.Fatalf("WaitReady returned before publication: %#v", got)
	case <-time.After(10 * time.Millisecond):
	}

	store := &mU04SnapshotStore{}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
		{Source: "b-source", Key: "b", Kind: "CDC", Provider: mU04Provider("b-source", "b", "b-1")},
	}, store, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err != nil || got.view == nil || got.view.Generation() != 1 {
			t.Fatalf("WaitReady result = %#v, want generation 1 view", got)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not unblock after publication")
	}
}

func TestSQLMultiSourceSnapshotCoordinatorWaitReadyHonorsCancellationAndFastPath(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if view, err := coordinator.WaitReady(canceled); !errors.Is(err, context.Canceled) || view != nil {
		t.Fatalf("canceled WaitReady = %#v/%v, want nil/context.Canceled", view, err)
	}

	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
	}, &mU04SnapshotStore{}, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); err != nil {
		t.Fatal(err)
	}
	view, err := coordinator.WaitReady(context.Background())
	if err != nil || view == nil || view.Generation() != 1 {
		t.Fatalf("ready fast path = %#v/%v, want generation 1 view", view, err)
	}
}

func TestSQLMultiSourceSnapshotCoordinatorWaitReadyStaysBlockedAfterFailedCapture(t *testing.T) {
	coordinator, err := hatSql.NewSQLMultiSourceSnapshotCoordinator(hatSql.SQLMultiSourceSnapshotCoordinatorOptions{MaxSources: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.CaptureWithCheckpoint(context.Background(), []hatSql.SQLMultiSourceSnapshotRequest{
		{Source: "a-source", Key: "a", Kind: "CDC", Provider: mU04Provider("a-source", "a", "a-1")},
	}, &mU04SnapshotStore{commitErr: errors.New("disk full")}, hatSql.SQLMultiSourceSnapshotCaptureOptions{}); !errors.Is(err, hatSql.ErrSQLMultiSourceSnapshotCommit) {
		t.Fatalf("failed capture error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	view, err := coordinator.WaitReady(ctx)
	if view != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady after failed capture = %#v/%v, want timeout", view, err)
	}
}
