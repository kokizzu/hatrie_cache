package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLSnapshotReadinessWaitsForAllObjectsAndPublishesConsistentSnapshot(t *testing.T) {
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []string{"orders", "customers"} {
		if err := registry.RegisterObject(object); err != nil {
			t.Fatalf("RegisterObject(%q): %v", object, err)
		}
	}
	if err := registry.RegisterDependent("dashboard", []string{"customers", "orders"}); err != nil {
		t.Fatal(err)
	}
	initial, err := registry.Snapshot("dashboard")
	if err != nil {
		t.Fatal(err)
	}
	if initial.Ready || !reflect.DeepEqual(initial.BlockedBy, []string{"customers", "orders"}) {
		t.Fatalf("initial snapshot = %#v", initial)
	}

	if err := registry.Advance("orders", 10); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkReady("orders", 10); err != nil {
		t.Fatal(err)
	}
	waitResult := make(chan struct {
		snapshot hatSql.SQLSnapshotReadinessSnapshot
		err      error
	}, 1)
	go func() {
		snapshot, err := registry.Wait(context.Background(), "dashboard")
		waitResult <- struct {
			snapshot hatSql.SQLSnapshotReadinessSnapshot
			err      error
		}{snapshot: snapshot, err: err}
	}()
	select {
	case result := <-waitResult:
		t.Fatalf("wait completed before every object was ready: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}

	if err := registry.MarkReady("customers", 20); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-waitResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !result.snapshot.Ready || len(result.snapshot.BlockedBy) != 0 {
			t.Fatalf("ready snapshot = %#v", result.snapshot)
		}
		if result.snapshot.Generation <= initial.Generation {
			t.Fatalf("generation = %d, initial = %d", result.snapshot.Generation, initial.Generation)
		}
		for _, object := range result.snapshot.Objects {
			if object.State != hatSql.SQLSnapshotReadinessStateReady {
				t.Fatalf("object %q state = %q", object.ID, object.State)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for readiness")
	}
}

func TestSQLSnapshotReadinessCancellationAndFailureAreQueryable(t *testing.T) {
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterObject("source"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDependent("query", []string{"source"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Wait(ctx, "query"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error = %v", err)
	}
	if err := registry.Cancel("source"); err != nil {
		t.Fatal(err)
	}
	canceled, err := registry.Wait(context.Background(), "query")
	if !errors.Is(err, hatSql.ErrSQLSnapshotReadinessCanceled) {
		t.Fatalf("object cancellation error = %v", err)
	}
	if canceled.Ready || len(canceled.BlockedBy) != 1 || canceled.Objects[0].State != hatSql.SQLSnapshotReadinessStateCanceled {
		t.Fatalf("canceled snapshot = %#v", canceled)
	}

	failedRegistry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := failedRegistry.RegisterObject("source"); err != nil {
		t.Fatal(err)
	}
	if err := failedRegistry.RegisterDependent("query", []string{"source"}); err != nil {
		t.Fatal(err)
	}
	if err := failedRegistry.Fail("source", "schema_mismatch"); err != nil {
		t.Fatal(err)
	}
	failed, err := failedRegistry.Wait(context.Background(), "query")
	if !errors.Is(err, hatSql.ErrSQLSnapshotReadinessFailed) {
		t.Fatalf("object failure error = %v", err)
	}
	if failed.Objects[0].ErrorCode != "schema_mismatch" || failed.Objects[0].State != hatSql.SQLSnapshotReadinessStateFailed {
		t.Fatalf("failed snapshot = %#v", failed)
	}
}

func TestSQLSnapshotReadinessWaitHonorsContextCancellationWhileBlocked(t *testing.T) {
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterObject("source"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDependent("query", []string{"source"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitResult := make(chan error, 1)
	go func() {
		_, err := registry.Wait(ctx, "query")
		waitResult <- err
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waitResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
}

func TestSQLSnapshotReadinessProgressIsMonotoneAndSnapshotsAreDetached(t *testing.T) {
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterObject("source"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDependent("query", []string{"source"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Advance("source", 10); err != nil {
		t.Fatal(err)
	}
	snapshot, err := registry.Snapshot("query")
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Objects[0].Progress = 999
	snapshot.BlockedBy[0] = "mutated"
	detached, err := registry.Snapshot("query")
	if err != nil {
		t.Fatal(err)
	}
	if detached.Objects[0].Progress != 10 || detached.BlockedBy[0] != "source" {
		t.Fatalf("snapshot was not detached: %#v", detached)
	}
	if err := registry.Advance("source", 9); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessProgressRegression) {
		t.Fatalf("progress regression error = %v", err)
	}
	if err := registry.MarkReady("source", 9); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessProgressRegression) {
		t.Fatalf("ready progress regression error = %v", err)
	}
	if err := registry.MarkReady("source", 10); err != nil {
		t.Fatal(err)
	}
	ready, err := registry.Snapshot("query")
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.Objects[0].Progress != 10 {
		t.Fatalf("ready snapshot = %#v", ready)
	}
}

func TestSQLSnapshotReadinessBoundsAndValidation(t *testing.T) {
	if _, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{MaxObjects: -1}); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessOptionsInvalid) {
		t.Fatalf("negative options error = %v", err)
	}
	registry, err := hatSql.NewSQLSnapshotReadinessRegistry(hatSql.SQLSnapshotReadinessRegistryOptions{
		MaxObjects:                  1,
		MaxDependents:               1,
		MaxDependenciesPerDependent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterObject("source"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterObject("other"); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessObjectLimit) {
		t.Fatalf("object bound error = %v", err)
	}
	if err := registry.RegisterDependent("query", []string{"missing"}); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessUnknownObject) {
		t.Fatalf("unknown dependency error = %v", err)
	}
	if err := registry.RegisterDependent("query", []string{"source", "source"}); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessDependencyLimit) {
		t.Fatalf("dependency bound error = %v", err)
	}
	if err := registry.RegisterDependent("query", []string{"source"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterDependent("query", []string{"source"}); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessDuplicateDependent) {
		t.Fatalf("duplicate dependent error = %v", err)
	}
	if _, err := registry.Snapshot("unknown"); !errors.Is(err, hatSql.ErrSQLSnapshotReadinessUnknownDependent) {
		t.Fatalf("unknown dependent error = %v", err)
	}
}
