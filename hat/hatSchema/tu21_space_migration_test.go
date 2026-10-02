package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU21SpaceMigrationResumesAndControlsMixedVersions(t *testing.T) {
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{MaxPlans: 4, MaxSteps: 4})
	if err != nil {
		t.Fatalf("NewSpaceMigrationManager() error = %v", err)
	}
	plan := SpaceMigrationPlan{
		ID:                 "orders-v3",
		Space:              "orders",
		FromVersion:        1,
		CompatibleVersions: []uint64{1, 2},
		Steps: []SpaceMigrationStep{
			{ID: "add-status", TargetVersion: 2},
			{ID: "add-index", TargetVersion: 3},
		},
	}
	status, err := manager.Prepare(plan)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if status.State != SpaceMigrationPrepared || status.CurrentVersion != 1 || status.Generation == 0 {
		t.Fatalf("prepared status = %#v", status)
	}
	for _, version := range []uint64{1, 2} {
		allowed, err := manager.AllowsVersion("orders-v3", version)
		if err != nil || !allowed {
			t.Fatalf("AllowsVersion(%d) = %t/%v, want true", version, allowed, err)
		}
	}

	var checks int
	var applied []string
	err = manager.Run(context.Background(), "orders-v3", SpaceMigrationCallbacks{
		Check: func(_ context.Context, got SpaceMigrationPlan) error {
			checks++
			if got.Space != "orders" {
				t.Fatalf("precondition plan space = %q", got.Space)
			}
			return nil
		},
		Apply: func(_ context.Context, step SpaceMigrationStep) error {
			applied = append(applied, step.ID)
			if step.ID == "add-index" && len(applied) == 2 {
				return context.Canceled
			}
			return nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run() error = %v, want context.Canceled", err)
	}
	status, err = manager.Status("orders-v3")
	if err != nil {
		t.Fatalf("Status() after pause error = %v", err)
	}
	if status.State != SpaceMigrationPaused || status.CompletedSteps != 1 || status.CurrentVersion != 2 {
		t.Fatalf("paused status = %#v", status)
	}
	if checks != 1 || !reflect.DeepEqual(applied, []string{"add-status", "add-index"}) {
		t.Fatalf("callbacks = checks %d applied %#v", checks, applied)
	}

	err = manager.Run(context.Background(), "orders-v3", SpaceMigrationCallbacks{
		Check: func(context.Context, SpaceMigrationPlan) error { return nil },
		Apply: func(_ context.Context, step SpaceMigrationStep) error {
			applied = append(applied, step.ID)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("resumed Run() error = %v", err)
	}
	status, err = manager.Status("orders-v3")
	if err != nil {
		t.Fatalf("Status() after commit error = %v", err)
	}
	if status.State != SpaceMigrationCommitted || status.CompletedSteps != 2 || status.CurrentVersion != 3 {
		t.Fatalf("committed status = %#v", status)
	}
	if allowed, err := manager.AllowsVersion("orders-v3", 1); err != nil || allowed {
		t.Fatalf("committed old version allowed = %t/%v, want false", allowed, err)
	}
	if allowed, err := manager.AllowsVersion("orders-v3", 3); err != nil || !allowed {
		t.Fatalf("committed target version allowed = %t/%v, want true", allowed, err)
	}
}

func TestTU21SpaceMigrationSnapshotRestoreAndRollback(t *testing.T) {
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{})
	if err != nil {
		t.Fatalf("NewSpaceMigrationManager() error = %v", err)
	}
	_, err = manager.Prepare(SpaceMigrationPlan{
		ID:          "users-v2",
		Space:       "users",
		FromVersion: 4,
		Steps:       []SpaceMigrationStep{{ID: "add-email", TargetVersion: 5}},
	})
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := manager.Run(context.Background(), "users-v2", SpaceMigrationCallbacks{
		Apply: func(context.Context, SpaceMigrationStep) error { return nil },
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	encoded, err := manager.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot() error = %v", err)
	}
	restored, err := RestoreSpaceMigrationManager(encoded, SpaceMigrationManagerOptions{})
	if err != nil {
		t.Fatalf("RestoreSpaceMigrationManager() error = %v", err)
	}
	want, err := manager.Status("users-v2")
	if err != nil {
		t.Fatalf("original Status() error = %v", err)
	}
	got, err := restored.Status("users-v2")
	if err != nil {
		t.Fatalf("restored Status() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restored status = %#v, want %#v", got, want)
	}
	var rolledBack []string
	if err := restored.Rollback(context.Background(), "users-v2", SpaceMigrationCallbacks{
		Rollback: func(_ context.Context, step SpaceMigrationStep) error {
			rolledBack = append(rolledBack, step.ID)
			return nil
		},
	}); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	got, err = restored.Status("users-v2")
	if err != nil {
		t.Fatalf("Status() after rollback error = %v", err)
	}
	if got.State != SpaceMigrationRolledBack || got.CurrentVersion != 4 || !reflect.DeepEqual(rolledBack, []string{"add-email"}) {
		t.Fatalf("rolled back status = %#v, steps %#v", got, rolledBack)
	}
}

func TestTU21SpaceMigrationRejectsInvalidPlansAndCorruptSnapshots(t *testing.T) {
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{MaxPlans: 1, MaxSteps: 1})
	if err != nil {
		t.Fatalf("NewSpaceMigrationManager() error = %v", err)
	}
	invalid := []SpaceMigrationPlan{
		{ID: "", Space: "orders", FromVersion: 1, Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 2}}},
		{ID: "bad-space", Space: "", FromVersion: 1, Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 2}}},
		{ID: "bad-version", Space: "orders", FromVersion: 2, Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 2}}},
		{ID: "bad-step", Space: "orders", FromVersion: 1, Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 3}}},
	}
	for _, plan := range invalid {
		if _, err := manager.Prepare(plan); !errors.Is(err, ErrSpaceMigrationInvalid) {
			t.Fatalf("Prepare(%#v) error = %v, want ErrSpaceMigrationInvalid", plan, err)
		}
	}
	if _, err := manager.Prepare(SpaceMigrationPlan{
		ID: "orders-v2", Space: "orders", FromVersion: 1,
		Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 2}},
	}); err != nil {
		t.Fatalf("valid Prepare() error = %v", err)
	}
	if _, err := manager.Prepare(SpaceMigrationPlan{
		ID: "second", Space: "users", FromVersion: 1,
		Steps: []SpaceMigrationStep{{ID: "x", TargetVersion: 2}},
	}); !errors.Is(err, ErrSpaceMigrationLimit) {
		t.Fatalf("capacity Prepare() error = %v, want ErrSpaceMigrationLimit", err)
	}
	if _, err := RestoreSpaceMigrationManager([]byte("corrupt"), SpaceMigrationManagerOptions{}); !errors.Is(err, ErrSpaceMigrationSnapshotInvalid) {
		t.Fatalf("corrupt restore error = %v, want ErrSpaceMigrationSnapshotInvalid", err)
	}
}

func TestTU21SpaceMigrationResumesInterruptedRollback(t *testing.T) {
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Prepare(SpaceMigrationPlan{
		ID:          "orders-v3",
		Space:       "orders",
		FromVersion: 1,
		Steps: []SpaceMigrationStep{
			{ID: "add-status", TargetVersion: 2},
			{ID: "add-region", TargetVersion: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Run(context.Background(), "orders-v3", SpaceMigrationCallbacks{
		Apply: func(context.Context, SpaceMigrationStep) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	rollbackCalls := 0
	wantPause := errors.New("rollback paused")
	if err := manager.Rollback(context.Background(), "orders-v3", SpaceMigrationCallbacks{
		Rollback: func(context.Context, SpaceMigrationStep) error {
			rollbackCalls++
			if rollbackCalls == 1 {
				return wantPause
			}
			return nil
		},
	}); !errors.Is(err, wantPause) {
		t.Fatalf("first rollback error = %v, want %v", err, wantPause)
	}
	paused, err := manager.Status("orders-v3")
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != SpaceMigrationPaused || paused.CompletedSteps != 2 || paused.CurrentVersion != 3 {
		t.Fatalf("paused rollback status = %#v", paused)
	}
	if err := manager.Rollback(context.Background(), "orders-v3", SpaceMigrationCallbacks{
		Rollback: func(context.Context, SpaceMigrationStep) error {
			rollbackCalls++
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := manager.Status("orders-v3")
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack.State != SpaceMigrationRolledBack || rolledBack.CompletedSteps != 0 || rolledBack.CurrentVersion != 1 {
		t.Fatalf("rolled back status = %#v", rolledBack)
	}
}

func TestTU21SpaceMigrationRejectsCompatibilityBeyondTarget(t *testing.T) {
	manager, err := NewSpaceMigrationManager(SpaceMigrationManagerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.Prepare(SpaceMigrationPlan{
		ID:                 "orders-v2",
		Space:              "orders",
		FromVersion:        1,
		CompatibleVersions: []uint64{1, 3},
		Steps:              []SpaceMigrationStep{{ID: "add-status", TargetVersion: 2}},
	})
	if !errors.Is(err, ErrSpaceMigrationInvalid) {
		t.Fatalf("future compatibility error = %v, want ErrSpaceMigrationInvalid", err)
	}
}
