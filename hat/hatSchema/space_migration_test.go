package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestSpaceMigrationManagerAppliesResumesSnapshotsAndRollsBack(t *testing.T) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	manager := NewSpaceMigrationManager()
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatalf("RegisterPlan() error = %v", err)
	}
	if err := manager.Start(plan.ID, base); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var applied []int
	err := manager.Apply(context.Background(), plan.ID, func(_ context.Context, operation SpaceMigrationOperation) error {
		applied = append(applied, operation.Step)
		if operation.Direction != SpaceMigrationDirectionUp {
			t.Fatalf("apply direction = %q", operation.Direction)
		}
		if operation.To.Version != operation.From.Version+1 {
			t.Fatalf("apply versions = %d -> %d", operation.From.Version, operation.To.Version)
		}
		operation.To.Sources["users"] = Source{
			Name:    "callback-mutation",
			Columns: []Column{{Name: "bad", Type: TypeText}},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !reflect.DeepEqual(applied, []int{0, 1}) {
		t.Fatalf("applied steps = %v, want [0 1]", applied)
	}
	current, ok := manager.Schema(plan.ID)
	if !ok || current.Sources["users"].Name != "users" || len(current.Sources["users"].Columns) != 3 {
		t.Fatalf("callback mutated committed schema = %#v, found=%v", current, ok)
	}
	progress, ok := manager.Progress(plan.ID)
	if !ok || progress.Status != SpaceMigrationStatusApplied || progress.CurrentVersion != 3 || progress.NextStep != 2 {
		t.Fatalf("progress = %#v, found=%v", progress, ok)
	}

	snapshot := manager.Snapshot()
	cleanSnapshot := manager.Snapshot()
	snapshot.States[0].Current.Sources["users"] = Source{Name: "snapshot-mutation", Columns: []Column{{Name: "bad", Type: TypeText}}}
	currentAfterSnapshotMutation, ok := manager.Schema(plan.ID)
	if !ok || currentAfterSnapshotMutation.Sources["users"].Name != "users" {
		t.Fatalf("snapshot mutated manager state = %#v, found=%v", currentAfterSnapshotMutation, ok)
	}
	restored := NewSpaceMigrationManager()
	if err := restored.Restore(cleanSnapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	restoredSchema, ok := restored.Schema(plan.ID)
	if !ok || restoredSchema.Fingerprint() != cleanSnapshot.States[0].Current.Fingerprint() {
		t.Fatalf("restored schema = %#v, found=%v", restoredSchema, ok)
	}

	var rolledBack []int
	err = restored.Rollback(context.Background(), plan.ID, func(_ context.Context, operation SpaceMigrationOperation) error {
		rolledBack = append(rolledBack, operation.Step)
		if operation.Direction != SpaceMigrationDirectionDown {
			t.Fatalf("rollback direction = %q", operation.Direction)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if !reflect.DeepEqual(rolledBack, []int{1, 0}) {
		t.Fatalf("rolled back steps = %v, want [1 0]", rolledBack)
	}
	progress, ok = restored.Progress(plan.ID)
	if !ok || progress.Status != SpaceMigrationStatusRolledBack || progress.CurrentVersion != 1 || progress.NextStep != 0 {
		t.Fatalf("rolled back progress = %#v, found=%v", progress, ok)
	}
}

func TestSpaceMigrationManagerResumesAfterFailedStep(t *testing.T) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	manager := NewSpaceMigrationManager()
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatalf("RegisterPlan() error = %v", err)
	}
	if err := manager.Start(plan.ID, base); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	failOnce := true
	err := manager.Apply(context.Background(), plan.ID, func(_ context.Context, operation SpaceMigrationOperation) error {
		if failOnce && operation.Step == 0 {
			failOnce = false
			return errors.New("temporary migration failure")
		}
		return nil
	})
	if err == nil {
		t.Fatal("Apply() succeeded through an injected failure")
	}
	progress, ok := manager.Progress(plan.ID)
	if !ok || progress.Status != SpaceMigrationStatusFailed || progress.NextStep != 0 || progress.CurrentVersion != 1 {
		t.Fatalf("failed progress = %#v, found=%v", progress, ok)
	}

	if err := manager.Apply(context.Background(), plan.ID, func(_ context.Context, _ SpaceMigrationOperation) error { return nil }); err != nil {
		t.Fatalf("Apply() resume error = %v", err)
	}
	progress, ok = manager.Progress(plan.ID)
	if !ok || progress.Status != SpaceMigrationStatusApplied || progress.NextStep != 2 {
		t.Fatalf("resumed progress = %#v, found=%v", progress, ok)
	}
}

func TestSpaceMigrationManagerChecksFingerprintAndRollingCompatibility(t *testing.T) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	manager := NewSpaceMigrationManager()
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatalf("RegisterPlan() error = %v", err)
	}
	stale := base.Clone()
	stale.Version = 2
	if err := manager.Start(plan.ID, stale); !errors.Is(err, ErrSpaceMigrationPrecondition) {
		t.Fatalf("Start(stale) error = %v, want %v", err, ErrSpaceMigrationPrecondition)
	}

	incompatible := plan
	incompatible.ID = "required-column"
	incompatible.Migrations = []Migration{{
		Version: 2,
		Name:    "require email",
		Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText, NotNull: true}}},
		Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "email"}}},
	}}
	if err := manager.RegisterPlan(incompatible); err != nil {
		t.Fatalf("RegisterPlan(incompatible) error = %v", err)
	}
	if err := manager.Start(incompatible.ID, base); !errors.Is(err, ErrSpaceMigrationCompatibility) {
		t.Fatalf("Start(incompatible) error = %v, want %v", err, ErrSpaceMigrationCompatibility)
	}
	incompatible.Compatibility = SpaceMigrationCompatibilityExclusive
	incompatible.ID = "exclusive-required-column"
	if err := manager.RegisterPlan(incompatible); err != nil {
		t.Fatalf("RegisterPlan(exclusive) error = %v", err)
	}
	if err := manager.Start(incompatible.ID, base); err != nil {
		t.Fatalf("Start(exclusive) error = %v", err)
	}
}

func TestSpaceMigrationManagerRejectsNonReversiblePlan(t *testing.T) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	plan.Migrations[0].Down = []Change{{
		Kind:       ChangeAddColumn,
		SourceName: "users",
		Column:     Column{Name: "wrong", Type: TypeText},
	}}
	manager := NewSpaceMigrationManager()
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatalf("RegisterPlan() error = %v", err)
	}
	if err := manager.Start(plan.ID, base); !errors.Is(err, ErrSpaceMigrationPlanInvalid) {
		t.Fatalf("Start() error = %v, want %v", err, ErrSpaceMigrationPlanInvalid)
	}
}

func BenchmarkSpaceMigrationPreviewSequence(b *testing.B) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		current := base.Clone()
		b.StartTimer()
		for _, migration := range plan.Migrations {
			var err error
			current, err = Preview(&current, migration)
			if err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
	}
}

func BenchmarkSpaceMigrationManagerApply(b *testing.B) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		manager := NewSpaceMigrationManager()
		if err := manager.RegisterPlan(plan); err != nil {
			b.Fatal(err)
		}
		if err := manager.Start(plan.ID, base); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := manager.Apply(context.Background(), plan.ID, func(_ context.Context, _ SpaceMigrationOperation) error { return nil }); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkSpaceMigrationManagerSnapshot(b *testing.B) {
	base := testSpaceMigrationSchema()
	plan := testSpaceMigrationPlan(base)
	manager := NewSpaceMigrationManager()
	if err := manager.RegisterPlan(plan); err != nil {
		b.Fatal(err)
	}
	if err := manager.Start(plan.ID, base); err != nil {
		b.Fatal(err)
	}
	if err := manager.Apply(context.Background(), plan.ID, func(_ context.Context, _ SpaceMigrationOperation) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = manager.Snapshot()
	}
}

func testSpaceMigrationSchema() Schema {
	return Schema{
		Version: 1,
		Sources: map[string]Source{
			"users": {
				Name:    "users",
				Columns: []Column{{Name: "id", Type: TypeInteger}},
			},
		},
	}
}

func testSpaceMigrationPlan(base Schema) SpaceMigrationPlan {
	return SpaceMigrationPlan{
		PlanVersion:     SpaceMigrationPlanVersion,
		ID:              "users-rollout",
		Space:           "users",
		BaseVersion:     base.Version,
		BaseFingerprint: base.Fingerprint(),
		Compatibility:   SpaceMigrationCompatibilityRolling,
		Migrations: []Migration{
			{
				Version: 2,
				Name:    "add email",
				Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}},
				Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "email"}}},
			},
			{
				Version: 3,
				Name:    "add active",
				Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "active", Type: TypeBoolean}}},
				Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "active"}}},
			},
		},
	}
}
