package hatSchema

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU21VersionedMigrationManagerAppliesValidatedSteps(t *testing.T) {
	base := versionedMigrationBaseSchema()
	preconditionCalls := 0
	plan, err := NewVersionedMigrationPlan("users-v2", base, []VersionedMigrationStep{
		{
			Migration: versionedMigrationAddEmail(),
			Preconditions: []VersionedMigrationPrecondition{func(schema Schema) error {
				preconditionCalls++
				if len(schema.Sources["users"].Columns) != 1 {
					t.Fatalf("first precondition schema = %#v", schema)
				}
				return nil
			}},
		},
		{
			Migration: versionedMigrationAddActive(),
			Preconditions: []VersionedMigrationPrecondition{func(schema Schema) error {
				if len(schema.Sources["users"].Columns) != 2 {
					t.Fatalf("second precondition schema = %#v", schema)
				}
				return nil
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewVersionedMigrationPlan() error = %v", err)
	}
	manager, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager() error = %v", err)
	}
	if err := manager.ApplyNext(context.Background()); err != nil {
		t.Fatalf("first ApplyNext() error = %v", err)
	}
	checkpoint := manager.Snapshot()
	if checkpoint.AppliedSteps != 1 || checkpoint.CurrentVersion != 1 || checkpoint.Phase != VersionedMigrationActive {
		t.Fatalf("after first step checkpoint = %#v", checkpoint)
	}
	if preconditionCalls != 1 {
		t.Fatalf("precondition calls = %d, want 1", preconditionCalls)
	}
	if err := manager.ApplyNext(context.Background()); err != nil {
		t.Fatalf("second ApplyNext() error = %v", err)
	}
	checkpoint = manager.Snapshot()
	if checkpoint.AppliedSteps != 2 || checkpoint.CurrentVersion != 2 || checkpoint.Phase != VersionedMigrationComplete {
		t.Fatalf("after second step checkpoint = %#v", checkpoint)
	}
	schema := manager.Schema()
	if got := len(schema.Sources["users"].Columns); got != 3 {
		t.Fatalf("column count = %d, want 3", got)
	}
}

func TestTU21VersionedMigrationManagerPreconditionFailureIsAtomic(t *testing.T) {
	sentinel := errors.New("table is not quiescent")
	plan, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{
		{
			Migration: versionedMigrationAddEmail(),
			Preconditions: []VersionedMigrationPrecondition{func(Schema) error {
				return sentinel
			}},
		},
	})
	if err != nil {
		t.Fatalf("NewVersionedMigrationPlan() error = %v", err)
	}
	manager, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager() error = %v", err)
	}
	wantFingerprint := versionedMigrationBaseSchema().Fingerprint()
	err = manager.ApplyNext(context.Background())
	if !errors.Is(err, sentinel) || !errors.Is(err, ErrVersionedMigrationPrecondition) {
		t.Fatalf("ApplyNext() error = %v, want precondition and sentinel", err)
	}
	checkpoint := manager.Snapshot()
	if checkpoint.AppliedSteps != 0 || checkpoint.CurrentVersion != 0 || checkpoint.SchemaFingerprint != wantFingerprint || checkpoint.Phase != VersionedMigrationActive {
		t.Fatalf("failed ApplyNext() changed state = %#v", checkpoint)
	}
}

func TestTU21VersionedMigrationManagerClientAdmissionAndRollbackGuard(t *testing.T) {
	plan, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{
		{Migration: versionedMigrationAddEmail()},
		{Migration: versionedMigrationAddActive()},
	})
	if err != nil {
		t.Fatalf("NewVersionedMigrationPlan() error = %v", err)
	}
	manager, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager() error = %v", err)
	}
	if err := manager.AdmitClient("old-client", 0); err != nil {
		t.Fatalf("AdmitClient(old) error = %v", err)
	}
	if err := manager.ApplyNext(context.Background()); err != nil {
		t.Fatalf("first ApplyNext() error = %v", err)
	}
	if err := manager.AdmitClient("new-client", 1); err != nil {
		t.Fatalf("AdmitClient(new) error = %v", err)
	}
	if err := manager.ApplyNext(context.Background()); err != nil {
		t.Fatalf("second ApplyNext() error = %v", err)
	}
	if err := manager.Rollback(); !errors.Is(err, ErrVersionedMigrationRollback) {
		t.Fatalf("Rollback() error = %v, want guard", err)
	}
	if err := manager.ReleaseClient("new-client"); err != nil {
		t.Fatalf("ReleaseClient(new) error = %v", err)
	}
	if err := manager.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	checkpoint := manager.Snapshot()
	if checkpoint.Phase != VersionedMigrationRolledBack || checkpoint.AppliedSteps != 0 || checkpoint.CurrentVersion != 0 {
		t.Fatalf("rollback checkpoint = %#v", checkpoint)
	}
	if got := manager.Schema().Fingerprint(); got != versionedMigrationBaseSchema().Fingerprint() {
		t.Fatalf("rollback fingerprint = %s, want %s", got, versionedMigrationBaseSchema().Fingerprint())
	}
	if err := manager.ReleaseClient("old-client"); err != nil {
		t.Fatalf("ReleaseClient(old) error = %v", err)
	}
}

func TestTU21VersionedMigrationManagerCheckpointRestoreAndIntegrity(t *testing.T) {
	plan, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{
		{Migration: versionedMigrationAddEmail()},
		{Migration: versionedMigrationAddActive()},
	})
	if err != nil {
		t.Fatalf("NewVersionedMigrationPlan() error = %v", err)
	}
	manager, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager() error = %v", err)
	}
	if err := manager.AdmitClient("reader", 0); err != nil {
		t.Fatalf("AdmitClient() error = %v", err)
	}
	if err := manager.ApplyNext(context.Background()); err != nil {
		t.Fatalf("ApplyNext() error = %v", err)
	}
	encoded, err := MarshalVersionedMigrationCheckpoint(manager.Snapshot())
	if err != nil {
		t.Fatalf("MarshalVersionedMigrationCheckpoint() error = %v", err)
	}
	decoded, err := UnmarshalVersionedMigrationCheckpoint(encoded)
	if err != nil {
		t.Fatalf("UnmarshalVersionedMigrationCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, manager.Snapshot()) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, manager.Snapshot())
	}
	restored, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager(restored) error = %v", err)
	}
	if err := restored.Restore(decoded); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if !reflect.DeepEqual(restored.Snapshot(), manager.Snapshot()) {
		t.Fatalf("restored checkpoint = %#v, want %#v", restored.Snapshot(), manager.Snapshot())
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := UnmarshalVersionedMigrationCheckpoint(corrupt); !errors.Is(err, ErrVersionedMigrationCheckpoint) {
		t.Fatalf("corrupt checkpoint error = %v, want ErrVersionedMigrationCheckpoint", err)
	}
	if err := restored.Restore(VersionedMigrationCheckpoint{
		PlanID:            decoded.PlanID,
		BaseVersion:       decoded.BaseVersion,
		TargetVersion:     decoded.TargetVersion,
		CurrentVersion:    decoded.CurrentVersion,
		AppliedSteps:      decoded.AppliedSteps,
		Generation:        decoded.Generation,
		Phase:             decoded.Phase,
		SchemaFingerprint: "wrong",
		Clients:           decoded.Clients,
	}); !errors.Is(err, ErrVersionedMigrationCheckpoint) {
		t.Fatalf("wrong fingerprint restore error = %v, want ErrVersionedMigrationCheckpoint", err)
	}
}

func TestTU21VersionedMigrationPlanRejectsNonReversibleChanges(t *testing.T) {
	_, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{{
		Migration: Migration{
			Version: 1,
			Name:    "bad-down",
			Up: []Change{{
				Kind:       ChangeAddColumn,
				SourceName: "users",
				Column:     Column{Name: "email", Type: TypeText},
			}},
			Down: []Change{{
				Kind:       ChangeDropColumn,
				SourceName: "users",
				Column:     Column{Name: "different", Type: TypeText},
			}},
		},
	}})
	if !errors.Is(err, ErrVersionedMigrationPlanInvalid) {
		t.Fatalf("invalid reverse plan error = %v, want ErrVersionedMigrationPlanInvalid", err)
	}
}

func TestTU21VersionedMigrationManagerRejectsStaleConcurrentApply(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	plan, err := NewVersionedMigrationPlan("users-v2", versionedMigrationBaseSchema(), []VersionedMigrationStep{{
		Migration: versionedMigrationAddEmail(),
		Preconditions: []VersionedMigrationPrecondition{func(Schema) error {
			close(started)
			<-release
			return nil
		}},
	}})
	if err != nil {
		t.Fatalf("NewVersionedMigrationPlan() error = %v", err)
	}
	manager, err := NewVersionedMigrationManager(plan)
	if err != nil {
		t.Fatalf("NewVersionedMigrationManager() error = %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- manager.ApplyNext(context.Background()) }()
	<-started
	if err := manager.AdmitClient("reader", 0); err != nil {
		t.Fatalf("AdmitClient() error = %v", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrVersionedMigrationConflict) {
		t.Fatalf("stale ApplyNext() error = %v, want conflict", err)
	}
	if checkpoint := manager.Snapshot(); checkpoint.AppliedSteps != 0 || checkpoint.CurrentVersion != 0 {
		t.Fatalf("stale apply changed schema = %#v", checkpoint)
	}
}

func versionedMigrationBaseSchema() Schema {
	return Schema{
		Version: 0,
		Sources: map[string]Source{
			"users": {
				Name:    "users",
				Columns: []Column{{Name: "id", Type: TypeInteger, NotNull: true}},
			},
		},
	}
}

func versionedMigrationAddEmail() Migration {
	return Migration{
		Version: 1,
		Name:    "add-email",
		Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}},
		Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "email", Type: TypeText}}},
	}
}

func versionedMigrationAddActive() Migration {
	return Migration{
		Version: 2,
		Name:    "add-active",
		Up:      []Change{{Kind: ChangeAddColumn, SourceName: "users", Column: Column{Name: "active", Type: TypeBoolean}}},
		Down:    []Change{{Kind: ChangeDropColumn, SourceName: "users", Column: Column{Name: "active", Type: TypeBoolean}}},
	}
}
