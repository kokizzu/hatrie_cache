package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestTU21VersionedMigrationLifecycleAndClientGate(t *testing.T) {
	manager := NewVersionedMigrationManager()
	if err := manager.RegisterClient(MigrationClient{Name: "api", MinVersion: 1, MaxVersion: 1}); err != nil {
		t.Fatal(err)
	}
	plan := MigrationPlan{
		Name:          "users-v2",
		FromVersion:   1,
		ToVersion:     2,
		TotalUnits:    4,
		Preconditions: []string{"backup-complete"},
	}
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgePrecondition(plan.Name, "backup-complete"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(plan.Name); !errors.Is(err, ErrVersionedMigrationClientCompatibility) {
		t.Fatalf("start with old client error = %v", err)
	}
	if err := manager.RegisterClient(MigrationClient{Name: "api", MinVersion: 1, MaxVersion: 2}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(plan.Name); err != nil {
		t.Fatal(err)
	}
	if err := manager.Advance(plan.Name, 2); err != nil {
		t.Fatal(err)
	}
	if err := manager.Pause(plan.Name); err != nil {
		t.Fatal(err)
	}
	status, err := manager.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != MigrationPaused || status.CompletedUnits != 2 {
		t.Fatalf("paused status = %+v", status)
	}
	if err := manager.Resume(plan.Name); err != nil {
		t.Fatal(err)
	}
	if err := manager.Advance(plan.Name, 2); err != nil {
		t.Fatal(err)
	}
	status, err = manager.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != MigrationCompleted || status.CompletedUnits != plan.TotalUnits {
		t.Fatalf("completed status = %+v", status)
	}
}

func TestTU21VersionedMigrationDependenciesAndRollback(t *testing.T) {
	manager := NewVersionedMigrationManager()
	if err := manager.RegisterClient(MigrationClient{Name: "worker", MinVersion: 1, MaxVersion: 3}); err != nil {
		t.Fatal(err)
	}
	base := MigrationPlan{Name: "base", FromVersion: 1, ToVersion: 2, TotalUnits: 1}
	dependent := MigrationPlan{Name: "dependent", FromVersion: 2, ToVersion: 3, TotalUnits: 1, Dependencies: []string{"base"}}
	if err := manager.RegisterPlan(base); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterPlan(dependent); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(dependent.Name); !errors.Is(err, ErrVersionedMigrationDependency) {
		t.Fatalf("dependent start error = %v", err)
	}
	if err := manager.Start(base.Name); err != nil {
		t.Fatal(err)
	}
	if err := manager.Advance(base.Name, 1); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(dependent.Name); err != nil {
		t.Fatal(err)
	}
	if manager.UnregisterClient("worker") {
		t.Fatal("active migration allowed client removal")
	}
	if err := manager.Rollback(base.Name); !errors.Is(err, ErrVersionedMigrationDependency) {
		t.Fatalf("rollback with dependent error = %v", err)
	}
	if err := manager.Advance(dependent.Name, 1); err != nil {
		t.Fatal(err)
	}
	if err := manager.Rollback(base.Name); !errors.Is(err, ErrVersionedMigrationDependency) {
		t.Fatalf("rollback with completed dependent error = %v", err)
	}
}

func TestTU21VersionedMigrationProgressIsAtomic(t *testing.T) {
	manager := NewVersionedMigrationManager()
	plan := MigrationPlan{Name: "items", FromVersion: 1, ToVersion: 2, TotalUnits: 3}
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgePrecondition(plan.Name, "missing"); !errors.Is(err, ErrVersionedMigrationPrecondition) {
		t.Fatalf("unknown precondition error = %v", err)
	}
	if err := manager.Start(plan.Name); err != nil {
		t.Fatal(err)
	}
	if err := manager.Advance(plan.Name, 4); !errors.Is(err, ErrVersionedMigrationProgress) {
		t.Fatalf("overflow progress error = %v", err)
	}
	status, err := manager.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if status.CompletedUnits != 0 || status.Phase != MigrationRunning {
		t.Fatalf("state changed after rejected progress: %+v", status)
	}
}

func TestTU21VersionedMigrationSnapshotRoundTripAndCorruption(t *testing.T) {
	manager := NewVersionedMigrationManager()
	plan := MigrationPlan{
		Name:          "orders",
		FromVersion:   2,
		ToVersion:     3,
		TotalUnits:    5,
		Preconditions: []string{"dual-write"},
	}
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgePrecondition(plan.Name, "dual-write"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(plan.Name); err != nil {
		t.Fatal(err)
	}
	if err := manager.Advance(plan.Name, 2); err != nil {
		t.Fatal(err)
	}
	encoded, err := manager.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewVersionedMigrationManager()
	if err := restored.UnmarshalBinary(encoded); err != nil {
		t.Fatal(err)
	}
	want, err := manager.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != want.Phase || got.CompletedUnits != want.CompletedUnits || len(got.AcknowledgedPreconditions) != 1 || got.AcknowledgedPreconditions[0] != "dual-write" {
		t.Fatalf("restored status = %+v, want %+v", got, want)
	}
	encoded[len(encoded)-1] ^= 0xff
	if err := restored.UnmarshalBinary(encoded); !errors.Is(err, ErrVersionedMigrationSnapshot) {
		t.Fatalf("corrupt snapshot error = %v", err)
	}
	after, err := restored.Status(plan.Name)
	if err != nil {
		t.Fatal(err)
	}
	if after.Phase != want.Phase || after.CompletedUnits != want.CompletedUnits {
		t.Fatalf("corrupt snapshot changed state: %+v", after)
	}
}

func TestTU21VersionedMigrationValidationAndCopies(t *testing.T) {
	manager := NewVersionedMigrationManager()
	plan := MigrationPlan{
		Name:          "copy-test",
		FromVersion:   1,
		ToVersion:     2,
		TotalUnits:    1,
		Dependencies:  []string{"dep"},
		Preconditions: []string{"check"},
	}
	if err := manager.RegisterPlan(plan); err != nil {
		t.Fatal(err)
	}
	plan.Dependencies[0] = "mutated"
	plan.Preconditions[0] = "mutated"
	status, err := manager.Status("copy-test")
	if err != nil {
		t.Fatal(err)
	}
	if status.Plan.Dependencies[0] != "dep" || status.Plan.Preconditions[0] != "check" {
		t.Fatalf("plan was not copied: %+v", status.Plan)
	}
	status.Plan.Dependencies[0] = "caller-mutated"
	again, err := manager.Status("copy-test")
	if err != nil {
		t.Fatal(err)
	}
	if again.Plan.Dependencies[0] != "dep" {
		t.Fatalf("status leaked internal plan: %+v", again.Plan)
	}
	if err := manager.RegisterPlan(MigrationPlan{Name: "copy-test", FromVersion: 1, ToVersion: 2, TotalUnits: 1}); !errors.Is(err, ErrVersionedMigrationExists) {
		t.Fatalf("duplicate plan error = %v", err)
	}
}

func TestTU21VersionedMigrationRejectsImpossiblePayloadLength(t *testing.T) {
	manager := NewVersionedMigrationManager()
	encoded := make([]byte, 13)
	encoded[0] = 'H'
	encoded[1] = 'M'
	encoded[2] = 'G'
	encoded[3] = '1'
	encoded[4] = 1
	binary.BigEndian.PutUint32(encoded[5:9], ^uint32(0))
	if err := manager.UnmarshalBinary(encoded); !errors.Is(err, ErrVersionedMigrationSnapshot) {
		t.Fatalf("impossible payload length error = %v", err)
	}
}
