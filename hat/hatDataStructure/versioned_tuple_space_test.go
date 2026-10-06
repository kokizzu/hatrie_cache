package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestVersionedTupleSpaceUpgradesOnlineAndNormalizesWrites(t *testing.T) {
	formatV1 := mustMigrationFormat(t, 1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
	})
	activeDefault := TupleBool(false)
	formatV2 := mustMigrationFormat(t, 2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool, Default: &activeDefault},
	})
	manager := mustSpaceUpgradeManager(t, formatV1, formatV2)
	space, err := NewVersionedTupleSpace(manager)
	if err != nil {
		t.Fatal(err)
	}

	oldAlice := mustSpaceTuple(t, formatV1, 1, "alice")
	oldBob := mustSpaceTuple(t, formatV1, 2, "bob")
	currentCarol := mustSpaceTuple(t, formatV2, 3, "carol", TupleBool(true))
	if err := space.Restore("alice", oldAlice); err != nil {
		t.Fatal(err)
	}
	if err := space.Restore("bob", oldBob); err != nil {
		t.Fatal(err)
	}
	if err := space.Restore("carol", currentCarol); err != nil {
		t.Fatal(err)
	}

	started, err := space.StartUpgrade()
	if err != nil {
		t.Fatal(err)
	}
	if started.TargetVersion != 2 || started.Total != 3 || started.Pending != 2 || !started.Active || started.Complete {
		t.Fatalf("StartUpgrade() = %#v", started)
	}

	// A write from an old client is accepted while the upgrade is active but
	// is normalized immediately, so it does not extend the conversion queue.
	if err := space.Upsert("dave", oldBob); err != nil {
		t.Fatal(err)
	}
	dave, err := space.Get("dave")
	if err != nil {
		t.Fatal(err)
	}
	if dave.Version() != 2 {
		t.Fatalf("old-version write was stored as version %d, want 2", dave.Version())
	}

	// Reads may make progress without waiting for the batch worker.
	alice, err := space.Get("alice")
	if err != nil {
		t.Fatal(err)
	}
	if alice.Version() != 2 {
		t.Fatalf("lazy read returned version %d, want 2", alice.Version())
	}
	status, err := space.UpgradeStep(1)
	if err != nil {
		t.Fatal(err)
	}
	for status.Active {
		status, err = space.UpgradeStep(1)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !status.Complete || status.Failed != 0 || status.Pending != 0 || status.Migrated < 2 {
		t.Fatalf("completed upgrade status = %#v", status)
	}
	for _, key := range []string{"alice", "bob", "carol", "dave"} {
		record, err := space.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if record.Version() != 2 {
			t.Fatalf("Get(%q) returned version %d, want 2", key, record.Version())
		}
	}
	if got := space.Len(); got != 4 {
		t.Fatalf("Len() = %d, want 4", got)
	}

	finished, err := space.StartUpgrade()
	if err != nil {
		t.Fatal(err)
	}
	if !finished.Complete || finished.Active || finished.Pending != 0 {
		t.Fatalf("no-op StartUpgrade() = %#v", finished)
	}
	if !space.Delete("dave") || space.Delete("dave") {
		t.Fatal("Delete() did not report exactly one deletion")
	}
}

func TestVersionedTupleSpaceKeepsFailedRecordsForRetry(t *testing.T) {
	formatV1 := mustMigrationFormat(t, 11, []TupleFieldSpec{{Name: "name", Type: TupleFieldString}})
	formatV2 := mustMigrationFormat(t, 12, []TupleFieldSpec{{Name: "name", Type: TupleFieldString}})
	manager, err := NewVersionedTupleMigrationManager(formatV2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		t.Fatal(err)
	}
	rejected := errors.New("poison record")
	if err := manager.RegisterMigration(11, 12, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		if values[0].String == "bad" {
			return nil, rejected
		}
		return values, nil
	}); err != nil {
		t.Fatal(err)
	}
	space, err := NewVersionedTupleSpace(manager)
	if err != nil {
		t.Fatal(err)
	}
	if err := space.Restore("good", mustSpaceTuple(t, formatV1, "good")); err != nil {
		t.Fatal(err)
	}
	if err := space.Restore("bad", mustSpaceTuple(t, formatV1, "bad")); err != nil {
		t.Fatal(err)
	}
	if _, err := space.StartUpgrade(); err != nil {
		t.Fatal(err)
	}
	status, err := space.UpgradeStep(8)
	if !errors.Is(err, ErrVersionedTupleSpaceUpgradeRecord) || !errors.Is(err, rejected) {
		t.Fatalf("UpgradeStep() error = %v, want record and callback errors", err)
	}
	if status.Active || status.Complete || status.Failed != 1 || status.Pending != 1 || status.Migrated != 1 {
		t.Fatalf("failed upgrade status = %#v", status)
	}
	good, err := space.Get("good")
	if err != nil {
		t.Fatal(err)
	}
	if good.Version() != 12 {
		t.Fatalf("good record version = %d, want 12", good.Version())
	}
	bad, err := space.Get("bad")
	if err != nil {
		t.Fatal(err)
	}
	if bad.Version() != 11 {
		t.Fatalf("failed record version = %d, want 11", bad.Version())
	}
	values, err := formatV1.Unpack(bad.Tuple())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []TupleFieldValue{TupleString("bad")}) {
		t.Fatalf("failed record values = %#v", values)
	}
}

func mustSpaceUpgradeManager(t *testing.T, formatV1, formatV2 TupleFormat) *VersionedTupleMigrationManager {
	t.Helper()
	manager, err := NewVersionedTupleMigrationManager(formatV2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterMigration(1, 2, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return append(values, TupleBool(false)), nil
	}); err != nil {
		t.Fatal(err)
	}
	return manager
}

func mustSpaceTuple(t *testing.T, format TupleFormat, values ...any) VersionedTuple {
	t.Helper()
	fields := make([]TupleFieldValue, len(values))
	for index, value := range values {
		switch typed := value.(type) {
		case int:
			fields[index] = TupleInt64(int64(typed))
		case string:
			fields[index] = TupleString(typed)
		case TupleFieldValue:
			fields[index] = typed
		default:
			t.Fatalf("unsupported test tuple value %T", value)
		}
	}
	tuple, err := format.PackVersioned(fields)
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}
