package hatDataStructure

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTU20OnlineTupleUpgradeReadsWritesAndMigratesAtomically(t *testing.T) {
	previous, next, converter := tu20UpgradeFormats(t)
	upgrade, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 2, converter)
	if err != nil {
		t.Fatalf("NewOnlineTupleUpgrade() error = %v", err)
	}
	oldRows := []VersionedTuple{tu20UpgradeOldRow(t, previous, 1, "alice"), tu20UpgradeOldRow(t, previous, 2, "bob")}

	read, err := upgrade.Read(oldRows[0])
	if err != nil {
		t.Fatalf("Read(old) error = %v", err)
	}
	if read.Version() != next.Version() || read.Tuple().FieldCount() != next.FieldCount() {
		t.Fatalf("converted read = version %d/%d fields, want %d/%d", read.Version(), read.Tuple().FieldCount(), next.Version(), next.FieldCount())
	}
	written, err := upgrade.Write([]TupleFieldValue{TupleInt64(3), TupleString("carol"), TupleBool(true), TupleInt64(9)})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if written.Version() != next.Version() {
		t.Fatalf("Write() version = %d, want %d", written.Version(), next.Version())
	}

	first, err := upgrade.MigrateBatch(context.Background(), oldRows, 1)
	if err != nil {
		t.Fatalf("first MigrateBatch() error = %v", err)
	}
	if len(first) != 1 || first[0].Version() != next.Version() {
		t.Fatalf("first migrated rows = %#v", first)
	}
	snapshot := upgrade.Snapshot()
	if snapshot.NextRow != 1 || snapshot.MigratedRows != 1 || snapshot.Phase != TupleUpgradePhaseActive {
		t.Fatalf("progress after first batch = %#v", snapshot)
	}
	if err := upgrade.BeginCutover(); !errors.Is(err, ErrTupleUpgradeIncomplete) {
		t.Fatalf("early BeginCutover() error = %v, want incomplete", err)
	}
	second, err := upgrade.MigrateBatch(context.Background(), oldRows[1:], 1)
	if err != nil {
		t.Fatalf("second MigrateBatch() error = %v", err)
	}
	if len(second) != 1 || second[0].Version() != next.Version() {
		t.Fatalf("second migrated rows = %#v", second)
	}
	if err := upgrade.BeginCutover(); err != nil {
		t.Fatalf("BeginCutover() error = %v", err)
	}
	if upgrade.Phase() != TupleUpgradePhaseCutover {
		t.Fatalf("phase after BeginCutover() = %s", upgrade.Phase())
	}
	if err := upgrade.CompleteCutover(); err != nil {
		t.Fatalf("CompleteCutover() error = %v", err)
	}
	if !upgrade.Complete() || upgrade.Phase() != TupleUpgradePhaseComplete {
		t.Fatalf("completed upgrade = phase %s complete=%t", upgrade.Phase(), upgrade.Complete())
	}
	if _, err := upgrade.Read(oldRows[0]); !errors.Is(err, ErrTupleUpgradeFormatMismatch) {
		t.Fatalf("old read after complete error = %v, want format mismatch", err)
	}
}

func TestTU20OnlineTupleUpgradeCheckpointRestoreAndRollback(t *testing.T) {
	previous, next, converter := tu20UpgradeFormats(t)
	upgrade, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 2, converter)
	if err != nil {
		t.Fatal(err)
	}
	row := tu20UpgradeOldRow(t, previous, 1, "alice")
	if _, err := upgrade.MigrateBatch(context.Background(), []VersionedTuple{row}, 1); err != nil {
		t.Fatal(err)
	}
	wire, err := MarshalTupleUpgradeCheckpoint(upgrade.Snapshot())
	if err != nil {
		t.Fatalf("MarshalTupleUpgradeCheckpoint() error = %v", err)
	}
	decoded, err := UnmarshalTupleUpgradeCheckpoint(wire)
	if err != nil {
		t.Fatalf("UnmarshalTupleUpgradeCheckpoint() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, upgrade.Snapshot()) {
		t.Fatalf("decoded checkpoint = %#v, want %#v", decoded, upgrade.Snapshot())
	}
	resumed, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 2, converter)
	if err != nil {
		t.Fatal(err)
	}
	if err := resumed.Restore(decoded); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if !reflect.DeepEqual(resumed.Snapshot(), upgrade.Snapshot()) {
		t.Fatalf("restored snapshot = %#v, want %#v", resumed.Snapshot(), upgrade.Snapshot())
	}
	if err := resumed.Rollback(); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	if resumed.Phase() != TupleUpgradePhaseRolledBack {
		t.Fatalf("rollback phase = %s", resumed.Phase())
	}
	nextRow := tu20UpgradeNewRow(t, next, 8, "new")
	oldAfterRollback, err := resumed.Read(nextRow)
	if err != nil {
		t.Fatalf("Read(next after rollback) error = %v", err)
	}
	if oldAfterRollback.Version() != previous.Version() {
		t.Fatalf("rollback conversion version = %d, want %d", oldAfterRollback.Version(), previous.Version())
	}
	if _, err := resumed.Write([]TupleFieldValue{TupleInt64(9), TupleString("old"), TupleBool(false)}); err != nil {
		t.Fatalf("rollback Write() error = %v", err)
	}

	corrupt := append([]byte(nil), wire...)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := UnmarshalTupleUpgradeCheckpoint(corrupt); !errors.Is(err, ErrTupleUpgradeCheckpointWire) {
		t.Fatalf("corrupt checkpoint error = %v, want wire error", err)
	}
	if _, err := UnmarshalTupleUpgradeCheckpoint(append(wire, 0)); !errors.Is(err, ErrTupleUpgradeCheckpointWire) {
		t.Fatalf("trailing checkpoint error = %v, want wire error", err)
	}
}

func TestTU20OnlineTupleUpgradeRejectsFailedOrCanceledBatches(t *testing.T) {
	previous, next, converter := tu20UpgradeFormats(t)
	failing := converter
	failing.ToNext = func(tuple VersionedTuple) (VersionedTuple, error) {
		values, err := previous.Unpack(tuple.Tuple())
		if err != nil {
			return VersionedTuple{}, err
		}
		if values[0].Int64 == 99 {
			return VersionedTuple{}, errors.New("conversion failed")
		}
		return next.PackVersioned(values)
	}
	upgrade, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 2, failing)
	if err != nil {
		t.Fatal(err)
	}
	rows := []VersionedTuple{tu20UpgradeOldRow(t, previous, 1, "ok"), tu20UpgradeOldRow(t, previous, 99, "bad")}
	before := upgrade.Snapshot()
	if _, err := upgrade.MigrateBatch(context.Background(), rows, 2); err == nil {
		t.Fatal("failed MigrateBatch() error = nil")
	}
	if !reflect.DeepEqual(upgrade.Snapshot(), before) {
		t.Fatalf("failed batch changed progress: got %#v, want %#v", upgrade.Snapshot(), before)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := upgrade.MigrateBatch(ctx, rows[:1], 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled MigrateBatch() error = %v, want canceled", err)
	}
	if !reflect.DeepEqual(upgrade.Snapshot(), before) {
		t.Fatalf("canceled batch changed progress: got %#v, want %#v", upgrade.Snapshot(), before)
	}
	tooSmall, err := NewOnlineTupleUpgrade("users-v1-v2", previous, next, 1, converter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tooSmall.MigrateBatch(context.Background(), rows, 2); !errors.Is(err, ErrTupleUpgradeInvalid) {
		t.Fatalf("over-limit MigrateBatch() error = %v, want invalid", err)
	}
}

func tu20UpgradeFormats(t testing.TB) (TupleFormat, TupleFormat, TupleUpgradeConverter) {
	t.Helper()
	previous, err := NewTupleFormat(1, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
	})
	if err != nil {
		t.Fatal(err)
	}
	defaultScore := TupleInt64(0)
	next, err := NewTupleFormat(2, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
		{Name: "score", Type: TupleFieldInt64, Default: &defaultScore},
	})
	if err != nil {
		t.Fatal(err)
	}
	return previous, next, TupleUpgradeConverter{
		ToNext: func(tuple VersionedTuple) (VersionedTuple, error) {
			values, err := previous.Unpack(tuple.Tuple())
			if err != nil {
				return VersionedTuple{}, err
			}
			return next.PackVersioned(values)
		},
		ToPrevious: func(tuple VersionedTuple) (VersionedTuple, error) {
			values, err := next.Unpack(tuple.Tuple())
			if err != nil {
				return VersionedTuple{}, err
			}
			return previous.PackVersioned(values[:previous.FieldCount()])
		},
	}
}

func tu20UpgradeOldRow(t testing.TB, format TupleFormat, id int64, name string) VersionedTuple {
	t.Helper()
	tuple, err := format.PackVersioned([]TupleFieldValue{TupleInt64(id), TupleString(name), TupleBool(true)})
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}

func tu20UpgradeNewRow(t testing.TB, format TupleFormat, id int64, name string) VersionedTuple {
	t.Helper()
	tuple, err := format.PackVersioned([]TupleFieldValue{TupleInt64(id), TupleString(name), TupleBool(true), TupleInt64(4)})
	if err != nil {
		t.Fatal(err)
	}
	return tuple
}
