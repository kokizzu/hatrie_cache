package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestVersionedTupleMigrationManagerMigratesLinearPlan(t *testing.T) {
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
	regionDefault := TupleString("unknown")
	formatV3 := mustMigrationFormat(t, 3, []TupleFieldSpec{
		{Name: "id", Type: TupleFieldInt64},
		{Name: "name", Type: TupleFieldString},
		{Name: "active", Type: TupleFieldBool},
		{Name: "region", Type: TupleFieldString, Default: &regionDefault},
	})

	manager, err := NewVersionedTupleMigrationManager(formatV3)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV2); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterMigration(1, 2, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		if len(values) != 2 {
			t.Fatalf("v1 values = %#v, want two fields", values)
		}
		return []TupleFieldValue{values[0], TupleString(values[1].String + "-migrated")}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterMigration(2, 3, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return append(values, TupleString("ap-southeast")), nil
	}); err != nil {
		t.Fatal(err)
	}

	plan, err := manager.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if plan.FromVersion != 1 || plan.ToVersion != 3 || !reflect.DeepEqual(plan.Versions, []uint64{1, 2, 3}) {
		t.Fatalf("Plan() = %#v", plan)
	}
	plan.Versions[1] = 99
	unchanged, err := manager.Plan(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unchanged.Versions, []uint64{1, 2, 3}) {
		t.Fatalf("Plan() returned aliased versions: %#v", unchanged)
	}

	original, err := formatV1.PackVersioned([]TupleFieldValue{TupleInt64(42), TupleString("orders")})
	if err != nil {
		t.Fatal(err)
	}
	migrated, err := manager.Migrate(original)
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Version() != 3 {
		t.Fatalf("Migrate() version = %d, want 3", migrated.Version())
	}
	if original.Version() != 1 {
		t.Fatalf("Migrate() mutated original version to %d", original.Version())
	}
	if err := migrated.Validate(formatV3); err != nil {
		t.Fatalf("migrated Validate() error = %v", err)
	}
	values, err := formatV3.Unpack(migrated.Tuple())
	if err != nil {
		t.Fatal(err)
	}
	want := []TupleFieldValue{TupleInt64(42), TupleString("orders-migrated"), TupleBool(false), TupleString("ap-southeast")}
	if !reflect.DeepEqual(values, want) {
		t.Fatalf("migrated values = %#v, want %#v", values, want)
	}

	partial, err := manager.MigrateTo(original, 2)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Version() != 2 {
		t.Fatalf("MigrateTo() version = %d, want 2", partial.Version())
	}
	if err := partial.Validate(formatV2); err != nil {
		t.Fatalf("partial Validate() error = %v", err)
	}
}

func TestVersionedTupleMigrationManagerRejectsInvalidPlansWithoutMutation(t *testing.T) {
	formatV1 := mustMigrationFormat(t, 11, []TupleFieldSpec{{Name: "name", Type: TupleFieldString}})
	formatV2 := mustMigrationFormat(t, 12, []TupleFieldSpec{{Name: "name", Type: TupleFieldString}})
	manager, err := NewVersionedTupleMigrationManager(formatV2)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterFormat(formatV1); err != nil {
		t.Fatal(err)
	}
	original, err := formatV1.PackVersioned([]TupleFieldValue{TupleString("before")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Migrate(original); !errors.Is(err, ErrVersionedTupleMigrationPath) {
		t.Fatalf("missing migration error = %v, want %v", err, ErrVersionedTupleMigrationPath)
	}
	if err := manager.RegisterMigration(11, 12, func([]TupleFieldValue) ([]TupleFieldValue, error) {
		return nil, errors.New("migration rejected")
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.RegisterMigration(12, 11, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return values, nil
	}); !errors.Is(err, ErrVersionedTupleMigrationCycle) {
		t.Fatalf("cyclic migration error = %v, want %v", err, ErrVersionedTupleMigrationCycle)
	}
	if err := manager.RegisterMigration(11, 12, func(values []TupleFieldValue) ([]TupleFieldValue, error) {
		return values, nil
	}); !errors.Is(err, ErrVersionedTupleMigrationDuplicate) {
		t.Fatalf("duplicate migration error = %v, want %v", err, ErrVersionedTupleMigrationDuplicate)
	}
	if _, err := manager.Migrate(original); !errors.Is(err, ErrVersionedTupleMigrationCallback) {
		t.Fatalf("callback migration error = %v, want %v", err, ErrVersionedTupleMigrationCallback)
	}
	values, err := formatV1.Unpack(original.Tuple())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(values, []TupleFieldValue{TupleString("before")}) {
		t.Fatalf("original values after rejected migration = %#v", values)
	}
	if _, err := manager.Plan(999, 12); !errors.Is(err, ErrVersionedTupleMigrationFormat) {
		t.Fatalf("unknown source plan error = %v, want %v", err, ErrVersionedTupleMigrationFormat)
	}
}

func mustMigrationFormat(t *testing.T, version uint64, fields []TupleFieldSpec) TupleFormat {
	t.Helper()
	format, err := NewTupleFormat(version, fields)
	if err != nil {
		t.Fatal(err)
	}
	return format
}
