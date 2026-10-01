package hatSql

import (
	"errors"
	"math"
	"testing"
)

func TestTypedTableFieldUpdateSetAndAdd(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "count", Kind: TypedTableInt64},
			{Name: "ratio", Kind: TypedTableFloat64},
			{Name: "label", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("event-0", []TypedTableValue{
		TypedInt64(3), TypedFloat64(1.5), TypedString("cold"),
	}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	change, err := table.Update("event-0", []TypedTableUpdate{
		{Column: "count", Kind: TypedTableUpdateAdd, Value: TypedInt64(2)},
		{Column: "ratio", Kind: TypedTableUpdateSet, Value: TypedFloat64(2.5)},
		{Column: "label", Kind: TypedTableUpdateSet, Value: TypedString("hot")},
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if change.Operation != "UPDATE" || change.Key != "event-0" {
		t.Fatalf("Update() change = %#v", change)
	}
	if change.Before[0] != TypedInt64(3) || change.After[0] != TypedInt64(5) {
		t.Fatalf("Update() count change = %#v -> %#v", change.Before[0], change.After[0])
	}
	if change.After[1] != TypedFloat64(2.5) || change.After[2] != TypedString("hot") {
		t.Fatalf("Update() after = %#v", change.After)
	}

	rows, err := table.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatalf("ResolveSQLSource() error = %v", err)
	}
	if len(rows) != 1 || rows[0]["count"] != int64(5) || rows[0]["ratio"] != 2.5 || rows[0]["label"] != "hot" {
		t.Fatalf("ResolveSQLSource() rows = %#v", rows)
	}
}

func TestTypedTableFieldUpdateRejectsInvalidOperationAtomically(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "count", Kind: TypedTableInt64},
			{Name: "label", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("event-0", []TypedTableValue{TypedInt64(9), TypedString("stable")}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	sequence := table.sequence

	_, err = table.Update("event-0", []TypedTableUpdate{
		{Column: "count", Kind: TypedTableUpdateAdd, Value: TypedInt64(1)},
		{Column: "label", Kind: TypedTableUpdateAdd, Value: TypedString("bad")},
	})
	if !errors.Is(err, ErrTypedTableUpdateInvalid) {
		t.Fatalf("Update() error = %v, want ErrTypedTableUpdateInvalid", err)
	}
	if table.sequence != sequence {
		t.Fatalf("rejected Update() sequence = %d, want %d", table.sequence, sequence)
	}
	rows, err := table.ResolveSQLSource("CACHE", "events")
	if err != nil {
		t.Fatalf("ResolveSQLSource() error = %v", err)
	}
	if len(rows) != 1 || rows[0]["count"] != int64(9) || rows[0]["label"] != "stable" {
		t.Fatalf("rejected Update() changed rows = %#v", rows)
	}

	_, err = table.Update("event-0", []TypedTableUpdate{{
		Column: "count", Kind: TypedTableUpdateAdd, Value: TypedInt64(math.MaxInt64),
	}})
	if !errors.Is(err, ErrTypedTableUpdateOverflow) {
		t.Fatalf("overflow Update() error = %v, want ErrTypedTableUpdateOverflow", err)
	}
}

func TestTypedTableFieldUpdateRejectsGeneratedColumns(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "generated",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:                  "derived",
				Kind:                  TypedTableInt64,
				GeneratedDependencies: []string{"base"},
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 2), nil
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("row-0", []TypedTableValue{TypedInt64(2), TypedNull()}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	_, err = table.Update("row-0", []TypedTableUpdate{{
		Column: "base", Kind: TypedTableUpdateAdd, Value: TypedInt64(1),
	}})
	if !errors.Is(err, ErrTypedTableUpdateGenerated) {
		t.Fatalf("Update() error = %v, want ErrTypedTableUpdateGenerated", err)
	}
}

func TestTypedTableFieldUpdateBoundsOperationCount(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "count", Kind: TypedTableInt64}},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("event-0", []TypedTableValue{TypedInt64(1)}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	operations := make([]TypedTableUpdate, typedTableMaxFieldUpdates+1)
	for index := range operations {
		operations[index] = TypedTableUpdate{Column: "count", Kind: TypedTableUpdateAdd, Value: TypedInt64(1)}
	}
	_, err = table.Update("event-0", operations)
	if !errors.Is(err, ErrTypedTableUpdateInvalid) {
		t.Fatalf("oversized Update() error = %v, want ErrTypedTableUpdateInvalid", err)
	}
}
