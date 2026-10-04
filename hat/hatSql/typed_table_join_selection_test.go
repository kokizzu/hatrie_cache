package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableJoinArrangementsAcquireBestChoosesSmallestAndMaintainsIt(t *testing.T) {
	left, right := newJoinSelectionTables(t)
	if _, err := left.Upsert("left-1", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-1", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}

	lease, selection, err := arrangements.AcquireBest([]hatSql.TypedTableJoinArrangementCandidate{
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, EstimatedStateRows: 100},
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}, EstimatedStateRows: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if selection.Definition != (hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}) || selection.Reused {
		t.Fatalf("selection = %#v, want a new id arrangement", selection)
	}
	if got := arrangements.Active(); got != 1 {
		t.Fatalf("active arrangements = %d, want one selected arrangement", got)
	}
	if got := lease.Rows(); len(got) != 1 || got[0].LeftKey != "left-1" || got[0].RightKey != "right-1" {
		t.Fatalf("selected rows = %#v, want one id match", got)
	}

	second, secondSelection, err := arrangements.AcquireBest([]hatSql.TypedTableJoinArrangementCandidate{
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, EstimatedStateRows: 1},
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}, EstimatedStateRows: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !secondSelection.Reused || secondSelection.Definition != selection.Definition {
		t.Fatalf("reuse selection = %#v, want existing id arrangement", secondSelection)
	}
	if !second.Release() {
		t.Fatal("second Release() = false")
	}

	leftUpdate, err := left.Upsert("left-1", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.ApplyLeft([]hatSql.TypedTableChange{leftUpdate}); err != nil {
		t.Fatal(err)
	}
	if got := lease.Rows(); len(got) != 0 {
		t.Fatalf("rows after left key change = %#v, want no match", got)
	}
	rightUpdate, err := right.Upsert("right-1", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.ApplyRight([]hatSql.TypedTableChange{rightUpdate}); err != nil {
		t.Fatal(err)
	}
	if got := lease.Rows(); len(got) != 1 || got[0].LeftKey != "left-1" || got[0].RightKey != "right-1" {
		t.Fatalf("rows after maintained updates = %#v, want one id match", got)
	}
	if !lease.Release() {
		t.Fatal("Release() = false")
	}
	if got := arrangements.Active(); got != 0 {
		t.Fatalf("active arrangements after release = %d, want zero", got)
	}
}

func TestTypedTableJoinArrangementsAcquireBestSkipsIncompatibleAlternatives(t *testing.T) {
	left, right := newJoinSelectionTables(t)
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	lease, selection, err := arrangements.AcquireBest([]hatSql.TypedTableJoinArrangementCandidate{
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "missing", RightField: "id"}, EstimatedStateRows: 0},
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}, EstimatedStateRows: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if selection.Definition != (hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}) {
		t.Fatalf("selection = %#v, want compatible id arrangement", selection)
	}
	if !lease.Release() {
		t.Fatal("Release() = false")
	}
}

func TestTypedTableJoinArrangementsAcquireBestRequiresCandidates(t *testing.T) {
	left, right := newJoinSelectionTables(t)
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := arrangements.AcquireBest(nil); err == nil {
		t.Fatal("AcquireBest(nil) = nil error, want validation error")
	}
	if got := arrangements.Active(); got != 0 {
		t.Fatalf("active arrangements after rejected request = %d, want zero", got)
	}
}

func TestTypedTableJoinArrangementsAcquireBestBreaksTiesDeterministically(t *testing.T) {
	left, right := newJoinSelectionTables(t)
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	lease, selection, err := arrangements.AcquireBest([]hatSql.TypedTableJoinArrangementCandidate{
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, EstimatedStateRows: 7},
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, EstimatedStateRows: 3},
		{Definition: hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}, EstimatedStateRows: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if selection.Definition != (hatSql.TypedTableJoinDefinition{LeftField: "id", RightField: "id"}) || selection.EstimatedStateRows != 3 {
		t.Fatalf("selection = %#v, want deterministic id tie winner", selection)
	}
	if !lease.Release() {
		t.Fatal("Release() = false")
	}
}

func newJoinSelectionTables(t testing.TB) (*hatSql.TypedTable, *hatSql.TypedTable) {
	t.Helper()
	schema := func(name string) hatSql.TypedTableSchema {
		return hatSql.TypedTableSchema{
			Name: name,
			Columns: []hatSql.TypedTableColumn{
				{Name: "team", Kind: hatSql.TypedTableString},
				{Name: "id", Kind: hatSql.TypedTableInt64},
			},
		}
	}
	left, err := hatSql.NewTypedTable(schema("join-selection-left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(schema("join-selection-right"))
	if err != nil {
		t.Fatal(err)
	}
	return left, right
}
