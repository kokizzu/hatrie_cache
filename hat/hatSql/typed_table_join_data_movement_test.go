package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableJoinDataMovementAccountsOfferedChanges(t *testing.T) {
	left := newDataMovementJoinTable(t, "movement-left")
	right := newDataMovementJoinTable(t, "movement-right")
	join, err := hatSql.NewTypedTableJoinWithOptions(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"}, hatSql.TypedTableJoinOptions{TrackDataMovement: true})
	if err != nil {
		t.Fatal(err)
	}

	leftInsert := dataMovementJoinUpsert(t, left, "left", "red", 7)
	leftUpdate := dataMovementJoinUpsert(t, left, "left", "blue", 9)
	rightInsert := dataMovementJoinUpsert(t, right, "right", "blue", 3)
	if err := join.ApplyLeft([]hatSql.TypedTableChange{leftInsert, leftUpdate}); err != nil {
		t.Fatal(err)
	}
	if err := join.ApplyRight([]hatSql.TypedTableChange{rightInsert}); err != nil {
		t.Fatal(err)
	}

	movement := join.DataMovement()
	if movement.LeftChanges != 2 || movement.LeftRows != 3 || movement.LeftBytes != 106 {
		t.Fatalf("left movement = %#v, want two changes, three row images, and 106 bytes", movement)
	}
	if movement.RightChanges != 1 || movement.RightRows != 1 || movement.RightBytes != 43 {
		t.Fatalf("right movement = %#v, want one change, one row image, and 43 bytes", movement)
	}
	if len(join.Rows()) != 1 {
		t.Fatalf("join rows = %d, want one matching row", len(join.Rows()))
	}

	if err := join.ApplyLeft([]hatSql.TypedTableChange{leftInsert}); err != nil {
		t.Fatal(err)
	}
	if got := join.DataMovement().LeftChanges; got != 3 {
		t.Fatalf("stale offered change count = %d, want 3", got)
	}
}

func TestTypedTableJoinArrangementDataMovementMatchesJoin(t *testing.T) {
	left := newDataMovementJoinTable(t, "movement-arrangement-left")
	right := newDataMovementJoinTable(t, "movement-arrangement-right")
	arrangements, err := hatSql.NewTypedTableJoinArrangementsWithOptions(left, right, hatSql.TypedTableJoinOptions{TrackDataMovement: true})
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()

	leftChange := dataMovementJoinUpsert(t, left, "left", "red", 7)
	if err := arrangement.ApplyLeft([]hatSql.TypedTableChange{leftChange}); err != nil {
		t.Fatal(err)
	}
	movement, err := arrangement.DataMovement()
	if err != nil {
		t.Fatal(err)
	}
	if movement.LeftChanges != 1 || movement.LeftRows != 1 || movement.LeftBytes == 0 {
		t.Fatalf("arrangement movement = %#v", movement)
	}
}

func TestTypedTableJoinDataMovementIsDisabledByDefault(t *testing.T) {
	left := newDataMovementJoinTable(t, "movement-default-left")
	right := newDataMovementJoinTable(t, "movement-default-right")
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	change := dataMovementJoinUpsert(t, left, "left", "red", 7)
	if err := join.ApplyLeft([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	if got := join.DataMovement(); got != (hatSql.TypedTableJoinDataMovement{}) {
		t.Fatalf("default data movement = %#v, want zero", got)
	}
}

func newDataMovementJoinTable(t testing.TB, name string) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "amount", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func dataMovementJoinUpsert(t *testing.T, table *hatSql.TypedTable, key, team string, amount int64) hatSql.TypedTableChange {
	t.Helper()
	change, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedInt64(amount)})
	if err != nil {
		t.Fatal(err)
	}
	return change
}
