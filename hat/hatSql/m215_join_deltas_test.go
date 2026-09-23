package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableJoinApplyLeftDeltasMaintainsRows(t *testing.T) {
	left, right := newM215JoinTables(t, "left_deltas")
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{
		LeftField: "team", RightField: "team",
	})
	if err != nil {
		t.Fatal(err)
	}

	change, err := left.Upsert("left-new", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)})
	if err != nil {
		t.Fatal(err)
	}
	deltas, err := join.ApplyLeftDeltas([]hatSql.TypedTableChange{change})
	if err != nil {
		t.Fatal(err)
	}
	assertM215JoinDeltas(t, deltas, []hatSql.TypedTableJoinDelta{
		{LeftKey: "left-new", RightKey: "right-a", Left: m215Values("red", 3), Right: m215Values("red", 10), Diff: 1},
		{LeftKey: "left-new", RightKey: "right-b", Left: m215Values("red", 3), Right: m215Values("red", 20), Diff: 1},
	})

	updated, err := left.Upsert("left-new", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(4)})
	if err != nil {
		t.Fatal(err)
	}
	deltas, err = join.ApplyLeftDeltas([]hatSql.TypedTableChange{updated})
	if err != nil {
		t.Fatal(err)
	}
	assertM215JoinDeltas(t, deltas, []hatSql.TypedTableJoinDelta{
		{LeftKey: "left-new", RightKey: "right-a", Left: m215Values("red", 3), Right: m215Values("red", 10), Diff: -1},
		{LeftKey: "left-new", RightKey: "right-b", Left: m215Values("red", 3), Right: m215Values("red", 20), Diff: -1},
	})
	if got := join.Rows(); len(got) != 2 || got[0].LeftKey != "left-existing" {
		t.Fatalf("join rows after unmatched update = %#v, want existing pair only", got)
	}
}

func TestTypedTableJoinApplyRightDeltasCoalescesSameKeyBatch(t *testing.T) {
	left, right := newM215JoinTables(t, "right_deltas")
	join, err := hatSql.NewTypedTableJoin(left, right, hatSql.TypedTableJoinDefinition{
		LeftField: "team", RightField: "team",
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := right.Upsert("right-new", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(1)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := right.Upsert("right-new", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)})
	if err != nil {
		t.Fatal(err)
	}
	deltas, err := join.ApplyRightDeltas([]hatSql.TypedTableChange{first, second})
	if err != nil {
		t.Fatal(err)
	}
	assertM215JoinDeltas(t, deltas, []hatSql.TypedTableJoinDelta{
		{LeftKey: "left-existing", RightKey: "right-new", Left: m215Values("red", 1), Right: m215Values("red", 2), Diff: 1},
	})

	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err := lease.ApplyRightDeltas(nil); err != nil {
		t.Fatal(err)
	}
}

func m215Values(team string, value int64) []hatSql.TypedTableValue {
	return []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedInt64(value)}
}

func newM215JoinTables(t testing.TB, suffix string) (*hatSql.TypedTable, *hatSql.TypedTable) {
	t.Helper()
	schema := func(name string) hatSql.TypedTableSchema {
		return hatSql.TypedTableSchema{
			Name: name + "_" + suffix,
			Columns: []hatSql.TypedTableColumn{
				{Name: "team", Kind: hatSql.TypedTableString},
				{Name: "value", Kind: hatSql.TypedTableInt64},
			},
		}
	}
	left, err := hatSql.NewTypedTable(schema("m215_left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(schema("m215_right"))
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]int64{"left-existing": 1} {
		if _, err := left.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	for key, value := range map[string]int64{"right-a": 10, "right-b": 20} {
		if _, err := right.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	return left, right
}

func assertM215JoinDeltas(t testing.TB, got, want []hatSql.TypedTableJoinDelta) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("join deltas = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index].LeftKey != want[index].LeftKey || got[index].RightKey != want[index].RightKey || got[index].Diff != want[index].Diff {
			t.Fatalf("join delta %d = %#v, want %#v", index, got[index], want[index])
		}
		if !reflect.DeepEqual(got[index].Left, want[index].Left) || !reflect.DeepEqual(got[index].Right, want[index].Right) {
			t.Fatalf("join delta %d rows = %#v/%#v, want cloned rows", index, got[index].Left, got[index].Right)
		}
	}
}
