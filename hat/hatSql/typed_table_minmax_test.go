package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateMaintainsExactMinMaxAcrossUpdatesAndDeletes(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "scores",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key    string
		points int64
	}{{"a", 4}, {"b", 4}, {"c", 9}} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(row.points)}); err != nil {
			t.Fatal(err)
		}
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, MinField: "points", MaxField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	assertTypedTableMinMax(t, aggregate.Rows(), int64(4), int64(9))

	if _, err := table.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(7)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	changes, _, err = table.ChangesAfter(3, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	assertTypedTableMinMax(t, aggregate.Rows(), int64(7), int64(9))

	if _, err := table.Delete("c"); err != nil {
		t.Fatal(err)
	}
	changes, _, err = table.ChangesAfter(5, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	assertTypedTableMinMax(t, aggregate.Rows(), int64(7), int64(7))
}

func TestTypedTableAggregateArrangementsDoNotShareDifferentMinMaxDefinitions(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "scores", Columns: []hatSql.TypedTableColumn{{Name: "points", Kind: hatSql.TypedTableInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	minimum, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{MinField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	defer minimum.Release()
	maximum, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{MaxField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	defer maximum.Release()
	if arrangements.Active() != 2 {
		t.Fatalf("active arrangements = %d, want distinct min/max definitions", arrangements.Active())
	}
}

func assertTypedTableMinMax(t *testing.T, rows []hatSql.Row, minimum, maximum int64) {
	t.Helper()
	if len(rows) != 1 || rows[0]["count"] == nil || rows[0]["min"] != minimum || rows[0]["max"] != maximum {
		t.Fatalf("aggregate rows = %#v, want min/max %d/%d", rows, minimum, maximum)
	}
}
