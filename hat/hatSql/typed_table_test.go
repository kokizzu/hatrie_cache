package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableChangefeedAndExactAggregate(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(5)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("b"); err != nil {
		t.Fatal(err)
	}

	changes, last, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if last != 4 || len(changes) != 4 || changes[0].Sequence != 1 || changes[2].Before[0].String != "red" || changes[2].After[0].String != "blue" {
		t.Fatalf("changes = %#v, last = %d", changes, last)
	}
	aggregate, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	rows := aggregate.Rows()
	if len(rows) != 1 || rows[0]["team"] != "blue" || rows[0]["count"] != int64(1) || rows[0]["sum"] != float64(5) {
		t.Fatalf("aggregate rows = %#v", rows)
	}
	if err := table.CompactChangesThrough(2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := table.ChangesAfter(0, 16); !errors.Is(err, hatSql.ErrTypedTableChangesCompacted) {
		t.Fatalf("ChangesAfter(0) error = %v, want compaction gap", err)
	}
	changes, last, err = table.ChangesAfter(2, 16)
	if err != nil || last != 4 || len(changes) != 2 || changes[0].Sequence != 3 {
		t.Fatalf("ChangesAfter(2) = %#v, %d, %v", changes, last, err)
	}
	rows, err = table.ResolveSQLSource("CACHE", "events")
	if err != nil || len(rows) != 1 || rows[0]["team"] != "blue" || rows[0]["points"] != int64(5) {
		t.Fatalf("ResolveSQLSource() = %#v, %v", rows, err)
	}
	batch, available, err := table.ResolveSQLColumnarSource("CACHE", "events", []string{"team", "points"})
	team, teamOK := batch.Value("team", 0)
	points, pointsOK := batch.Value("points", 0)
	if err != nil || !available || batch.Rows != 1 || !teamOK || team != "blue" || !pointsOK || points != int64(5) {
		t.Fatalf("ResolveSQLColumnarSource() = %#v, %t, %v", batch, available, err)
	}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT team WHERE points = 5", table, nil, hatSql.QueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["team"] != "blue" {
		t.Fatalf("typed table SQL query = %#v, %v", result, err)
	}
}

func TestTypedTableRejectsSchemaAndValueMismatches(t *testing.T) {
	if _, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}, {Name: "team", Kind: hatSql.TypedTableInt64}}}); err == nil {
		t.Fatal("NewTypedTable() error = nil, want duplicate-column failure")
	}
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{Name: "events", Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("a", []hatSql.TypedTableValue{hatSql.TypedInt64(1)}); err == nil {
		t.Fatal("Upsert() error = nil, want value-kind mismatch")
	}
}
