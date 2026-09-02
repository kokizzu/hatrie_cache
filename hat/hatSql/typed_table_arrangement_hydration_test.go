package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementHydratesInBoundedBatches(t *testing.T) {
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
	for key, values := range map[string][]hatSql.TypedTableValue{
		"ada": {hatSql.TypedString("red"), hatSql.TypedInt64(1)},
		"lin": {hatSql.TypedString("blue"), hatSql.TypedInt64(2)},
		"max": {hatSql.TypedString("red"), hatSql.TypedInt64(3)},
	} {
		if _, err := table.Upsert(key, values); err != nil {
			t.Fatal(err)
		}
	}

	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()

	if freshness, err := arrangement.Freshness(); err != nil || freshness.Checkpoint != 0 || freshness.SourceSequence != 3 || !freshness.Stale {
		t.Fatalf("initial Freshness() = %#v/%v", freshness, err)
	}
	first, err := arrangement.Hydrate(2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Before != 0 || first.After != 2 || first.SourceSequence != 3 || first.Applied != 2 || first.Complete {
		t.Fatalf("first Hydrate() = %#v", first)
	}
	second, err := arrangement.Hydrate(0)
	if err != nil {
		t.Fatal(err)
	}
	if second.Before != 2 || second.After != 3 || second.SourceSequence != 3 || second.Applied != 1 || !second.Complete {
		t.Fatalf("second Hydrate() = %#v", second)
	}
	got := arrangement.Rows()
	gotByTeam := make(map[string]hatSql.Row, len(got))
	for _, row := range got {
		gotByTeam[row["team"].(string)] = row
	}
	wantByTeam := map[string]hatSql.Row{
		"blue": {"team": "blue", "count": int64(1), "sum": float64(2)},
		"red":  {"team": "red", "count": int64(2), "sum": float64(4)},
	}
	if !reflect.DeepEqual(gotByTeam, wantByTeam) {
		t.Fatalf("Rows() = %#v, want groups %#v", got, wantByTeam)
	}

	if _, err := table.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(5)}); err != nil {
		t.Fatal(err)
	}
	if freshness, err := arrangement.Freshness(); err != nil || freshness.Checkpoint != 3 || freshness.SourceSequence != 4 || !freshness.Stale {
		t.Fatalf("updated Freshness() = %#v/%v", freshness, err)
	}
	if report, err := arrangement.Hydrate(1); err != nil || report.After != 4 || !report.Complete {
		t.Fatalf("updated Hydrate() = %#v/%v", report, err)
	}

	if err := table.CompactChangesThrough(4); err != nil {
		t.Fatal(err)
	}
	lagging, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	defer lagging.Release()
	if _, err := lagging.Hydrate(1); !errors.Is(err, hatSql.ErrTypedTableChangesCompacted) {
		t.Fatalf("lagging Hydrate() error = %v, want ErrTypedTableChangesCompacted", err)
	}
}

func TestTypedTableJoinArrangementHydratesBothInputs(t *testing.T) {
	left, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "left",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	right, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "right",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()

	if _, err := left.Upsert("left-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	freshness, err := arrangement.Freshness()
	if err != nil {
		t.Fatal(err)
	}
	if freshness.LeftCheckpoint != 0 || freshness.LeftSourceSequence != 1 || freshness.RightCheckpoint != 0 || freshness.RightSourceSequence != 1 || !freshness.Stale {
		t.Fatalf("Freshness() = %#v", freshness)
	}
	first, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeftBefore != 0 || first.LeftAfter != 1 || first.LeftApplied != 1 || first.RightBefore != 0 || first.RightAfter != 1 || first.RightApplied != 1 || !first.Complete {
		t.Fatalf("first Hydrate() = %#v", first)
	}
	if got := arrangement.Rows(); len(got) != 1 || got[0].LeftKey != "left-red" || got[0].RightKey != "right-red" {
		t.Fatalf("Rows() = %#v", got)
	}

	if _, err := left.Upsert("left-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	second, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if second.LeftAfter != 2 || second.RightAfter != 2 || !second.Complete {
		t.Fatalf("second Hydrate() = %#v", second)
	}
	if got := arrangement.Rows(); len(got) != 2 {
		t.Fatalf("Rows() after second Hydrate() = %#v", got)
	}
}
