package hatSql_test

import (
	"strconv"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementHydrationProgressReportsETA(t *testing.T) {
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
	for index := 0; index < 4; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []hatSql.TypedTableValue{
			hatSql.TypedString("red"),
			hatSql.TypedInt64(int64(index)),
		}); err != nil {
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

	now := time.Unix(0, 0)
	progress := hatSql.NewTypedTableArrangementHydrationProgress(func() time.Time { return now })
	if _, err := arrangement.HydrateWithProgress(2, progress); err != nil {
		t.Fatal(err)
	}
	snapshot := progress.Snapshot()
	if snapshot.Completed != 2 || snapshot.Total != 4 || snapshot.Pending != 2 || snapshot.Complete {
		t.Fatalf("first progress snapshot = %#v", snapshot)
	}
	if snapshot.Elapsed != 0 || snapshot.ETA != 0 {
		t.Fatalf("first progress timing = %#v", snapshot)
	}

	now = now.Add(2 * time.Second)
	if _, err := arrangement.HydrateWithProgress(1, progress); err != nil {
		t.Fatal(err)
	}
	snapshot = progress.Snapshot()
	if snapshot.Completed != 3 || snapshot.Total != 4 || snapshot.Pending != 1 || snapshot.Complete {
		t.Fatalf("second progress snapshot = %#v", snapshot)
	}
	if snapshot.Elapsed != 2*time.Second || snapshot.ETA <= 0 || snapshot.ETA >= time.Second {
		t.Fatalf("second progress timing = %#v", snapshot)
	}

	now = now.Add(time.Second)
	if report, err := arrangement.HydrateWithProgress(1, progress); err != nil || !report.Complete {
		t.Fatalf("final HydrateWithProgress() = %#v/%v", report, err)
	}
	snapshot = progress.Snapshot()
	if snapshot.Completed != 4 || snapshot.Total != 4 || snapshot.Pending != 0 || !snapshot.Complete || snapshot.ETA != 0 {
		t.Fatalf("final progress snapshot = %#v", snapshot)
	}
}

func TestTypedTableJoinArrangementHydrationProgressAggregatesInputs(t *testing.T) {
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

	if _, err := left.Upsert("left", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}

	progress := hatSql.NewTypedTableArrangementHydrationProgress(nil)
	if _, err := arrangement.HydrateWithProgress(1, progress); err != nil {
		t.Fatal(err)
	}
	snapshot := progress.Snapshot()
	if snapshot.Completed != 2 || snapshot.Total != 2 || snapshot.Pending != 0 || !snapshot.Complete {
		t.Fatalf("join progress snapshot = %#v", snapshot)
	}
}
