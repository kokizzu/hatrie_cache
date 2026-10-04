package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateHydrationStatusAndAdmission(t *testing.T) {
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

	status, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Checkpoint != 0 || status.SourceSequence != 2 || status.Remaining != 2 || status.Ready {
		t.Fatalf("initial hydration status = %#v", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- arrangement.WaitHydrated(ctx) }()
	select {
	case err := <-ready:
		t.Fatalf("WaitHydrated returned before hydration: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if report, err := arrangement.Hydrate(1); err != nil || report.Complete {
		t.Fatalf("first Hydrate() = %#v/%v", report, err)
	}
	if report, err := arrangement.Hydrate(0); err != nil || !report.Complete {
		t.Fatalf("second Hydrate() = %#v/%v", report, err)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("WaitHydrated() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitHydrated did not unblock")
	}
	status, err = arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.Checkpoint != 2 || status.Remaining != 0 {
		t.Fatalf("ready hydration status = %#v", status)
	}

	if _, err := table.Upsert("max", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := arrangement.WaitHydrated(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitHydrated(canceled) error = %v, want context.Canceled", err)
	}
}

func TestTypedTableJoinHydrationStatusAndAdmission(t *testing.T) {
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

	status, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.LeftSourceSequence != 1 || status.RightSourceSequence != 1 || status.Ready {
		t.Fatalf("initial join hydration status = %#v", status)
	}
	ready := make(chan error, 1)
	go func() { ready <- arrangement.WaitHydrated(context.Background()) }()
	select {
	case err := <-ready:
		t.Fatalf("join WaitHydrated returned before hydration: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if report, err := arrangement.Hydrate(1); err != nil || !report.Complete {
		t.Fatalf("join Hydrate() = %#v/%v", report, err)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("join WaitHydrated() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("join WaitHydrated did not unblock")
	}
	status, err = arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.LeftCheckpoint != 1 || status.RightCheckpoint != 1 {
		t.Fatalf("ready join hydration status = %#v", status)
	}
}
