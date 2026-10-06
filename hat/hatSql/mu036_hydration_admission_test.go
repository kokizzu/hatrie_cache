package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementHydrationAdmissionWaitsForTarget(t *testing.T) {
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

	status, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Checkpoint != 0 || status.SourceSequence != 3 || status.Target != 3 || status.Ready || !status.Stale {
		t.Fatalf("initial HydrationStatus() = %#v", status)
	}
	first, err := arrangement.Hydrate(2)
	if err != nil {
		t.Fatal(err)
	}
	if first.Complete {
		t.Fatalf("first Hydrate() = %#v, want incomplete", first)
	}
	status, err = arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Checkpoint != 2 || status.SourceSequence != 3 || status.Ready || !status.Stale {
		t.Fatalf("partial HydrationStatus() = %#v", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		status hatSql.TypedTableAggregateArrangementHydrationStatus
		err    error
	}, 1)
	go func() {
		ready, err := arrangement.WaitReady(ctx, 3)
		result <- struct {
			status hatSql.TypedTableAggregateArrangementHydrationStatus
			err    error
		}{ready, err}
	}()
	second, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Complete {
		t.Fatalf("second Hydrate() = %#v, want complete", second)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.status.Ready || got.status.Checkpoint != 3 || got.status.Target != 3 {
			t.Fatalf("WaitReady() = %#v, want ready status", got)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady() did not observe hydration completion")
	}

	if _, err := arrangement.WaitReady(context.Background(), 4); !errors.Is(err, hatSql.ErrTypedTableArrangementHydrationTargetAhead) {
		t.Fatalf("WaitReady() ahead error = %v, want target-ahead error", err)
	}
	if _, err := table.Upsert("zoe", []hatSql.TypedTableValue{hatSql.TypedString("green"), hatSql.TypedInt64(4)}); err != nil {
		t.Fatal(err)
	}
	status, err = arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Checkpoint != 3 || status.SourceSequence != 4 || status.Ready || !status.Stale {
		t.Fatalf("post-write HydrationStatus() = %#v", status)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := arrangement.WaitReady(canceled, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WaitReady() error = %v, want context.Canceled", err)
	}
}

func TestTypedTableJoinArrangementHydrationAdmissionWaitsForBothInputs(t *testing.T) {
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
	if status.LeftCheckpoint != 0 || status.RightCheckpoint != 0 || status.LeftSourceSequence != 1 || status.RightSourceSequence != 1 || status.Ready || !status.Stale {
		t.Fatalf("initial join HydrationStatus() = %#v", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan struct {
		status hatSql.TypedTableJoinArrangementHydrationStatus
		err    error
	}, 1)
	go func() {
		ready, err := arrangement.WaitReady(ctx, 1, 1)
		result <- struct {
			status hatSql.TypedTableJoinArrangementHydrationStatus
			err    error
		}{ready, err}
	}()
	if report, err := arrangement.Hydrate(1); err != nil || !report.Complete {
		t.Fatalf("Hydrate() = %#v/%v, want complete", report, err)
	}
	select {
	case got := <-result:
		if got.err != nil || !got.status.Ready || got.status.LeftCheckpoint != 1 || got.status.RightCheckpoint != 1 {
			t.Fatalf("join WaitReady() = %#v, want ready status", got)
		}
	case <-time.After(time.Second):
		t.Fatal("join WaitReady() did not observe hydration completion")
	}
}
