package hatSql_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateHydrationAdmission(t *testing.T) {
	table := newHydrationAdmissionTable(t, "scores")
	if _, err := table.Upsert("one", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("two", []hatSql.TypedTableValue{hatSql.TypedString("blue")}); err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()

	status, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Ready || status.Checkpoint != 0 || status.SourceSequence != 2 || status.Pending != 2 {
		t.Fatalf("initial hydration status = %#v", status)
	}
	if err := status.Admit(); !errors.Is(err, hatSql.ErrTypedTableArrangementNotReady) {
		t.Fatalf("initial admission error = %v", err)
	}

	ready, err := arrangement.HydrateUntilReady(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.Pending != 0 || ready.Checkpoint != 2 || ready.SourceSequence != 2 {
		t.Fatalf("ready hydration status = %#v", ready)
	}
	if err := ready.Admit(); err != nil {
		t.Fatalf("ready admission error = %v", err)
	}
	if _, err := arrangement.HydrateUntilReady(context.Background(), -1); err == nil {
		t.Fatal("negative hydration batch unexpectedly succeeded")
	}

	if _, err := table.Upsert("three", []hatSql.TypedTableValue{hatSql.TypedString("green")}); err != nil {
		t.Fatal(err)
	}
	stale, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if stale.Ready || stale.Pending != 1 || stale.Checkpoint != 2 || stale.SourceSequence != 3 {
		t.Fatalf("stale hydration status = %#v", stale)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := arrangement.HydrateUntilReady(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hydration error = %v", err)
	}
}

func TestTypedTableJoinHydrationAdmission(t *testing.T) {
	left := newHydrationAdmissionTable(t, "left")
	right := newHydrationAdmissionTable(t, "right")
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

	status, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Ready || status.LeftPending != 1 || status.RightPending != 1 {
		t.Fatalf("initial join hydration status = %#v", status)
	}
	if err := status.Admit(); !errors.Is(err, hatSql.ErrTypedTableArrangementNotReady) {
		t.Fatalf("initial join admission error = %v", err)
	}
	ready, err := arrangement.HydrateUntilReady(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.LeftPending != 0 || ready.RightPending != 0 {
		t.Fatalf("ready join hydration status = %#v", ready)
	}
	if err := ready.Admit(); err != nil {
		t.Fatalf("ready join admission error = %v", err)
	}
}

func newHydrationAdmissionTable(t *testing.T, name string) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    name,
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}
