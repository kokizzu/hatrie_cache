package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementHydrationStatusBlocksUntilReady(t *testing.T) {
	table := newM036HydrationTable(t, "scores")
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

	initial, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if initial.Ready || initial.State != hatSql.TypedTableArrangementHydrationStateStale || initial.Checkpoint != 0 || initial.SourceSequence != 2 || initial.Remaining != 2 {
		t.Fatalf("initial HydrationStatus() = %#v", initial)
	}
	waitResult := make(chan struct {
		status hatSql.TypedTableAggregateArrangementHydrationStatus
		err    error
	}, 1)
	go func() {
		status, err := arrangement.WaitForHydration(context.Background())
		waitResult <- struct {
			status hatSql.TypedTableAggregateArrangementHydrationStatus
			err    error
		}{status: status, err: err}
	}()
	select {
	case result := <-waitResult:
		t.Fatalf("wait returned while arrangement was stale: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}

	first, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Applied != 1 || first.Complete {
		t.Fatalf("first Hydrate() = %#v", first)
	}
	middle, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if middle.Ready || middle.Remaining != 1 || middle.Checkpoint != 1 || middle.SourceSequence != 2 {
		t.Fatalf("middle HydrationStatus() = %#v", middle)
	}
	select {
	case result := <-waitResult:
		t.Fatalf("wait returned before final batch: %#v", result)
	case <-time.After(20 * time.Millisecond):
	}

	second, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if second.Applied != 1 || !second.Complete {
		t.Fatalf("second Hydrate() = %#v", second)
	}
	select {
	case result := <-waitResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if !result.status.Ready || result.status.State != hatSql.TypedTableArrangementHydrationStateReady || result.status.Remaining != 0 || result.status.Generation <= initial.Generation {
			t.Fatalf("ready status = %#v", result.status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for arrangement hydration")
	}

	if _, err := table.Upsert("max", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(3)}); err != nil {
		t.Fatal(err)
	}
	stale, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if stale.Ready || stale.State != hatSql.TypedTableArrangementHydrationStateStale || stale.Remaining != 1 || stale.SourceSequence != 3 {
		t.Fatalf("post-write HydrationStatus() = %#v", stale)
	}
}

func TestTypedTableJoinArrangementHydrationStatusTracksBothInputs(t *testing.T) {
	left := newM036HydrationTable(t, "left")
	right := newM036HydrationTable(t, "right")
	arrangements, err := hatSql.NewTypedTableJoinArrangements(left, right)
	if err != nil {
		t.Fatal(err)
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableJoinDefinition{LeftField: "team", RightField: "team"})
	if err != nil {
		t.Fatal(err)
	}
	defer arrangement.Release()
	if _, err := left.Upsert("left-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-red", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)}); err != nil {
		t.Fatal(err)
	}

	initial, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if initial.Ready || initial.LeftRemaining != 1 || initial.RightRemaining != 1 || initial.State != hatSql.TypedTableArrangementHydrationStateStale {
		t.Fatalf("initial join HydrationStatus() = %#v", initial)
	}
	hydration, err := arrangement.Hydrate(1)
	if err != nil {
		t.Fatal(err)
	}
	if !hydration.Complete {
		t.Fatalf("join Hydrate() = %#v", hydration)
	}
	ready, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if !ready.Ready || ready.LeftCheckpoint != 1 || ready.RightCheckpoint != 1 || ready.LeftRemaining != 0 || ready.RightRemaining != 0 || ready.State != hatSql.TypedTableArrangementHydrationStateReady {
		t.Fatalf("ready join HydrationStatus() = %#v", ready)
	}

	if _, err := right.Upsert("right-blue", []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(4)}); err != nil {
		t.Fatal(err)
	}
	stale, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if stale.Ready || stale.LeftRemaining != 0 || stale.RightRemaining != 1 || stale.State != hatSql.TypedTableArrangementHydrationStateStale {
		t.Fatalf("stale join HydrationStatus() = %#v", stale)
	}
}

func TestTypedTableArrangementHydrationFailureAndContextCancellation(t *testing.T) {
	table := newM036HydrationTable(t, "scores")
	if _, err := table.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}); err != nil {
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
	if err := table.CompactChangesThrough(1); err != nil {
		t.Fatal(err)
	}
	if _, err := arrangement.Hydrate(1); !errors.Is(err, hatSql.ErrTypedTableChangesCompacted) {
		t.Fatalf("Hydrate() error = %v", err)
	}
	failed, err := arrangement.HydrationStatus()
	if err != nil {
		t.Fatal(err)
	}
	if !failed.Failed || failed.Ready || failed.State != hatSql.TypedTableArrangementHydrationStateFailed {
		t.Fatalf("failed HydrationStatus() = %#v", failed)
	}
	if _, err := arrangement.WaitForHydration(context.Background()); !errors.Is(err, hatSql.ErrTypedTableArrangementHydrationFailed) {
		t.Fatalf("WaitForHydration() error = %v", err)
	}

	cancelTable := newM036HydrationTable(t, "cancel")
	if _, err := cancelTable.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	cancelArrangements, err := hatSql.NewTypedTableAggregateArrangements(cancelTable)
	if err != nil {
		t.Fatal(err)
	}
	cancelArrangement, err := cancelArrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelArrangement.Release()
	ctx, cancel := context.WithCancel(context.Background())
	waitResult := make(chan error, 1)
	go func() {
		_, err := cancelArrangement.WaitForHydration(ctx)
		waitResult <- err
	}()
	select {
	case err := <-waitResult:
		t.Fatalf("wait returned before cancellation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-waitResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait cancellation error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
}

func newM036HydrationTable(t *testing.T, name string) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if table == nil {
		t.Fatal("NewTypedTable returned an invalid table")
	}
	return table
}
