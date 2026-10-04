package hatSql_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestMU036HydrationAdmissionBlocksAndPublishesProgress(t *testing.T) {
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationReady || progress.Generation != 0 {
		t.Fatalf("initial progress = %#v", progress)
	}
	if err := admission.Start(3); err != nil {
		t.Fatal(err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationHydrating || progress.Target != 3 || progress.Pending != 3 || progress.Generation != 1 {
		t.Fatalf("hydrating progress = %#v", progress)
	}

	type result struct {
		progress hatSql.TypedTableArrangementHydrationProgress
		err      error
	}
	ready := make(chan result, 1)
	go func() {
		progress, err := admission.WaitReady(context.Background())
		ready <- result{progress: progress, err: err}
	}()
	select {
	case got := <-ready:
		t.Fatalf("WaitReady returned before hydration: %#v", got)
	case <-time.After(10 * time.Millisecond):
	}

	if err := admission.Advance(2, 3, 2); err != nil {
		t.Fatal(err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationHydrating || progress.Checkpoint != 2 || progress.Pending != 1 || progress.Applied != 2 {
		t.Fatalf("partial progress = %#v", progress)
	}
	if err := admission.Advance(3, 3, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ready:
		if got.err != nil || got.progress.State != hatSql.TypedTableArrangementHydrationReady || got.progress.Applied != 3 {
			t.Fatalf("WaitReady result = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not unblock after hydration completed")
	}
}

func TestMU036HydrationAdmissionCancellationFailureAndReset(t *testing.T) {
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	if err := admission.Start(1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := admission.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WaitReady error = %v", err)
	}

	wantErr := errors.New("hydration failed")
	admission.Fail(wantErr)
	if _, err := admission.WaitReady(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("failed WaitReady error = %v", err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationFailed || progress.Error != wantErr.Error() {
		t.Fatalf("failed progress = %#v", progress)
	}
	admission.Fail(errors.New(strings.Repeat("x", 2048)))
	if progress := admission.Progress(); len(progress.Error) > 1024 {
		t.Fatalf("failure status error length = %d, want at most 1024", len(progress.Error))
	}
	if err := admission.Reset(); err != nil {
		t.Fatal(err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationPending || progress.Generation != 1 {
		t.Fatalf("reset progress = %#v", progress)
	}
	ready := make(chan error, 1)
	go func() {
		_, err := admission.WaitReady(context.Background())
		ready <- err
	}()
	if err := admission.Start(0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitReady did not unblock when a reset generation was already at target")
	}
	if err := admission.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := admission.Start(1); err != nil {
		t.Fatal(err)
	}
	if err := admission.Advance(1, 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMU036AggregateHydrateWithAdmission(t *testing.T) {
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
	for key, points := range map[string]int64{"ada": 1, "lin": 2, "max": 3} {
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(points)}); err != nil {
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
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	first, err := arrangement.HydrateWithAdmission(context.Background(), admission, 2)
	if err != nil || first.After != 2 {
		t.Fatalf("first HydrateWithAdmission() = %#v/%v", first, err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationHydrating || progress.Checkpoint != 2 || progress.Target != 3 {
		t.Fatalf("partial aggregate progress = %#v", progress)
	}
	second, err := arrangement.HydrateWithAdmission(context.Background(), admission, 2)
	if err != nil || !second.Complete {
		t.Fatalf("second HydrateWithAdmission() = %#v/%v", second, err)
	}
	if _, err := admission.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress := admission.Progress(); progress.State != hatSql.TypedTableArrangementHydrationReady || progress.Applied != 3 {
		t.Fatalf("complete aggregate progress = %#v", progress)
	}
}

func TestMU036JoinHydrateWithAdmission(t *testing.T) {
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
	admission := hatSql.NewTypedTableArrangementHydrationAdmission()
	if _, err := arrangement.HydrateWithAdmission(context.Background(), admission, 1); err != nil {
		t.Fatal(err)
	}
	progress := admission.Progress()
	if progress.State != hatSql.TypedTableArrangementHydrationReady || progress.Checkpoint != 2 || progress.Target != 2 || progress.Applied != 2 {
		t.Fatalf("join progress = %#v", progress)
	}
	if rows := arrangement.Rows(); len(rows) != 1 {
		t.Fatalf("join rows = %#v", rows)
	}
}
