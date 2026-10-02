package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestMU036HydrationAdmissionBlocksUntilAllArrangementsAreReady(t *testing.T) {
	admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := admission.Register("orders", 0, 5); err != nil {
		t.Fatal(err)
	}
	if err := admission.Register("customers", 0, 3); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- admission.Admit(context.Background(), "customers", "orders")
	}()
	<-started
	select {
	case err := <-result:
		t.Fatalf("Admit returned before readiness: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	if err := admission.Complete("orders", 5); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		t.Fatalf("Admit returned before all readiness: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := admission.Complete("customers", 3); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("Admit() error = %v", err)
	}

	snapshot := admission.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Key != "customers" || snapshot[1].Key != "orders" {
		t.Fatalf("Snapshot() = %#v, want deterministic key order", snapshot)
	}
	for _, progress := range snapshot {
		if progress.State != hatSql.TypedTableArrangementHydrationReady || progress.Pending != 0 {
			t.Fatalf("ready progress = %#v", progress)
		}
	}
}

func TestMU036AggregateHydrationUpdatesProgressAndAdmission(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "scores",
		Columns: []hatSql.TypedTableColumn{{Name: "team", Kind: hatSql.TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, team := range map[string]string{"a": "red", "b": "blue", "c": "green"} {
		if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(team)}); err != nil {
			t.Fatal(err)
		}
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
	admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(4)
	if err != nil {
		t.Fatal(err)
	}

	first, err := admission.HydrateAggregate(context.Background(), "scores_by_team", arrangement, 2)
	if err != nil {
		t.Fatal(err)
	}
	if first.After != 2 || first.SourceSequence != 3 || first.Complete {
		t.Fatalf("first hydration = %#v", first)
	}
	progress := admission.Snapshot()
	if len(progress) != 1 || progress[0].State != hatSql.TypedTableArrangementHydrationHydrating || progress[0].Checkpoint != 2 || progress[0].SourceSequence != 3 || progress[0].Pending != 1 {
		t.Fatalf("partial progress = %#v", progress)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := admission.Admit(ctx, "scores_by_team"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Admit() error = %v, want context deadline", err)
	}
	second, err := admission.HydrateAggregate(context.Background(), "scores_by_team", arrangement, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.After != 3 || !second.Complete {
		t.Fatalf("complete hydration = %#v", second)
	}
	if err := admission.Admit(context.Background(), "scores_by_team"); err != nil {
		t.Fatalf("ready Admit() error = %v", err)
	}
}

func TestMU036HydrationAdmissionFailureCanBeReRegistered(t *testing.T) {
	admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := admission.Register("orders", 0, 1); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("replay gap")
	if err := admission.Fail("orders", failure); err != nil {
		t.Fatal(err)
	}
	if err := admission.Admit(context.Background(), "orders"); !errors.Is(err, hatSql.ErrTypedTableArrangementHydrationFailed) || !errors.Is(err, failure) {
		t.Fatalf("failed Admit() error = %v", err)
	}
	if err := admission.Register("orders", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := admission.Admit(context.Background(), "orders"); err != nil {
		t.Fatalf("re-registered Admit() error = %v", err)
	}

	if err := admission.Register("z", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := admission.Register("a", 1, 1); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(admission.Snapshot()))
	for _, progress := range admission.Snapshot() {
		keys = append(keys, progress.Key)
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("Snapshot keys = %#v, want sorted %#v", keys, want)
	}
}

func TestMU036JoinHydrationPublishesBothInputProgresses(t *testing.T) {
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
	if _, err := left.Upsert("left-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
		t.Fatal(err)
	}
	if _, err := right.Upsert("right-red", []hatSql.TypedTableValue{hatSql.TypedString("red")}); err != nil {
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
	admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(2)
	if err != nil {
		t.Fatal(err)
	}
	report, err := admission.HydrateJoin(context.Background(), "teams_join", arrangement, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Complete || len(arrangement.Rows()) != 1 {
		t.Fatalf("join hydration = %#v, rows = %#v", report, arrangement.Rows())
	}
	progress := admission.Snapshot()
	if len(progress) != 1 || progress[0].State != hatSql.TypedTableArrangementHydrationReady || progress[0].LeftCheckpoint != 1 || progress[0].LeftSourceSequence != 1 || progress[0].RightCheckpoint != 1 || progress[0].RightSourceSequence != 1 || progress[0].Pending != 0 {
		t.Fatalf("join progress = %#v", progress)
	}
	if err := admission.Admit(context.Background(), "teams_join"); err != nil {
		t.Fatalf("join Admit() error = %v", err)
	}
}

func TestMU036ReadySingleKeyAdmissionHasNoPerCallAllocation(t *testing.T) {
	admission, err := hatSql.NewTypedTableArrangementHydrationAdmission(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := admission.Register("ready", 1, 1); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if err := admission.Admit(context.Background(), "ready"); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("ready single-key admission allocations = %v, want 0", allocations)
	}
}
