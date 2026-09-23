package hatSql_test

import (
	"fmt"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableSortedArrangementsReusesCompatiblePrefix(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_registry")
	for _, row := range []struct {
		key, team string
		score     int64
	}{
		{key: "red-high", team: "red", score: 9},
		{key: "red-low", team: "red", score: 1},
		{key: "blue-high", team: "blue", score: 9},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.score),
		}); err != nil {
			t.Fatal(err)
		}
	}

	registry, err := hatSql.NewTypedTableSortedArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	composite, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
		OrderBy: []hatSql.TypedTableSortedArrangementOrder{
			{Field: "team"},
			{Field: "score", Descending: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if composite.Reused() {
		t.Fatal("first arrangement acquire was unexpectedly marked reused")
	}
	prefix, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	if !prefix.Reused() {
		t.Fatal("compatible prefix acquire was not marked reused")
	}
	if got := registry.Active(); got != 1 {
		t.Fatalf("active arrangements = %d, want one shared arrangement", got)
	}
	if got := prefix.Rows(); len(got) != 3 || got[0].Key != "blue-high" || got[1].Key != "red-high" || got[2].Key != "red-low" {
		t.Fatalf("prefix rows = %#v, want composite order", got)
	}

	change, err := table.Upsert("blue-low", []hatSql.TypedTableValue{
		hatSql.TypedString("blue"),
		hatSql.TypedInt64(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := composite.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	if prefix.Checkpoint() != composite.Checkpoint() {
		t.Fatalf("shared checkpoint = %d and %d, want equal", prefix.Checkpoint(), composite.Checkpoint())
	}
	if got := prefix.Rows(); len(got) != 4 || got[0].Key != "blue-high" || got[1].Key != "blue-low" {
		t.Fatalf("updated prefix rows = %#v, want shared update", got)
	}

	if !prefix.Release() || !composite.Release() {
		t.Fatal("expected both leases to release")
	}
	if got := registry.Active(); got != 0 {
		t.Fatalf("active arrangements after release = %d, want zero", got)
	}
}

func TestTypedTableSortedArrangementsRejectsIncompatiblePrefix(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_registry_direction")
	registry, err := hatSql.NewTypedTableSortedArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	first, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
		OrderBy: []hatSql.TypedTableSortedArrangementOrder{
			{Field: "team"},
			{Field: "score", Descending: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	second, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{Field: "team", Descending: true})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if got := registry.Active(); got != 2 {
		t.Fatalf("active arrangements = %d, want two incompatible arrangements", got)
	}
}

func TestTypedTableSortedArrangementsConcurrentAcquireRelease(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_registry_concurrent")
	if _, err := table.Upsert("one", []hatSql.TypedTableValue{hatSql.TypedString("team"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	registry, err := hatSql.NewTypedTableSortedArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	base, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{
		OrderBy: []hatSql.TypedTableSortedArrangementOrder{
			{Field: "team"},
			{Field: "score", Descending: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer base.Release()

	const workers = 32
	errorsCh := make(chan error, workers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for index := 0; index < workers; index++ {
		go func() {
			defer waitGroup.Done()
			lease, err := registry.Acquire(hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
			if err != nil {
				errorsCh <- err
				return
			}
			if !lease.Reused() || len(lease.RowsPage(0, 1)) != 1 || !lease.Release() {
				errorsCh <- fmt.Errorf("concurrent prefix lease was not reused and released")
			}
		}()
	}
	waitGroup.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if got := registry.Active(); got != 1 {
		t.Fatalf("active arrangements after concurrent leases = %d, want one base arrangement", got)
	}
}
