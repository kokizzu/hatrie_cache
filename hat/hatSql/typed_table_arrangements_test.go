package hatSql_test

import (
	"reflect"
	"sync"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateArrangementsShareExactStateAndRelease(t *testing.T) {
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
	if _, err := table.Upsert("ada", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(4)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("lin", []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(6)}); err != nil {
		t.Fatal(err)
	}
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	definition := hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
	first, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := arrangements.Acquire(definition)
	if err != nil {
		t.Fatal(err)
	}
	if arrangements.Active() != 1 {
		t.Fatalf("Active() = %d, want 1 shared arrangement", arrangements.Active())
	}
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Apply(changes); err != nil {
		t.Fatal(err)
	}
	if second.Checkpoint() != 2 {
		t.Fatalf("shared Checkpoint() = %d, want 2", second.Checkpoint())
	}
	if got, want := second.Rows(), []hatSql.Row{{"team": "red", "count": int64(2), "sum": float64(10)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("shared Rows() = %#v, want %#v", got, want)
	}
	if !first.Release() || arrangements.Active() != 1 {
		t.Fatalf("first release active arrangements = %d", arrangements.Active())
	}
	if !second.Release() || arrangements.Active() != 0 {
		t.Fatalf("second release active arrangements = %d", arrangements.Active())
	}
	if second.Release() {
		t.Fatal("second Release() unexpectedly succeeded twice")
	}
}

func TestTypedTableAggregateArrangementConcurrentApplyIsExact(t *testing.T) {
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
	for index := 0; index < 16; index++ {
		if _, err := table.Upsert("row-"+string(rune('A'+index)), []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, 16)
	if err != nil {
		t.Fatal(err)
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
	errors := make(chan error, 8)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			errors <- arrangement.Apply(changes)
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if arrangement.Checkpoint() != 16 {
		t.Fatalf("Checkpoint() = %d, want 16", arrangement.Checkpoint())
	}
	if got, want := arrangement.Rows(), []hatSql.Row{{"team": "red", "count": int64(16), "sum": float64(16)}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
}

func TestTypedTableAggregateArrangementsKeepDifferentDefinitionsSeparate(t *testing.T) {
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
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	count, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	sum, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatal(err)
	}
	if arrangements.Active() != 2 {
		t.Fatalf("Active() = %d, want 2 distinct arrangements", arrangements.Active())
	}
	if _, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: " "}); err == nil {
		t.Fatal("Acquire() unexpectedly accepted a whitespace-only sum field")
	}
	count.Release()
	sum.Release()
}
