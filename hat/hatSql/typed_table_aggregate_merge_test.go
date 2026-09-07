package hatSql_test

import (
	"errors"
	"reflect"
	"sort"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableAggregateMergesExactPartitionPartials(t *testing.T) {
	definition := hatSql.TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		SumField:      "points",
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "label",
	}
	first := newAggregateMergePartition(t, "partial-one")
	second := newAggregateMergePartition(t, "partial-two")
	targetTable := newAggregateMergePartition(t, "partial-target")
	firstAggregate := newAggregateMergePartial(t, first, definition)
	secondAggregate := newAggregateMergePartial(t, second, definition)
	target := newAggregateMergePartial(t, targetTable, definition)

	applyAggregateMergeRow(t, first, firstAggregate, "one", "red", 1, "a")
	applyAggregateMergeRow(t, first, firstAggregate, "two", "red", 3, "b")
	applyAggregateMergeRow(t, second, secondAggregate, "three", "red", 5, "a")
	applyAggregateMergeRow(t, second, secondAggregate, "four", "blue", 2, "c")

	if err := target.MergePartials(firstAggregate, secondAggregate); err != nil {
		t.Fatalf("MergePartials() error = %v", err)
	}
	rows := target.Rows()
	sort.Slice(rows, func(left, right int) bool {
		return rows[left]["team"].(string) < rows[right]["team"].(string)
	})
	want := []hatSql.Row{
		{"team": "blue", "count": int64(1), "sum": float64(2), "min": int64(2), "max": int64(2), "count_distinct": int64(1)},
		{"team": "red", "count": int64(3), "sum": float64(9), "min": int64(1), "max": int64(5), "count_distinct": int64(2)},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("merged rows = %#v, want %#v", rows, want)
	}

	if err := target.MergePartial(firstAggregate); err != nil {
		t.Fatalf("second merge error = %v", err)
	}
	rows = target.Rows()
	sort.Slice(rows, func(left, right int) bool {
		return rows[left]["team"].(string) < rows[right]["team"].(string)
	})
	if got := rows[1]["count"]; got != int64(5) {
		t.Fatalf("repeated partial count = %#v, want 5", got)
	}
}

func TestTypedTableAggregateMergeRejectsIncompatiblePartialWithoutMutation(t *testing.T) {
	table := newAggregateMergePartition(t, "merge-incompatible")
	target, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	partial, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"label"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.MergePartials(partial); !errors.Is(err, hatSql.ErrTypedTableAggregatePartialDefinition) {
		t.Fatalf("incompatible merge error = %v, want definition error", err)
	}
	if rows := target.Rows(); len(rows) != 0 {
		t.Fatalf("target rows after rejected merge = %#v, want empty", rows)
	}
}

func TestTypedTableAggregateMergeRejectsNilAndSelfPartials(t *testing.T) {
	table := newAggregateMergePartition(t, "merge-nil")
	target, err := hatSql.NewTypedTableAggregate(table, hatSql.TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.MergePartial(nil); !errors.Is(err, hatSql.ErrTypedTableAggregatePartialNil) {
		t.Fatalf("nil partial error = %v, want nil error", err)
	}
	if err := target.MergePartial(target); !errors.Is(err, hatSql.ErrTypedTableAggregatePartialSelf) {
		t.Fatalf("self partial error = %v, want self error", err)
	}
}

func newAggregateMergePartition(t *testing.T, name string) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "points", Kind: hatSql.TypedTableInt64},
			{Name: "label", Kind: hatSql.TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func newAggregateMergePartial(t *testing.T, table *hatSql.TypedTable, definition hatSql.TypedTableAggregateDefinition) *hatSql.TypedTableAggregate {
	t.Helper()
	aggregate, err := hatSql.NewTypedTableAggregate(table, definition)
	if err != nil {
		t.Fatal(err)
	}
	return aggregate
}

func applyAggregateMergeRow(t *testing.T, table *hatSql.TypedTable, aggregate *hatSql.TypedTableAggregate, key, team string, points int64, label string) {
	t.Helper()
	change, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedString(team), hatSql.TypedInt64(points), hatSql.TypedString(label)})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
}
