package hatSql

import (
	"reflect"
	"testing"
)

func TestMZ028LegacyAggregateCachesOrderedGroupReferences(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{
		GroupBy:  []string{"team"},
		SumField: "points",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString("beta"), TypedInt64(2)}},
		{Sequence: 2, After: []TypedTableValue{TypedString("alpha"), TypedInt64(3)}},
	}); err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"team": "beta", "count": int64(1), "sum": float64(2)},
		{"team": "alpha", "count": int64(1), "sum": float64(3)},
	}
	first := aggregate.Rows()
	if !reflect.DeepEqual(first, want) {
		t.Fatalf("first Rows() = %#v, want %#v", first, want)
	}
	if aggregate.compactionCount != 1 {
		t.Fatalf("first Rows() compaction count = %d, want 1", aggregate.compactionCount)
	}
	second := aggregate.Rows()
	if !reflect.DeepEqual(second, want) {
		t.Fatalf("cached Rows() = %#v, want %#v", second, want)
	}
	if aggregate.compactionCount != 1 {
		t.Fatalf("cached Rows() compaction count = %d, want 1", aggregate.compactionCount)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 3, After: []TypedTableValue{TypedString("beta"), TypedInt64(5)}},
	}); err != nil {
		t.Fatal(err)
	}
	third := aggregate.Rows()
	want[0]["count"] = int64(2)
	want[0]["sum"] = float64(7)
	if !reflect.DeepEqual(third, want) {
		t.Fatalf("Rows() after existing group update = %#v, want %#v", third, want)
	}
	if aggregate.compactionCount != 1 {
		t.Fatalf("existing group update compaction count = %d, want 1", aggregate.compactionCount)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 4, After: []TypedTableValue{TypedString("gamma"), TypedInt64(4)}},
	}); err != nil {
		t.Fatal(err)
	}
	fourth := aggregate.Rows()
	want = append(want, Row{"team": "gamma", "count": int64(1), "sum": float64(4)})
	if !reflect.DeepEqual(fourth, want) {
		t.Fatalf("Rows() after new group = %#v, want %#v", fourth, want)
	}
	if aggregate.compactionCount != 2 {
		t.Fatalf("new group compaction count = %d, want 2", aggregate.compactionCount)
	}
}
