package hatSql

import (
	"reflect"
	"strconv"
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
	if aggregate.compactionCount != 1 {
		t.Fatalf("new group compaction count = %d, want 1", aggregate.compactionCount)
	}
}

func TestMZ028SingleNewGroupMaintainsOrderIncrementally(t *testing.T) {
	const initialGroups = 256
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events_incremental",
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
	changes := make([]TypedTableChange, initialGroups)
	for index := range changes {
		changes[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After:    []TypedTableValue{TypedString("team-" + strconv.Itoa(index)), TypedInt64(int64(index))},
		}
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	if got := aggregate.Rows(); len(got) != initialGroups {
		t.Fatalf("initial rows = %d, want %d", len(got), initialGroups)
	}
	compactions := aggregate.compactionCount
	if err := aggregate.Apply([]TypedTableChange{{
		Sequence: initialGroups + 1,
		After:    []TypedTableValue{TypedString("team-128-new"), TypedInt64(128)},
	}}); err != nil {
		t.Fatal(err)
	}
	rows := aggregate.Rows()
	if aggregate.compactionCount != compactions {
		t.Fatalf("single new group compactions = %d, want unchanged %d", aggregate.compactionCount, compactions)
	}
	if len(rows) != initialGroups+1 {
		t.Fatalf("rows after new group = %d, want %d", len(rows), initialGroups+1)
	}
	expectedTable, err := NewTypedTable(TypedTableSchema{
		Name: "events_incremental_expected",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedAggregate, err := NewTypedTableAggregate(expectedTable, TypedTableAggregateDefinition{
		GroupBy:  []string{"team"},
		SumField: "points",
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedChanges := append(append([]TypedTableChange(nil), changes...), TypedTableChange{
		Sequence: initialGroups + 1,
		After:    []TypedTableValue{TypedString("team-128-new"), TypedInt64(128)},
	})
	if err := expectedAggregate.Apply(expectedChanges); err != nil {
		t.Fatal(err)
	}
	if want := expectedAggregate.Rows(); !reflect.DeepEqual(rows, want) {
		t.Fatalf("incremental rows = %#v, want full-sort rows %#v", rows, want)
	}
}

func TestMZ028BatchedNewGroupsKeepFullCompactionFallback(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events_batch",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"team"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString("a"), TypedInt64(1)}},
		{Sequence: 2, After: []TypedTableValue{TypedString("b"), TypedInt64(2)}},
	}); err != nil {
		t.Fatal(err)
	}
	aggregate.Rows()
	if err := aggregate.Apply([]TypedTableChange{
		{Sequence: 3, After: []TypedTableValue{TypedString("c"), TypedInt64(3)}},
		{Sequence: 4, After: []TypedTableValue{TypedString("d"), TypedInt64(4)}},
	}); err != nil {
		t.Fatal(err)
	}
	aggregate.Rows()
	if aggregate.compactionCount != 2 {
		t.Fatalf("batched additions compaction count = %d, want 2", aggregate.compactionCount)
	}
}

var mz028GroupOrderBenchmarkRows []Row

func BenchmarkMZ028GroupOrderSingleInsert(b *testing.B) {
	const initialGroups = 2048
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{
		GroupBy:  []string{"team"},
		SumField: "points",
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]TypedTableChange, initialGroups)
	for index := range changes {
		changes[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After:    []TypedTableValue{TypedString("team-" + strconv.Itoa(index)), TypedInt64(int64(index))},
		}
	}
	if err := aggregate.Apply(changes); err != nil {
		b.Fatal(err)
	}
	mz028GroupOrderBenchmarkRows = aggregate.Rows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := aggregate.Apply([]TypedTableChange{{
			Sequence: uint64(initialGroups + index + 1),
			After:    []TypedTableValue{TypedString("team-new-" + strconv.Itoa(index)), TypedInt64(int64(index))},
		}}); err != nil {
			b.Fatal(err)
		}
		mz028GroupOrderBenchmarkRows = aggregate.Rows()
	}
}
