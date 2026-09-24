package hatSql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestMZ028BatchedNewGroupsMergeOrderedReferences(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events_merge",
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
	initial := make([]TypedTableChange, 256)
	for index := range initial {
		initial[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After:    []TypedTableValue{TypedString(fmt.Sprintf("team-%03d", index)), TypedInt64(int64(index))},
		}
	}
	if err := aggregate.Apply(initial); err != nil {
		t.Fatal(err)
	}
	aggregate.Rows()
	compactions := aggregate.compactionCount
	pending := []TypedTableChange{
		{Sequence: 257, After: []TypedTableValue{TypedString("team-000-new"), TypedInt64(1000)}},
		{Sequence: 258, After: []TypedTableValue{TypedString("team-200-new"), TypedInt64(2000)}},
	}
	if err := aggregate.Apply(pending); err != nil {
		t.Fatal(err)
	}
	got := aggregate.Rows()
	if aggregate.compactionCount != compactions {
		t.Fatalf("batched merge compaction count = %d, want unchanged %d", aggregate.compactionCount, compactions)
	}

	expectedTable, err := NewTypedTable(TypedTableSchema{
		Name: "events_merge_expected",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expected, err := NewTypedTableAggregate(expectedTable, TypedTableAggregateDefinition{
		GroupBy:  []string{"team"},
		SumField: "points",
	})
	if err != nil {
		t.Fatal(err)
	}
	all := append(append([]TypedTableChange(nil), initial...), pending...)
	if err := expected.Apply(all); err != nil {
		t.Fatal(err)
	}
	if want := expected.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("batched merge rows = %#v, want %#v", got, want)
	}
}

var mz028BatchedMergeBenchmarkRows []Row

func BenchmarkMZ028BatchedNewGroups(b *testing.B) {
	const initialGroups = 4096
	const batchGroups = 128
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		table, err := NewTypedTable(TypedTableSchema{
			Name: fmt.Sprintf("events_benchmark_%d", iteration),
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
		initial := make([]TypedTableChange, initialGroups)
		for index := range initial {
			initial[index] = TypedTableChange{
				Sequence: uint64(index + 1),
				After:    []TypedTableValue{TypedString(fmt.Sprintf("team-%05d", index)), TypedInt64(int64(index))},
			}
		}
		if err := aggregate.Apply(initial); err != nil {
			b.Fatal(err)
		}
		aggregate.Rows()
		pending := make([]TypedTableChange, batchGroups)
		for index := range pending {
			pending[index] = TypedTableChange{
				Sequence: uint64(initialGroups + index + 1),
				After:    []TypedTableValue{TypedString(fmt.Sprintf("team-%05d-new", index*32)), TypedInt64(int64(index))},
			}
		}
		b.StartTimer()
		if err := aggregate.Apply(pending); err != nil {
			b.Fatal(err)
		}
		mz028BatchedMergeBenchmarkRows = aggregate.Rows()
	}
}
