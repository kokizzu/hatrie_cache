package hatSql

import (
	"errors"
	"reflect"
	"testing"
)

func TestTypedTableAggregateApplyMonotoneMaintainsInsertOnlyAggregate(t *testing.T) {
	aggregate := newMonotoneAggregate(t)
	changes := []TypedTableChange{
		{Sequence: 1, Key: "one", After: []TypedTableValue{TypedString("red"), TypedInt64(2)}},
		{Sequence: 2, Key: "two", After: []TypedTableValue{TypedString("red"), TypedInt64(3)}},
		{Sequence: 3, Key: "three", After: []TypedTableValue{TypedString("blue"), TypedInt64(5)}},
	}
	if err := aggregate.ApplyMonotone(changes); err != nil {
		t.Fatalf("ApplyMonotone() error = %v", err)
	}
	if got := aggregate.Checkpoint(); got != 3 {
		t.Fatalf("Checkpoint() = %d, want 3", got)
	}
	want := []Row{
		{"team": "red", "count": int64(2), "sum": float64(5)},
		{"team": "blue", "count": int64(1), "sum": float64(5)},
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
	general := newMonotoneAggregate(t)
	if err := general.Apply(changes); err != nil {
		t.Fatalf("general Apply() error = %v", err)
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, general.Rows()) {
		t.Fatalf("monotone Rows() = %#v, general Rows() = %#v", got, general.Rows())
	}
}

func TestTypedTableAggregateApplyMonotoneKeepsAdvancedAggregateSemantics(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "monotone-advanced",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
			{Name: "label", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{
		GroupBy:       []string{"team"},
		MinField:      "points",
		MaxField:      "points",
		DistinctField: "label",
	})
	if err != nil {
		t.Fatalf("NewTypedTableAggregate() error = %v", err)
	}
	changes := []TypedTableChange{
		{Sequence: 1, After: []TypedTableValue{TypedString("red"), TypedInt64(2), TypedString("a")}},
		{Sequence: 2, After: []TypedTableValue{TypedString("red"), TypedInt64(5), TypedString("b")}},
	}
	if err := aggregate.ApplyMonotone(changes); err != nil {
		t.Fatalf("ApplyMonotone() error = %v", err)
	}
	want := []Row{{
		"team":           "red",
		"count":          int64(2),
		"min":            int64(2),
		"max":            int64(5),
		"count_distinct": int64(2),
	}}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Rows() = %#v, want %#v", got, want)
	}
}

func TestTypedTableAggregateApplyMonotoneIgnoresReplayAndRejectsInvalidChanges(t *testing.T) {
	aggregate := newMonotoneAggregate(t)
	insert := TypedTableChange{Sequence: 1, Key: "one", After: []TypedTableValue{TypedString("red"), TypedInt64(1)}}
	if err := aggregate.ApplyMonotone([]TypedTableChange{insert}); err != nil {
		t.Fatalf("initial ApplyMonotone() error = %v", err)
	}
	if err := aggregate.ApplyMonotone([]TypedTableChange{insert}); err != nil {
		t.Fatalf("replay ApplyMonotone() error = %v", err)
	}
	if got := aggregate.Checkpoint(); got != 1 {
		t.Fatalf("Checkpoint() after replay = %d, want 1", got)
	}

	tests := map[string]struct {
		change TypedTableChange
		want   error
	}{
		"gap": {
			change: TypedTableChange{Sequence: 3, After: []TypedTableValue{TypedString("red"), TypedInt64(1)}},
		},
		"before values": {
			change: TypedTableChange{Sequence: 2, Before: []TypedTableValue{TypedString("red"), TypedInt64(1)}, After: []TypedTableValue{TypedString("blue"), TypedInt64(1)}},
			want:   ErrTypedTableAggregateMonotoneBefore,
		},
		"missing after values": {
			change: TypedTableChange{Sequence: 2},
			want:   ErrTypedTableAggregateMonotoneAfterRequired,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			before := aggregate.Checkpoint()
			err := aggregate.ApplyMonotone([]TypedTableChange{test.change})
			if name != "gap" && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if name == "gap" && err == nil {
				t.Fatal("gap ApplyMonotone() error = nil")
			}
			if got := aggregate.Checkpoint(); got != before {
				t.Fatalf("Checkpoint() = %d after rejected change, want %d", got, before)
			}
		})
	}
}

func TestTypedTableAggregateApplyMonotoneHandlesNilAndEmptyBatches(t *testing.T) {
	var aggregate *TypedTableAggregate
	if err := aggregate.ApplyMonotone(nil); err == nil {
		t.Fatal("nil aggregate ApplyMonotone() error = nil")
	}
	aggregate = newMonotoneAggregate(t)
	if err := aggregate.ApplyMonotone(nil); err != nil {
		t.Fatalf("empty ApplyMonotone() error = %v", err)
	}
	if got := aggregate.Checkpoint(); got != 0 {
		t.Fatalf("Checkpoint() = %d, want 0", got)
	}
}

func newMonotoneAggregate(t *testing.T) *TypedTableAggregate {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "monotone",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"})
	if err != nil {
		t.Fatalf("NewTypedTableAggregate() error = %v", err)
	}
	return aggregate
}

func BenchmarkTypedTableAggregateApplyMonotone(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "monotone-benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]TypedTableChange, 1024)
	for index := range changes {
		changes[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After:    []TypedTableValue{TypedString("team"), TypedInt64(int64(index))},
		}
	}
	definition := TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		aggregate, err := NewTypedTableAggregate(table, definition)
		if err != nil {
			b.Fatal(err)
		}
		if err := aggregate.ApplyMonotone(changes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableAggregateApplyGeneralInsertOnly(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "general-benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "points", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]TypedTableChange, 1024)
	for index := range changes {
		changes[index] = TypedTableChange{
			Sequence: uint64(index + 1),
			After:    []TypedTableValue{TypedString("team"), TypedInt64(int64(index))},
		}
	}
	definition := TypedTableAggregateDefinition{GroupBy: []string{"team"}, SumField: "points"}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		aggregate, err := NewTypedTableAggregate(table, definition)
		if err != nil {
			b.Fatal(err)
		}
		if err := aggregate.Apply(changes); err != nil {
			b.Fatal(err)
		}
	}
}
