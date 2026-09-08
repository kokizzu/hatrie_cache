package hatSql

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestTypedTableAggregateRowsPreserveLegacyGroupOrdering(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "group", Kind: TypedTableString},
			{Name: "sequence", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := []struct {
		key      string
		group    string
		sequence int64
	}{
		{key: "a-2", group: "a", sequence: 2},
		{key: "aa-1", group: "aa", sequence: 1},
		{key: "b-2", group: "b", sequence: 2},
		{key: "a-10", group: "a", sequence: 10},
		{key: "b-10", group: "b", sequence: 10},
	}
	for _, row := range input {
		if _, err := table.Upsert(row.key, []TypedTableValue{TypedString(row.group), TypedInt64(row.sequence)}); err != nil {
			t.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, len(input))
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"group", "sequence"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	got := aggregate.Rows()
	want := []Row{
		{"group": "a", "sequence": int64(10), "count": int64(1)},
		{"group": "a", "sequence": int64(2), "count": int64(1)},
		{"group": "b", "sequence": int64(10), "count": int64(1)},
		{"group": "b", "sequence": int64(2), "count": int64(1)},
		{"group": "aa", "sequence": int64(1), "count": int64(1)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("aggregate rows = %#v, want %#v", got, want)
	}
}

func TestTypedTableAggregateRowsRefreshDeferredGroupKeys(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "group", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("first", []TypedTableValue{TypedString("b")}); err != nil {
		t.Fatal(err)
	}
	changes, _, err := table.ChangesAfter(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"group"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	if got := aggregate.Rows(); len(got) != 1 || got[0]["group"] != "b" {
		t.Fatalf("first aggregate rows = %#v", got)
	}
	if _, err := table.Upsert("second", []TypedTableValue{TypedString("a")}); err != nil {
		t.Fatal(err)
	}
	changes, _, err = table.ChangesAfter(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply(changes); err != nil {
		t.Fatal(err)
	}
	want := []Row{
		{"group": "a", "count": int64(1)},
		{"group": "b", "count": int64(1)},
	}
	if got := aggregate.Rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("refreshed aggregate rows = %#v, want %#v", got, want)
	}
}

var typedTableAggregateGroupStorageSink *TypedTableAggregate
var typedTableAggregateGroupStorageRowsSink []Row

func BenchmarkTypedTableAggregateGroupStorage(b *testing.B) {
	table, changes := newTypedTableAggregateGroupStorageInput(b)
	definition := TypedTableAggregateDefinition{GroupBy: []string{"group"}, SumField: "value"}
	b.Run("apply", func(b *testing.B) {
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
			typedTableAggregateGroupStorageSink = aggregate
		}
	})
	b.Run("apply_and_first_rows", func(b *testing.B) {
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
			typedTableAggregateGroupStorageRowsSink = aggregate.Rows()
		}
	})
	b.Run("rows", func(b *testing.B) {
		aggregate, err := NewTypedTableAggregate(table, definition)
		if err != nil {
			b.Fatal(err)
		}
		if err := aggregate.Apply(changes); err != nil {
			b.Fatal(err)
		}
		typedTableAggregateGroupStorageRowsSink = aggregate.Rows()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			typedTableAggregateGroupStorageRowsSink = aggregate.Rows()
		}
	})
}

func newTypedTableAggregateGroupStorageInput(b *testing.B) (*TypedTable, []TypedTableChange) {
	b.Helper()
	const rows = 4096
	table, err := NewTypedTable(TypedTableSchema{
		Name: "aggregate-storage",
		Columns: []TypedTableColumn{
			{Name: "group", Kind: TypedTableString},
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < rows; index++ {
		group := "group-" + strconv.Itoa(index) + strings.Repeat("x", 48)
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []TypedTableValue{TypedString(group), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	changes, _, err := table.ChangesAfter(0, rows)
	if err != nil {
		b.Fatal(err)
	}
	return table, changes
}
