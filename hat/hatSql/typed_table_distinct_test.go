package hatSql

import (
	"math"
	"testing"
)

func TestTypedTableAggregateMaintainsExactCountDistinct(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{Name: "events", Columns: []TypedTableColumn{{Name: "group", Kind: TypedTableString}, {Name: "value", Kind: TypedTableString}}})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{GroupBy: []string{"group"}, DistinctField: "value"})
	if err != nil {
		t.Fatal(err)
	}
	apply := func(key string, values []TypedTableValue) {
		t.Helper()
		change, err := table.Upsert(key, values)
		if err != nil {
			t.Fatal(err)
		}
		if err := aggregate.Apply([]TypedTableChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	assertRows := func(count, distinct int64) {
		t.Helper()
		rows := aggregate.Rows()
		if len(rows) != 1 || rows[0]["group"] != "west" || rows[0]["count"] != count || rows[0]["count_distinct"] != distinct {
			t.Fatalf("rows = %#v, want west count=%d count_distinct=%d", rows, count, distinct)
		}
	}

	apply("a", []TypedTableValue{TypedString("west"), TypedString("amber")})
	apply("b", []TypedTableValue{TypedString("west"), TypedString("amber")})
	apply("c", []TypedTableValue{TypedString("west"), TypedString("blue")})
	apply("d", []TypedTableValue{TypedString("west"), TypedNull()})
	assertRows(4, 2)

	apply("b", []TypedTableValue{TypedString("west"), TypedString("blue")})
	assertRows(4, 2)
	change, err := table.Delete("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertRows(3, 1)
	apply("c", []TypedTableValue{TypedString("west"), TypedNull()})
	assertRows(3, 1)
	change, err = table.Delete("b")
	if err != nil {
		t.Fatal(err)
	}
	if err := aggregate.Apply([]TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertRows(2, 0)

	arrangements, err := NewTypedTableAggregateArrangements(table)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"group"}})
	if err != nil {
		t.Fatal(err)
	}
	distinct, err := arrangements.Acquire(TypedTableAggregateDefinition{GroupBy: []string{"group"}, DistinctField: "value"})
	if err != nil {
		t.Fatal(err)
	}
	if arrangements.Active() != 2 || plain.entry == distinct.entry {
		t.Fatalf("arrangement identity = active %d, shared %t", arrangements.Active(), plain.entry == distinct.entry)
	}
	plain.Release()
	distinct.Release()
	if _, err := NewTypedTableAggregate(table, TypedTableAggregateDefinition{DistinctField: "missing"}); err == nil {
		t.Fatal("missing distinct field was accepted")
	}

	floatTable, err := NewTypedTable(TypedTableSchema{Name: "floats", Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableFloat64}}})
	if err != nil {
		t.Fatal(err)
	}
	floatAggregate, err := NewTypedTableAggregate(floatTable, TypedTableAggregateDefinition{DistinctField: "value"})
	if err != nil {
		t.Fatal(err)
	}
	nan := TypedFloat64(math.Float64frombits(0x7ff8000000000001))
	for _, key := range []string{"first", "second"} {
		change, err := floatTable.Upsert(key, []TypedTableValue{nan})
		if err != nil {
			t.Fatal(err)
		}
		if err := floatAggregate.Apply([]TypedTableChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	rows := floatAggregate.Rows()
	if len(rows) != 1 || rows[0]["count"] != int64(2) || rows[0]["count_distinct"] != int64(1) {
		t.Fatalf("float rows = %#v, want one distinct NaN", rows)
	}

	distinctKeys := make(map[typedTableDistinctValue]struct{})
	for _, value := range []TypedTableValue{TypedString("value"), TypedInt64(1), TypedFloat64(1), TypedBool(true)} {
		key, valid := typedTableAggregateDistinctValue(value)
		if !valid {
			t.Fatalf("distinct key rejected %#v", value)
		}
		distinctKeys[key] = struct{}{}
	}
	if len(distinctKeys) != 4 {
		t.Fatalf("typed distinct keys = %#v", distinctKeys)
	}
	if _, valid := typedTableAggregateDistinctValue(TypedNull()); valid {
		t.Fatal("NULL was accepted as a distinct key")
	}
}
