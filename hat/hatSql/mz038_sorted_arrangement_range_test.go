package hatSql_test

import (
	"errors"
	"reflect"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestMZ038SortedArrangementRangeSeeksAndBounds(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "mz038_range",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		team  string
		score int64
	}{
		{key: "a", team: "red", score: 1},
		{key: "b", team: "red", score: 2},
		{key: "c", team: "blue", score: 2},
		{key: "d", team: "green", score: 3},
		{key: "e", team: "yellow", score: 4},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.score),
		}); err != nil {
			t.Fatal(err)
		}
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "score"})
	if err != nil {
		t.Fatal(err)
	}

	got, err := arrangement.RowsRange(
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(2)}, Inclusive: true},
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(4)}, Inclusive: false},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if keys := mz038RangeKeys(got); !reflect.DeepEqual(keys, []string{"b", "c", "d"}) {
		t.Fatalf("inclusive lower/exclusive upper keys = %v, want [b c d]", keys)
	}

	got, err = arrangement.RowsRange(
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(2)}, Inclusive: false},
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(4)}, Inclusive: true},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if keys := mz038RangeKeys(got); !reflect.DeepEqual(keys, []string{"d"}) {
		t.Fatalf("exclusive lower/inclusive upper limited keys = %v, want [d]", keys)
	}

	got, err = arrangement.RowsRange(nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if keys := mz038RangeKeys(got); !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("unbounded limited keys = %v, want [a b]", keys)
	}

	got[0].Values[1] = hatSql.TypedInt64(99)
	fresh, err := arrangement.RowsRange(nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if fresh[0].Values[1].Int64 != 1 {
		t.Fatalf("range result exposed arrangement storage: %#v", fresh[0].Values)
	}

	change, err := table.Upsert("f", []hatSql.TypedTableValue{hatSql.TypedString("purple"), hatSql.TypedInt64(2)})
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	got, err = arrangement.RowsRange(
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(2)}, Inclusive: true},
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(3)}, Inclusive: true},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if keys := mz038RangeKeys(got); !reflect.DeepEqual(keys, []string{"b", "c", "f", "d"}) {
		t.Fatalf("range after insert keys = %v, want [b c f d]", keys)
	}
}

func TestMZ038SortedArrangementRangeSupportsCompositeAndValidatesBounds(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "mz038_composite_range",
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		team  string
		score int64
	}{
		{key: "blue-1", team: "blue", score: 1},
		{key: "blue-2", team: "blue", score: 2},
		{key: "red-1", team: "red", score: 1},
		{key: "red-2", team: "red", score: 2},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{
			hatSql.TypedString(row.team),
			hatSql.TypedInt64(row.score),
		}); err != nil {
			t.Fatal(err)
		}
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{
		OrderBy: []hatSql.TypedTableSortedArrangementOrder{
			{Field: "team"},
			{Field: "score", Descending: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := arrangement.RowsRange(
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedString("blue"), hatSql.TypedInt64(2)}, Inclusive: true},
		&hatSql.TypedTableSortedArrangementBound{Values: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(2)}, Inclusive: true},
		10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if keys := mz038RangeKeys(got); !reflect.DeepEqual(keys, []string{"blue-2", "blue-1", "red-2"}) {
		t.Fatalf("composite descending range keys = %v, want [blue-2 blue-1 red-2]", keys)
	}
	if got, err := arrangement.RowsRange(nil, nil, 0); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("zero limit returned %v rows, want empty", len(got))
	}

	for name, bounds := range map[string][2]*hatSql.TypedTableSortedArrangementBound{
		"missing component": {
			{Values: []hatSql.TypedTableValue{hatSql.TypedString("blue")}},
			nil,
		},
		"wrong component type": {
			{Values: []hatSql.TypedTableValue{hatSql.TypedInt64(1), hatSql.TypedInt64(2)}},
			nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := arrangement.RowsRange(bounds[0], bounds[1], 10); !errors.Is(err, hatSql.ErrTypedTableSortedArrangementBound) {
				t.Fatalf("range error = %v, want bound error", err)
			}
		})
	}
}

func mz038RangeKeys(rows []hatSql.TypedTableMergeJoinInput) []string {
	keys := make([]string, len(rows))
	for index, row := range rows {
		keys[index] = row.Key
	}
	return keys
}
