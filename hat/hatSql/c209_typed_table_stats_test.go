package hatSql

import (
	"math"
	"testing"
)

func TestTypedTableStatsReportsExactActiveColumnMetadata(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString, DictionaryEncoded: true},
			{Name: "score", Kind: TypedTableInt64},
			{Name: "ratio", Kind: TypedTableFloat64},
			{Name: "active", Kind: TypedTableBool},
			{Name: "empty", Kind: TypedTableString},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key    string
		team   string
		score  TypedTableValue
		ratio  float64
		active bool
	}{
		{key: "a", team: "red", score: TypedInt64(5), ratio: math.NaN(), active: false},
		{key: "b", team: "blue", score: TypedNull(), ratio: 1.5, active: true},
		{key: "c", team: "red", score: TypedInt64(9), ratio: -2, active: false},
	} {
		if _, err := table.Upsert(row.key, []TypedTableValue{
			TypedString(row.team), row.score, TypedFloat64(row.ratio), TypedBool(row.active), TypedNull(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	stats := table.Stats()
	if stats.RowCount != 3 {
		t.Fatalf("Stats().RowCount = %d, want 3", stats.RowCount)
	}
	assertTypedTableColumnStats(t, stats, "team", 3, 0, TypedString("blue"), TypedString("red"))
	assertTypedTableColumnStats(t, stats, "score", 3, 1, TypedInt64(5), TypedInt64(9))
	assertTypedTableColumnStats(t, stats, "ratio", 3, 0, TypedFloat64(-2), TypedFloat64(1.5))
	assertTypedTableColumnStats(t, stats, "active", 3, 0, TypedBool(false), TypedBool(true))
	empty := typedTableColumnStats(t, stats, "empty")
	if empty.ValueCount != 0 || empty.NullCount != 3 || empty.HasMinMax {
		t.Fatalf("empty stats = %#v, want zero values, three NULLs, no min/max", empty)
	}

	if _, err := table.Upsert("b", []TypedTableValue{
		TypedString("green"), TypedInt64(7), TypedFloat64(1.5), TypedBool(true), TypedNull(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("c"); err != nil {
		t.Fatal(err)
	}
	stats = table.Stats()
	if stats.RowCount != 2 {
		t.Fatalf("Stats() after update/delete RowCount = %d, want 2", stats.RowCount)
	}
	assertTypedTableColumnStats(t, stats, "team", 2, 0, TypedString("green"), TypedString("red"))
	assertTypedTableColumnStats(t, stats, "score", 2, 0, TypedInt64(5), TypedInt64(7))
	assertTypedTableColumnStats(t, stats, "ratio", 2, 0, TypedFloat64(1.5), TypedFloat64(1.5))
	assertTypedTableColumnStats(t, stats, "active", 2, 0, TypedBool(false), TypedBool(true))
}

func TestTypedTableStatsPreservesSchemaOrderAndNilSafety(t *testing.T) {
	if stats := (*TypedTable)(nil).Stats(); stats.RowCount != 0 || stats.Columns != nil {
		t.Fatalf("nil Stats() = %#v, want zero value", stats)
	}
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "ordered",
		Columns: []TypedTableColumn{{Name: "z", Kind: TypedTableInt64}, {Name: "a", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stats := table.Stats()
	if len(stats.Columns) != 2 || stats.Columns[0].Name != "z" || stats.Columns[1].Name != "a" {
		t.Fatalf("Stats().Columns = %#v, want schema order", stats.Columns)
	}
}

func assertTypedTableColumnStats(t *testing.T, stats TypedTableStats, name string, rows, nulls int, minimum, maximum TypedTableValue) {
	t.Helper()
	column := typedTableColumnStats(t, stats, name)
	if column.ValueCount != rows-nulls || column.NullCount != nulls || !column.HasMinMax || column.Min != minimum || column.Max != maximum {
		t.Fatalf("%s stats = %#v, want rows=%d nulls=%d min=%#v max=%#v", name, column, rows, nulls, minimum, maximum)
	}
}

func typedTableColumnStats(t *testing.T, stats TypedTableStats, name string) TypedTableColumnStats {
	t.Helper()
	for _, column := range stats.Columns {
		if column.Name == name {
			return column
		}
	}
	t.Fatalf("missing column stats for %q", name)
	return TypedTableColumnStats{}
}
