package hatSql_test

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableSortedArrangementOrdersAndMaintainsRows(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_rows")
	for _, row := range []struct {
		key, team string
		score     int64
	}{
		{key: "c", team: "blue", score: 2},
		{key: "a", team: "red", score: 3},
		{key: "b", team: "red", score: 1},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Upsert("null", []hatSql.TypedTableValue{hatSql.TypedNull(), hatSql.TypedInt64(0)}); err != nil {
		t.Fatal(err)
	}

	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "c", "a", "b", "null")
	rows := arrangement.Rows()
	rows[0].Values[0] = hatSql.TypedString("mutated")
	if fresh := arrangement.Rows(); fresh[0].Values[0].String != "blue" {
		t.Fatalf("sorted arrangement exposed row storage: %#v", fresh)
	}

	change, err := table.Upsert("c", []hatSql.TypedTableValue{hatSql.TypedString("green"), hatSql.TypedInt64(4)})
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "c", "a", "b", "null")

	change, err = table.Delete("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "c", "b", "null")

	change, err = table.Upsert("d", []hatSql.TypedTableValue{hatSql.TypedString("alpha"), hatSql.TypedInt64(5)})
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]hatSql.TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "d", "c", "b", "null")
}

func TestTypedTableSortedArrangementSupportsDescendingAndNullOrdering(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_descending")
	for _, row := range []struct {
		key   string
		score int64
	}{
		{key: "one", score: 1},
		{key: "two", score: 2},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.key), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := table.Upsert("null", []hatSql.TypedTableValue{hatSql.TypedString("null"), hatSql.TypedNull()}); err != nil {
		t.Fatal(err)
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "score", Descending: true, NullsFirst: true})
	if err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "null", "two", "one")
}

func TestTypedTableSortedArrangementRowsPageReturnsBoundedIndependentSnapshot(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_page")
	for _, row := range []struct {
		key, team string
		score     int64
	}{
		{key: "c", team: "blue", score: 2},
		{key: "a", team: "red", score: 3},
		{key: "b", team: "red", score: 1},
		{key: "d", team: "yellow", score: 4},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{hatSql.TypedString(row.team), hatSql.TypedInt64(row.score)}); err != nil {
			t.Fatal(err)
		}
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.RowsPage(1, 2), "a", "b")
	assertSortedArrangementKeys(t, arrangement.RowsPage(3, 10), "d")
	if got := arrangement.RowsPage(4, 10); len(got) != 0 {
		t.Fatalf("page beyond end = %#v, want empty", got)
	}
	if got := arrangement.RowsPage(0, 0); len(got) != 0 {
		t.Fatalf("zero-limit page = %#v, want empty", got)
	}
	assertSortedArrangementKeys(t, arrangement.RowsPage(-1, 2), "c", "a")

	page := arrangement.RowsPage(0, 1)
	page[0].Values[0] = hatSql.TypedString("mutated")
	if fresh := arrangement.RowsPage(0, 1); fresh[0].Values[0].String != "blue" {
		t.Fatalf("sorted arrangement page exposed row storage: %#v", fresh)
	}
}

func TestTypedTableSortedArrangementRejectsGapsAndInvalidDefinitions(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_validation")
	if _, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "missing"}); !errors.Is(err, hatSql.ErrTypedTableSortedArrangementField) {
		t.Fatalf("missing field error = %v, want field error", err)
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	first := hatSql.TypedTableChange{Sequence: 1, Operation: "INSERT", Key: "a", After: []hatSql.TypedTableValue{hatSql.TypedString("red"), hatSql.TypedInt64(1)}}
	gap := first
	gap.Sequence = 3
	if err := arrangement.Apply([]hatSql.TypedTableChange{first, gap}); !errors.Is(err, hatSql.ErrTypedTableSortedArrangementSequenceGap) {
		t.Fatalf("gap error = %v, want sequence gap", err)
	}
	if arrangement.Checkpoint() != 1 {
		t.Fatalf("checkpoint = %d, want 1", arrangement.Checkpoint())
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "a")
	if err := arrangement.Apply([]hatSql.TypedTableChange{first}); err != nil {
		t.Fatalf("replayed change error = %v", err)
	}
	if got := arrangement.Rows(); !reflect.DeepEqual(got[0].Values, first.After) {
		t.Fatalf("replayed row = %#v, want %#v", got[0].Values, first.After)
	}
}

func TestTypedTableSortedArrangementTreatsNaNAsNullLike(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "sorted_nan",
		Columns: []hatSql.TypedTableColumn{{Name: "value", Kind: hatSql.TypedTableFloat64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		value hatSql.TypedTableValue
	}{
		{key: "one", value: hatSql.TypedFloat64(1)},
		{key: "nan", value: hatSql.TypedFloat64(math.NaN())},
	} {
		if _, err := table.Upsert(row.key, []hatSql.TypedTableValue{row.value}); err != nil {
			t.Fatal(err)
		}
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "value"})
	if err != nil {
		t.Fatal(err)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), "one", "nan")
}

func TestTypedTableSortedArrangementBulkApplyRebuildsDeterministicOrder(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_bulk")
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 128)
	for index := range changes {
		value := len(changes) - index - 1
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(index + 1),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%03d", value),
			After:     []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%03d", value)), hatSql.TypedInt64(int64(value))},
		}
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(changes))
	for index := range want {
		want[index] = fmt.Sprintf("key-%03d", index)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), want...)
}

func TestTypedTableSortedArrangementBulkAppendPreservesOrder(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_bulk_append")
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 128)
	for index := range changes {
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(index + 1),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%03d", index),
			After:     []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%03d", index)), hatSql.TypedInt64(int64(index))},
		}
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	want := make([]string, len(changes))
	for index := range want {
		want[index] = fmt.Sprintf("key-%03d", index)
	}
	assertSortedArrangementKeys(t, arrangement.Rows(), want...)
}

func TestTypedTableSortedArrangementBulkAppendFallsBackForDuplicateKey(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_bulk_append_duplicate")
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 64)
	for index := range changes {
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(index + 1),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%03d", index),
			After:     []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%03d", index)), hatSql.TypedInt64(int64(index))},
		}
	}
	changes[len(changes)-1].Key = changes[len(changes)-2].Key
	changes[len(changes)-1].After = []hatSql.TypedTableValue{hatSql.TypedString("team-final"), hatSql.TypedInt64(999)}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	rows := arrangement.Rows()
	if len(rows) != len(changes)-1 {
		t.Fatalf("row count = %d, want %d", len(rows), len(changes)-1)
	}
	for _, row := range rows {
		if row.Key == changes[len(changes)-1].Key {
			if row.Values[0].String != "team-final" {
				t.Fatalf("duplicate key value = %#v, want final value", row.Values)
			}
			return
		}
	}
	t.Fatalf("duplicate key %q missing from rows", changes[len(changes)-1].Key)
}

func TestTypedTableSortedArrangementBulkApplyHandlesDeleteAndReinsert(t *testing.T) {
	table := newSortedArrangementTable(t, "sorted_bulk_reinsert")
	if _, err := table.Upsert("same", []hatSql.TypedTableValue{hatSql.TypedString("old"), hatSql.TypedInt64(1)}); err != nil {
		t.Fatal(err)
	}
	arrangement, err := hatSql.NewTypedTableSortedArrangement(table, hatSql.TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]hatSql.TypedTableChange, 64)
	for index := range changes {
		changes[index] = hatSql.TypedTableChange{
			Sequence:  uint64(index + 2),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%03d", index),
			After:     []hatSql.TypedTableValue{hatSql.TypedString(fmt.Sprintf("team-%03d", index)), hatSql.TypedInt64(int64(index))},
		}
	}
	changes[0] = hatSql.TypedTableChange{Sequence: 2, Operation: "DELETE", Key: "same"}
	changes[1] = hatSql.TypedTableChange{Sequence: 3, Operation: "INSERT", Key: "same", After: []hatSql.TypedTableValue{hatSql.TypedString("team-999"), hatSql.TypedInt64(999)}}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	rows := arrangement.Rows()
	if len(rows) != len(changes)-1 {
		t.Fatalf("row count = %d, want %d", len(rows), len(changes)-1)
	}
	count := 0
	for _, row := range rows {
		if row.Key == "same" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("reinserted key count = %d, want 1", count)
	}
}

func newSortedArrangementTable(t testing.TB, name string) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: name,
		Columns: []hatSql.TypedTableColumn{
			{Name: "team", Kind: hatSql.TypedTableString},
			{Name: "score", Kind: hatSql.TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func assertSortedArrangementKeys(t *testing.T, rows []hatSql.TypedTableMergeJoinInput, want ...string) {
	t.Helper()
	got := make([]string, len(rows))
	for index, row := range rows {
		got[index] = row.Key
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sorted arrangement keys = %v, want %v", got, want)
	}
}
