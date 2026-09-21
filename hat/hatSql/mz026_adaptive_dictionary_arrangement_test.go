package hatSql

import (
	"fmt"
	"testing"
)

func TestMZ026AdaptiveSortedArrangementUsesDictionaryForLowCardinality(t *testing.T) {
	table := newMZ026AdaptiveArrangementTable(t, 8)
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
		Field:              "team",
		DictionaryAdaptive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !arrangement.orderFields[0].dictionaryEncoded || arrangement.dictionary == nil {
		t.Fatal("adaptive arrangement did not select dictionary storage")
	}
	if got, want := len(arrangement.dictionary.positions), 8; got != want {
		t.Fatalf("adaptive dictionary values = %d, want %d", got, want)
	}
}

func TestMZ026AdaptiveSortedArrangementKeepsRawStorageForHighCardinality(t *testing.T) {
	table := newMZ026AdaptiveArrangementTable(t, 256)
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
		Field:              "team",
		DictionaryAdaptive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if arrangement.orderFields[0].dictionaryEncoded || arrangement.dictionary != nil {
		t.Fatal("adaptive arrangement selected dictionary storage for high cardinality")
	}
}

func TestMZ026AdaptiveSortedArrangementIsOffByDefault(t *testing.T) {
	table := newMZ026AdaptiveArrangementTable(t, 8)
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		t.Fatal(err)
	}
	if arrangement.orderFields[0].dictionaryEncoded || arrangement.dictionary != nil {
		t.Fatal("default sorted arrangement unexpectedly selected dictionary storage")
	}
}

func TestMZ026AdaptiveSortedArrangementDemotesAfterCardinalityGrowth(t *testing.T) {
	table := newMZ026AdaptiveArrangementTable(t, 8)
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{
		Field:              "team",
		DictionaryAdaptive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	changes := make([]TypedTableChange, 0, 25)
	for index := 0; index < 25; index++ {
		change, err := table.Upsert(fmt.Sprintf("key-%03d", index), []TypedTableValue{
			TypedString(fmt.Sprintf("new-%03d", index)), TypedInt64(int64(index)),
		})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, change)
	}
	if err := arrangement.Apply(changes); err != nil {
		t.Fatal(err)
	}
	if arrangement.orderFields[0].dictionaryEncoded || arrangement.dictionary != nil {
		t.Fatal("adaptive arrangement retained dictionary storage after cardinality growth")
	}
	for _, row := range arrangement.Rows() {
		if row.Key == "key-000" && row.Values[0].String != "new-000" {
			t.Fatalf("demoted row = %#v", row)
		}
	}
}

func newMZ026AdaptiveArrangementTable(t *testing.T, distinct int) *TypedTable {
	t.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "mz026_adaptive_arrangement",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%03d", index), []TypedTableValue{
			TypedString(fmt.Sprintf("team-%03d", index%distinct)),
			TypedInt64(int64(index)),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return table
}
