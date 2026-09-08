package hatSql

import "testing"

func TestTypedTableSortedArrangementDictionaryReleasesValues(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "sorted_dictionary_lifecycle",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, team := range []string{"red", "red", "blue"} {
		if _, err := table.Upsert(string(rune('a'+index)), []TypedTableValue{TypedString(team)}); err != nil {
			t.Fatal(err)
		}
	}
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{Field: "team", DictionaryEncoded: true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(arrangement.dictionary.values), 2; got != want {
		t.Fatalf("dictionary values = %d, want %d", got, want)
	}

	change, err := table.Upsert("a", []TypedTableValue{TypedString("blue")})
	if err != nil {
		t.Fatal(err)
	}
	if err := arrangement.Apply([]TypedTableChange{change}); err != nil {
		t.Fatal(err)
	}
	if got, want := len(arrangement.dictionary.positions), 2; got != want {
		t.Fatalf("dictionary positions after update = %d, want %d", got, want)
	}

	for _, key := range []string{"b", "c", "a"} {
		change, err := table.Delete(key)
		if err != nil {
			t.Fatal(err)
		}
		if err := arrangement.Apply([]TypedTableChange{change}); err != nil {
			t.Fatal(err)
		}
	}
	if len(arrangement.dictionary.positions) != 0 || len(arrangement.dictionary.values) != 0 {
		t.Fatalf("dictionary after deleting all rows = %#v, want empty", arrangement.dictionary)
	}
}
