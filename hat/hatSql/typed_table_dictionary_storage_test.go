package hatSql

import "testing"

func TestTypedTableDictionaryEncodedStringsRemainCorrect(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{Name: "events", Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString, DictionaryEncoded: true}, {Name: "points", Kind: TypedTableInt64}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		key   string
		team  string
		points int64
	}{{"a", "core", 1}, {"b", "edge", 2}, {"c", "core", 3}} {
		if _, err := table.Upsert(row.key, []TypedTableValue{TypedString(row.team), TypedInt64(row.points)}); err != nil {
			t.Fatal(err)
		}
	}
	storage := table.columns[0]
	if len(storage.strings) != 0 || len(storage.dictionaryCodes) != 3 || len(storage.dictionaryValues) != 2 {
		t.Fatalf("dictionary storage = %#v", storage)
	}
	if _, err := table.Upsert("b", []TypedTableValue{TypedString("core"), TypedInt64(4)}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Delete("a"); err != nil {
		t.Fatal(err)
	}
	rows := table.Rows()
	if len(rows) != 2 || rows[0]["team"] != "core" || rows[0]["points"] != int64(3) || rows[1]["team"] != "core" || rows[1]["points"] != int64(4) {
		t.Fatalf("rows = %#v", rows)
	}
	if len(table.columns[0].dictionaryValues) != 1 || table.columns[0].dictionaryValues[0] != "core" {
		t.Fatalf("dictionary values = %#v", table.columns[0].dictionaryValues)
	}
}
