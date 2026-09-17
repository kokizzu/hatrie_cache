package hatSql

import (
	"fmt"
	"testing"
)

func TestTypedTableAdaptiveDictionaryPromotesRepeatedStrings(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString, DictionaryAdaptive: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{{Kind: TypedTableString, Valid: true, String: "team-a"}}); err != nil {
			t.Fatal(err)
		}
	}
	if !table.columns[0].dictionary {
		t.Fatal("adaptive dictionary did not promote repeated values")
	}
	if len(table.columns[0].strings) != 0 || len(table.columns[0].dictionaryValues) != 1 {
		t.Fatalf("adaptive storage = %#v", table.columns[0])
	}
	rows := table.Rows()
	if len(rows) != 256 || rows[0]["team"] != "team-a" || rows[255]["team"] != "team-a" {
		t.Fatalf("adaptive rows = %#v", rows)
	}
}

func TestTypedTableAdaptiveDictionaryRejectsHighCardinalityValues(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryAdaptive: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		value := fmt.Sprintf("value-%d", index)
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{{Kind: TypedTableString, Valid: true, String: value}}); err != nil {
			t.Fatal(err)
		}
	}
	if table.columns[0].dictionary {
		t.Fatal("adaptive dictionary promoted high-cardinality values")
	}
	if len(table.columns[0].strings) != 256 {
		t.Fatalf("plain string rows = %d, want 256", len(table.columns[0].strings))
	}
	if rows := table.Rows(); len(rows) != 256 || rows[255]["value"] != "value-255" {
		t.Fatalf("high-cardinality rows = %#v", rows)
	}
}

func TestTypedTableDictionaryAdaptiveIsOffByDefault(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{{Kind: TypedTableString, Valid: true, String: "team-a"}}); err != nil {
			t.Fatal(err)
		}
	}
	if table.columns[0].dictionary {
		t.Fatal("dictionary encoding changed without an opt-in flag")
	}
}

func TestTypedTableDictionaryEncodedAllNullRowsRemainReadable(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "team", Kind: TypedTableString, DictionaryEncoded: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := table.Upsert("missing", []TypedTableValue{{Kind: TypedTableString}}); err != nil {
		t.Fatal(err)
	}
	rows := table.Rows()
	if len(rows) != 1 || rows[0]["team"] != nil {
		t.Fatalf("NULL dictionary row = %#v", rows)
	}
}
