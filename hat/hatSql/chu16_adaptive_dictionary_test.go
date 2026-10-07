package hatSql

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTypedTableAdaptiveDictionaryDemotionPreservesMutationSemantics(t *testing.T) {
	for _, appendChurn := range []bool{false, true} {
		t.Run(fmt.Sprintf("append=%t", appendChurn), func(t *testing.T) {
			newTable := func(adaptive bool) *TypedTable {
				table, err := NewTypedTable(TypedTableSchema{Name: "events", Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryAdaptive: adaptive}}})
				if err != nil {
					t.Fatal(err)
				}
				return table
			}
			plain, adaptive := newTable(false), newTable(true)
			upsert := func(index int, value TypedTableValue) {
				t.Helper()
				key := fmt.Sprintf("key-%d", index)
				want, err := plain.Upsert(key, []TypedTableValue{value})
				if err != nil {
					t.Fatal(err)
				}
				got, err := adaptive.Upsert(key, []TypedTableValue{value})
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("change mismatch for %s: got %#v want %#v", key, got, want)
				}
				if !reflect.DeepEqual(adaptive.Rows(), plain.Rows()) {
					t.Fatalf("row mismatch after upsert %s", key)
				}
			}
			for index := 0; index < 256; index++ {
				value := TypedString("repeated")
				if index%17 == 0 {
					value = TypedTableValue{Kind: TypedTableString}
				}
				upsert(index, value)
			}
			if !adaptive.columns[0].dictionary {
				t.Fatal("dictionary did not promote")
			}
			start, count := 0, 140
			if appendChurn {
				start, count = 256, 300
			}
			for index := 0; index < count; index++ {
				upsert(start+index, TypedString(fmt.Sprintf("distinct-%d", index)))
			}
			if adaptive.columns[0].dictionary {
				t.Fatal("dictionary did not demote")
			}
			for index := 0; index < 32; index++ {
				upsert(index, TypedTableValue{Kind: TypedTableString})
				upsert(index, TypedString(""))
			}
			for _, index := range []int{0, 17, 128, 255} {
				key := fmt.Sprintf("key-%d", index)
				want, err := plain.Delete(key)
				if err != nil {
					t.Fatal(err)
				}
				got, err := adaptive.Delete(key)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) || !reflect.DeepEqual(adaptive.Rows(), plain.Rows()) {
					t.Fatalf("delete mismatch for %s", key)
				}
				upsert(index, TypedString("repeated"))
			}
			if adaptive.columns[0].dictionary {
				t.Fatal("dictionary promoted again after demotion")
			}
		})
	}
}

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

func TestTypedTableAdaptiveDictionaryDemotesAfterCardinalityGrowth(t *testing.T) {
	const maxDistinct = 128 // Half of the 256-row admission window.
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString, DictionaryAdaptive: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < typedTableDictionaryProbeRows; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString("team-a")}); err != nil {
			t.Fatal(err)
		}
	}
	if !table.columns[0].dictionary {
		t.Fatal("adaptive dictionary did not promote before churn")
	}
	for index := 0; index < maxDistinct+1; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString(fmt.Sprintf("value-%d", index))}); err != nil {
			t.Fatal(err)
		}
	}
	if table.columns[0].dictionary {
		t.Fatal("adaptive dictionary remained enabled after cardinality growth")
	}
	if len(table.columns[0].strings) != typedTableDictionaryProbeRows {
		t.Fatalf("demoted string rows = %d, want %d", len(table.columns[0].strings), typedTableDictionaryProbeRows)
	}
	rows := table.Rows()
	if len(rows) != typedTableDictionaryProbeRows || rows[0]["value"] != "value-0" || rows[maxDistinct+1]["value"] != "team-a" {
		t.Fatalf("demoted rows = %#v", rows)
	}
}

func TestTypedTableExplicitDictionaryTakesPrecedenceOverAdaptiveDemotion(t *testing.T) {
	const maxDistinct = 128
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{{
			Name:               "value",
			Kind:               TypedTableString,
			DictionaryEncoded:  true,
			DictionaryAdaptive: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < typedTableDictionaryProbeRows; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString("team-a")}); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < maxDistinct+1; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedString(fmt.Sprintf("value-%d", index))}); err != nil {
			t.Fatal(err)
		}
	}
	if !table.columns[0].dictionary {
		t.Fatal("explicit dictionary was demoted by adaptive flag")
	}
	if rows := table.Rows(); len(rows) != typedTableDictionaryProbeRows || rows[0]["value"] != "value-0" {
		t.Fatalf("explicit dictionary rows = %#v", rows)
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
