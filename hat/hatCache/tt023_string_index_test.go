package hatCache

import "testing"

func TestSQLJSONFieldIndexStringLookupUsesRawStringKey(t *testing.T) {
	rows := []SQLRow{{"id": int64(1), "name": "needle"}}
	index := &sqlJSONFieldIndex{
		stringOnly: true,
		rows:       map[string][]SQLRow{"needle": rows},
	}

	got, available := sqlJSONFieldIndexLookupRows(index, "needle")
	if !available || len(got) != 1 || got[0]["id"] != int64(1) {
		t.Fatalf("string lookup = %#v, available=%v", got, available)
	}
	got, available = sqlJSONFieldIndexLookupRows(index, int64(1))
	if !available || len(got) != 0 {
		t.Fatalf("non-string lookup = %#v, available=%v", got, available)
	}
}

func TestSQLJSONFieldIndexStringLookupDoesNotAllocate(t *testing.T) {
	index := &sqlJSONFieldIndex{
		stringOnly: true,
		rows:       map[string][]SQLRow{"needle": {{"id": int64(1)}}},
	}
	allocs := testing.AllocsPerRun(100, func() {
		rows, available := sqlJSONFieldIndexLookupRows(index, "needle")
		if !available || len(rows) != 1 {
			t.Fatalf("lookup = %#v, available=%v", rows, available)
		}
	})
	if allocs != 0 {
		t.Fatalf("string lookup allocations = %v, want 0", allocs)
	}
}

func TestSQLJSONFieldIndexRefreshUsesRawKeysForStringOnlyData(t *testing.T) {
	index := &sqlJSONFieldIndex{}
	rows := []SQLRow{{"name": "needle"}, {"name": "other"}}
	if err := refreshSQLJSONFieldIndexSourceRows(index, "name", sqlJSONSource{raw: "strings"}, rows); err != nil {
		t.Fatalf("refresh string index: %v", err)
	}
	if !index.stringOnly {
		t.Fatal("string-only index reported mixed values")
	}
	if _, ok := index.rows["needle"]; !ok {
		t.Fatalf("raw string key missing from %#v", index.rows)
	}
	if _, ok := index.rows["s:needle"]; ok {
		t.Fatalf("legacy prefixed key retained in %#v", index.rows)
	}
}

func TestSQLJSONFieldIndexRefreshKeepsTypedKeysForMixedData(t *testing.T) {
	index := &sqlJSONFieldIndex{}
	rows := []SQLRow{{"value": "1"}, {"value": int64(1)}}
	if err := refreshSQLJSONFieldIndexSourceRows(index, "value", sqlJSONSource{raw: "mixed"}, rows); err != nil {
		t.Fatalf("refresh mixed index: %v", err)
	}
	if index.stringOnly {
		t.Fatal("mixed index reported string-only values")
	}
	if got := len(index.rows["s:1"]); got != 1 {
		t.Fatalf("string posting count = %d, want 1", got)
	}
	if got := len(index.rows["1"]); got != 1 {
		t.Fatalf("integer posting count = %d, want 1", got)
	}
	stringRows, available := sqlJSONFieldIndexLookupRows(index, "1")
	if !available || len(stringRows) != 1 || stringRows[0]["value"] != "1" {
		t.Fatalf("string lookup = %#v, available=%v", stringRows, available)
	}
	integerRows, available := sqlJSONFieldIndexLookupRows(index, int64(1))
	if !available || len(integerRows) != 1 || integerRows[0]["value"] != int64(1) {
		t.Fatalf("integer lookup = %#v, available=%v", integerRows, available)
	}
}
