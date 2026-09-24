package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCH042ColumnarDictionaryGroupFastPathSelection(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	batch := ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"region": {Values: []string{"west", "east"}, Codes: []uint32{0, 1, 0, 1}, codesTrusted: true},
		},
		Rows: 4,
	}
	if _, ok := sqlColumnarDictionaryGroupField(batch, "region", query.groupBy[0].collation); !ok {
		t.Fatal("trusted binary dictionary group field was not admitted to the code fast path")
	}
	if _, ok := sqlColumnarDictionaryGroupField(batch, "region", SQLCollationUnicodeCI); ok {
		t.Fatal("unicode case-insensitive grouping was admitted to the binary code fast path")
	}
	untrusted := batch
	untrustedDictionary := untrusted.Dictionaries["region"]
	untrustedDictionary.codesTrusted = false
	untrusted.Dictionaries["region"] = untrustedDictionary
	if _, ok := sqlColumnarDictionaryGroupField(untrusted, "region", query.groupBy[0].collation); ok {
		t.Fatal("untrusted dictionary was admitted to the code fast path")
	}
}

func TestCH042ColumnarDictionaryGroupMatchesRowSemantics(t *testing.T) {
	batch, rows := newCH042ColumnarDictionaryGroupInput(128)
	resolver := &sqlVectorColumnarResolver{batch: batch, rows: rows}
	query := "SELECT region, COUNT(*) AS n, SUM(score) AS total, AVG(score) AS average FROM CACHE('items') GROUP BY region"
	got, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want, err := ExecuteSQLQueryParameters(context.Background(), query, sqlVectorRowOnlyResolver{rows: rows}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Rows, want.Rows) {
		t.Fatalf("dictionary rows = %#v, row executor rows = %#v", got.Rows, want.Rows)
	}
}

func TestCH042ColumnarDictionaryGroupSupportsPackedCodes(t *testing.T) {
	batch, rows := newCH042ColumnarDictionaryGroupInput(128)
	batch.PackDictionaryValues()
	batch.PackDictionaryCodes()
	query, err := parseSQLQueryWithCache("SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := sqlColumnarDictionaryGroupField(batch, "region", query.groupBy[0].collation); !ok {
		t.Fatal("packed dictionary was not admitted to the code fast path")
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", &sqlVectorColumnarResolver{batch: batch, rows: rows}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 64 {
		t.Fatalf("packed dictionary group rows = %d, want 64", len(result.Rows))
	}
}

func TestCH042ColumnarDictionaryGroupTwoLevelUsesCodes(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT region, COUNT(*) AS n, MIN(score) AS minimum, MAX(score) AS maximum FROM CACHE('items') GROUP BY region", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	projections, ok := sqlOrderedGroupProjections(query)
	if !ok {
		t.Fatal("two-level test query was not admitted to grouped projections")
	}
	batch, _ := newCH042ColumnarDictionaryGroupInput(20_000)
	dictionary := batch.Dictionaries["region"]
	states, matched, err := executeSQLColumnarDictionaryTwoLevelGroupStates(query, batch, projections, "region", dictionary, func(int) (bool, error) {
		return true, nil
	}, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if matched != batch.Rows || len(states) != 64 {
		t.Fatalf("two-level code grouping matched=%d states=%d, want matched=%d states=64", matched, len(states), batch.Rows)
	}
}
