package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type externalLookupResolver struct {
	rows        []hatSql.Row
	lookupReady bool
	lookupCalls int
	scanCalls   int
}

func (resolver *externalLookupResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	resolver.scanCalls++
	return resolver.rows, nil
}

func (resolver *externalLookupResolver) ResolveSQLExternalSource(string) ([]hatSql.Row, error) {
	resolver.scanCalls++
	return resolver.rows, nil
}

func (resolver *externalLookupResolver) ResolveSQLLookupSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	resolver.lookupCalls++
	if name != "EXTERNAL" || key != "people" || field != "id" || value != int64(2) {
		return nil, false, nil
	}
	if !resolver.lookupReady {
		return nil, false, nil
	}
	return []hatSql.Row{{"id": int64(2), "name": "Lin"}, {"id": int64(99), "name": "Other"}}, true, nil
}

func TestExternalEqualityUsesLookupArrangementBeforeFullScan(t *testing.T) {
	resolver := &externalLookupResolver{rows: []hatSql.Row{
		{"id": int64(1), "name": "Ada"},
		{"id": int64(2), "name": "Lin"},
	}, lookupReady: true}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people') AS person
WHERE person.id = 2
SELECT person.name`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"name": "Lin"}}) {
		t.Fatalf("rows = %#v, want Lin", result.Rows)
	}
	if resolver.lookupCalls != 1 || resolver.scanCalls != 0 {
		t.Fatalf("lookup calls = %d, scan calls = %d, want one lookup and no scan", resolver.lookupCalls, resolver.scanCalls)
	}
}

func TestExternalEqualityFallsBackWhenLookupArrangementIsUnavailable(t *testing.T) {
	resolver := &externalLookupResolver{rows: []hatSql.Row{
		{"id": int64(1), "name": "Ada"},
		{"id": int64(2), "name": "Lin"},
	}}
	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people') AS person
WHERE person.id = 2
SELECT person.name`, resolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, []hatSql.Row{{"name": "Lin"}}) {
		t.Fatalf("fallback rows = %#v, want Lin", result.Rows)
	}
	if resolver.lookupCalls != 1 || resolver.scanCalls != 1 {
		t.Fatalf("fallback lookup calls = %d, scan calls = %d, want one each", resolver.lookupCalls, resolver.scanCalls)
	}
}
