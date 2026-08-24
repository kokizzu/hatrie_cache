package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSourceResolverFuncAndObserverFunc(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"name": name, "key": key}}, nil
	})
	rows, err := resolver.ResolveSQLSource("CACHE", "people")
	if err != nil || rows[0]["key"] != "people" {
		t.Fatalf("ResolveSQLSource() = %#v, %v", rows, err)
	}
	called := false
	observer := hatSql.QueryObserverFunc(func(event hatSql.QueryEvent) { called = event.QueryID == "q1" })
	observer.ObserveSQLQuery(hatSql.QueryEvent{QueryID: "q1"})
	if !called {
		t.Fatal("QueryObserverFunc did not call the observer")
	}
}
