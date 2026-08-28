package hatSql

import (
	"context"
	"testing"
)

func TestSQLSessionTemporaryTablesAndNamedResultsAreLocal(t *testing.T) {
	session := NewSQLSession(nil)
	if err := session.CreateTemporaryTable("people", []Row{{"id": int64(1), "name": "Ada"}}); err != nil {
		t.Fatalf("CreateTemporaryTable() error = %v", err)
	}
	if err := session.StoreNamedResult("adults", SQLQueryResult{Rows: []Row{{"id": int64(2)}}}); err != nil {
		t.Fatalf("StoreNamedResult() error = %v", err)
	}
	result, err := session.Execute(context.Background(), `FROM CACHE('people') SELECT name`, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("temporary query = %#v, %v", result, err)
	}
	result, err = session.Execute(context.Background(), `FROM CACHE('adults') SELECT id`, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(2) {
		t.Fatalf("named result query = %#v, %v", result, err)
	}
	other := NewSQLSession(nil)
	result, err = other.Execute(context.Background(), `FROM CACHE('people') SELECT name`, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("temporary table leaked into another session: %#v, %v", result, err)
	}
}

func TestSQLSessionViewsResolveAndRejectCycles(t *testing.T) {
	session := NewSQLSession(nil)
	if err := session.CreateTemporaryTable("people", []Row{{"name": "Ada"}}); err != nil {
		t.Fatal(err)
	}
	if err := session.CreateView("people_view", `FROM CACHE('people') SELECT name`); err != nil {
		t.Fatalf("CreateView() error = %v", err)
	}
	result, err := session.Execute(context.Background(), `FROM CACHE('people_view') SELECT name`, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("view query = %#v, %v", result, err)
	}
	if err := session.CreateView("loop", `FROM CACHE('loop') SELECT name`); err == nil {
		t.Fatal("CreateView() accepted a dependency cycle")
	}
	if err := session.CreateView("first", `FROM CACHE('people') SELECT name`); err != nil {
		t.Fatal(err)
	}
	if err := session.CreateView("second", `FROM CACHE('first') SELECT name`); err != nil {
		t.Fatal(err)
	}
	if err := session.CreateView("first", `FROM CACHE('second') SELECT name`); err == nil {
		t.Fatal("CreateView() accepted an indirect dependency cycle")
	}
	result, err = session.Execute(context.Background(), `FROM CACHE('first') SELECT name`, nil, SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("cycle rejection did not preserve prior view: %#v, %v", result, err)
	}
}
