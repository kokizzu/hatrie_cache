package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLSessionTransactionalViewChangesPublishOneVersion(t *testing.T) {
	session := hatSql.NewSQLSession(nil)
	initial := session.ViewCatalogSnapshot()
	if initial.Version != 0 || len(initial.Views) != 0 {
		t.Fatalf("initial catalog = %#v, want version 0 with no views", initial)
	}

	catalog, err := session.ApplyViewChanges([]hatSql.SQLSessionViewChange{
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
		{Name: "base", Query: `FROM VALUES ('Ada') AS rows(name) SELECT name`},
	})
	if err != nil {
		t.Fatalf("ApplyViewChanges() error = %v", err)
	}
	if catalog.Version != 1 || len(catalog.Views) != 2 {
		t.Fatalf("published catalog = %#v, want version 1 with two views", catalog)
	}
	if catalog.Views[0].Name != "base" || catalog.Views[1].Name != "derived" {
		t.Fatalf("published views = %#v, want stable name order", catalog.Views)
	}
	if len(catalog.Views[1].Dependencies) != 1 || catalog.Views[1].Dependencies[0] != "base" {
		t.Fatalf("derived dependencies = %#v, want [base]", catalog.Views[1].Dependencies)
	}
	catalog.Views[1].Dependencies[0] = "mutated"
	if snapshot := session.ViewCatalogSnapshot(); snapshot.Views[1].Dependencies[0] != "base" {
		t.Fatalf("catalog snapshot was not isolated: %#v", snapshot)
	}

	result, err := session.Execute(context.Background(), `FROM CACHE('derived') SELECT name`, nil, hatSql.SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("derived result = %#v, %v", result, err)
	}

	catalog, err = session.ApplyViewChanges([]hatSql.SQLSessionViewChange{
		{Name: "base", Query: `FROM VALUES ('Lin') AS rows(name) SELECT name`},
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
	})
	if err != nil || catalog.Version != 2 {
		t.Fatalf("replacement catalog = %#v, %v", catalog, err)
	}
	result, err = session.Execute(context.Background(), `FROM CACHE('derived') SELECT name`, nil, hatSql.SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("replaced derived result = %#v, %v", result, err)
	}

	beforeFailedBatch := session.ViewCatalogSnapshot()
	if _, err := session.ApplyViewChanges([]hatSql.SQLSessionViewChange{
		{Name: "base", Query: `FROM CACHE('derived') SELECT name`},
		{Name: "derived", Query: `FROM CACHE('base') SELECT name`},
	}); err == nil {
		t.Fatal("ApplyViewChanges() accepted a cyclic batch")
	}
	afterFailedBatch := session.ViewCatalogSnapshot()
	if afterFailedBatch.Version != beforeFailedBatch.Version || len(afterFailedBatch.Views) != len(beforeFailedBatch.Views) {
		t.Fatalf("failed batch changed catalog: before=%#v after=%#v", beforeFailedBatch, afterFailedBatch)
	}
	if _, err := session.ApplyViewChanges([]hatSql.SQLSessionViewChange{
		{Name: "base", Query: `FROM CACHE('derived') SELECT name`},
		{Name: "BASE", Query: `FROM VALUES ('ignored') AS rows(name) SELECT name`},
	}); err == nil {
		t.Fatal("ApplyViewChanges() accepted duplicate names")
	}
	if got := session.CatalogVersion(); got != beforeFailedBatch.Version {
		t.Fatalf("duplicate batch changed catalog version = %d, want %d", got, beforeFailedBatch.Version)
	}
	result, err = session.Execute(context.Background(), `FROM CACHE('derived') SELECT name`, nil, hatSql.SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("failed batch changed published view = %#v, %v", result, err)
	}
}

func TestSQLSessionCreateOrReplaceViewDDLUsesTransactionalBoundary(t *testing.T) {
	session := hatSql.NewSQLSession(nil)
	if _, err := session.Execute(context.Background(), `CREATE VIEW names AS FROM VALUES ('Ada') AS rows(name) SELECT name`, nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatalf("CREATE VIEW error = %v", err)
	}
	if got := session.ViewCatalogSnapshot().Version; got != 1 {
		t.Fatalf("catalog version after CREATE VIEW = %d, want 1", got)
	}
	if _, err := session.Execute(context.Background(), `CREATE OR REPLACE VIEW names AS FROM VALUES ('Lin') AS rows(name) SELECT name`, nil, hatSql.SQLQueryOptions{}); err != nil {
		t.Fatalf("CREATE OR REPLACE VIEW error = %v", err)
	}
	if got := session.ViewCatalogSnapshot().Version; got != 2 {
		t.Fatalf("catalog version after replacement = %d, want 2", got)
	}
	result, err := session.Execute(context.Background(), `FROM CACHE('names') SELECT name`, nil, hatSql.SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("replaced view result = %#v, %v", result, err)
	}
}
