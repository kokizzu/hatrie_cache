package hatCache

import (
	"context"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLSystemTablesResolverExposesPartsMutationsAndQueryHistory(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: "alpha", Value: "one"})
	if !response.OK {
		t.Fatalf("seed command failed: %#v", response)
	}

	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	response = journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "beta", Value: "two"})
	if !response.OK {
		t.Fatalf("journal command failed: %#v", response)
	}

	manager := NewSQLQueryManager(8)
	_, err = manager.Execute(context.Background(), "FROM CACHE('items') SELECT value", hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"value": int64(1)}}, nil
	}), nil, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}

	resolver := NewSQLSystemTablesResolver(trie, SQLSystemTablesResolverOptions{
		QueryManager: manager,
		Journal:      journal,
	})

	parts, err := resolver.ResolveSQLSource("CACHE", "system.parts")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0]["name"] != "root" || parts[0]["rows"] != int64(2) || parts[0]["active"] != true {
		t.Fatalf("parts = %#v", parts)
	}

	mutations, err := resolver.ResolveSQLSource("CACHE", "system.mutations")
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) != 1 || mutations[0]["command"] != "SET" || mutations[0]["key"] != "beta" || mutations[0]["state"] != "committed" {
		t.Fatalf("mutations = %#v", mutations)
	}
	if _, exposed := mutations[0]["value"]; exposed {
		t.Fatalf("mutation value must not be exposed: %#v", mutations[0])
	}

	history, err := resolver.ResolveSQLSource("CACHE", "system.query_history")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0]["state"] != string(SQLQueryStateSucceeded) {
		t.Fatalf("query history = %#v", history)
	}
	if _, exposed := history[0]["query"]; exposed {
		t.Fatalf("query text must not be exposed: %#v", history[0])
	}
	queryResult, err := ExecuteSQLQuery("FROM CACHE('system.query_history') SELECT query_id, state", resolver)
	if err != nil {
		t.Fatal(err)
	}
	if len(queryResult.Rows) != 1 || queryResult.Rows[0]["state"] != string(SQLQueryStateSucceeded) {
		t.Fatalf("query history SQL result = %#v", queryResult.Rows)
	}

	active, err := resolver.ResolveSQLSource("CACHE", "system.queries")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active queries = %#v", active)
	}

	keys, err := resolver.ResolveSQLSource("KEYS", "")
	if err != nil || len(keys) != 2 {
		t.Fatalf("delegated keys = %#v, %v", keys, err)
	}
}

func TestSQLSystemTablesResolverUsesPartitionRows(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c"} {
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "SET", Key: key, Value: "value"})
		if !response.OK {
			t.Fatalf("seed %q failed: %#v", key, response)
		}
	}

	rows, err := NewSQLSystemTablesResolver(trie, SQLSystemTablesResolverOptions{}).ResolveSQLSource("CACHE", "system.parts")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["partition"] != int64(0) || rows[1]["partition"] != int64(1) {
		t.Fatalf("partition rows = %#v", rows)
	}
	if rows[0]["rows"].(int64)+rows[1]["rows"].(int64) != int64(3) {
		t.Fatalf("partition row counts = %#v", rows)
	}
}
