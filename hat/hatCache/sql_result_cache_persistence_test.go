package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestHatTrieSQLResultCachePersistenceDelegates(t *testing.T) {
	trie := &HatTrie{}
	if err := trie.ConfigureSQLResultCache(1); err != nil {
		t.Fatalf("ConfigureSQLResultCache() error = %v", err)
	}
	cache := trie.automaticSQLResultCache()
	if _, err := cache.ExecuteVersioned(context.Background(), "schema-v1/query", func() (string, bool) {
		return "source-v1", true
	}, func(context.Context) (hatSql.QueryResult, error) {
		return hatSql.QueryResult{Columns: []string{"id"}, Rows: []hatSql.Row{{"id": int64(1)}}}, nil
	}); err != nil {
		t.Fatalf("seed ExecuteVersioned() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "result-cache.bin")
	if err := trie.PersistSQLResultCache(path); err != nil {
		t.Fatalf("PersistSQLResultCache() error = %v", err)
	}

	restoredTrie := &HatTrie{}
	if err := restoredTrie.ConfigureSQLResultCache(1); err != nil {
		t.Fatalf("restored ConfigureSQLResultCache() error = %v", err)
	}
	if err := restoredTrie.RestoreSQLResultCache(path); err != nil {
		t.Fatalf("RestoreSQLResultCache() error = %v", err)
	}
	calls := 0
	result, err := restoredTrie.automaticSQLResultCache().ExecuteVersioned(context.Background(), "schema-v1/query", func() (string, bool) {
		return "source-v1", true
	}, func(context.Context) (hatSql.QueryResult, error) {
		calls++
		return hatSql.QueryResult{}, nil
	})
	if err != nil {
		t.Fatalf("restored ExecuteVersioned() error = %v", err)
	}
	if calls != 0 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("restored calls/result = %d/%#v, want 0/id 1", calls, result.Rows)
	}
}

func TestHatTrieSQLResultCachePersistenceRequiresOptInCache(t *testing.T) {
	trie := &HatTrie{}
	path := filepath.Join(t.TempDir(), "result-cache.bin")
	if err := trie.PersistSQLResultCache(path); !errors.Is(err, ErrSQLResultCacheDisabled) {
		t.Fatalf("disabled PersistSQLResultCache() error = %v, want %v", err, ErrSQLResultCacheDisabled)
	}
	if err := trie.RestoreSQLResultCache(path); !errors.Is(err, ErrSQLResultCacheDisabled) {
		t.Fatalf("disabled RestoreSQLResultCache() error = %v, want %v", err, ErrSQLResultCacheDisabled)
	}
}
