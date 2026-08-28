package hatCache

import (
	"context"
	"strings"
	"testing"
)

func TestSQLGroupSkewLimitRejectsDominantKey(t *testing.T) {
	t.Parallel()
	query := "FROM VALUES ('hot'), ('hot'), ('hot'), ('cold') AS events(kind) GROUP BY kind SELECT kind, COUNT(*) AS total"
	_, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxGroupRowsPerKey: 2})
	if err == nil || !strings.Contains(err.Error(), "group skew limit") {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v, want group skew limit", err)
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxGroupRowsPerKey: 3})
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("limit-compatible grouped result = %#v, %v", result, err)
	}
}

func TestSQLGroupSkewLimitRejectsDominantIndexedGroup(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"kind":"cold"},{"kind":"hot"},{"kind":"hot"},{"kind":"hot"}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "kind"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	query := "FROM CACHE('events') AS event GROUP BY event.kind SELECT event.kind, COUNT(*) AS total ORDER BY event.kind"
	_, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{MaxGroupRowsPerKey: 2})
	if err == nil || !strings.Contains(err.Error(), "group skew limit") {
		t.Fatalf("indexed ExecuteSQLQueryParameters() error = %v, want group skew limit", err)
	}
}

func TestSQLGroupSkewLimitRejectsDominantSpilledGroup(t *testing.T) {
	t.Parallel()
	query := "FROM VALUES ('hot'), ('hot'), ('hot'), ('cold') AS events(kind) GROUP BY kind SELECT kind, COUNT(*) AS total ORDER BY kind"
	_, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxGroupRowsPerKey: 2, MaxGroupBytes: 1, SpillDirectory: t.TempDir(), MaxSpillBytes: 1 << 20})
	if err == nil || !strings.Contains(err.Error(), "group skew limit") {
		t.Fatalf("spilled ExecuteSQLQueryParameters() error = %v, want group skew limit", err)
	}
}
