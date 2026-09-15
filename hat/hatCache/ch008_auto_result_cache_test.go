package hatCache

import (
	"context"
	"reflect"
	"testing"
)

func TestHatTrieAutomaticSQLResultCacheIsOptInAndInvalidates(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("people", `[{"id":1,"name":"Ada"}]`); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id, person.name"

	first, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := trie.SQLResultCacheStats(); got.Entries != 0 || got.Hits != 0 || got.Misses != 0 {
		t.Fatalf("default SQL result-cache stats = %#v, want inactive", got)
	}
	if !reflect.DeepEqual(first.Rows, second.Rows) {
		t.Fatalf("default query results differ: first=%#v second=%#v", first.Rows, second.Rows)
	}

	if err := trie.ConfigureSQLResultCache(2); err != nil {
		t.Fatal(err)
	}
	cached, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cached.Rows[0]["name"] = "caller mutation"
	hit, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := hit.Rows[0]["name"]; got != "Ada" {
		t.Fatalf("automatic cache hit name = %#v, want Ada", got)
	}
	if got := trie.SQLResultCacheStats(); got.Entries != 1 || got.Misses != 1 || got.Hits != 1 {
		t.Fatalf("automatic SQL result-cache stats after hit = %#v, want one miss and one hit", got)
	}

	if err := trie.UpsertStringChecked("people", `[{"id":2,"name":"Lin"}]`); err != nil {
		t.Fatal(err)
	}
	updated, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := updated.Rows[0]["id"]; got != float64(2) {
		t.Fatalf("invalidated automatic cache id = %#v, want 2", got)
	}
	if got := trie.SQLResultCacheStats(); got.Misses != 2 || got.Hits != 1 {
		t.Fatalf("automatic SQL result-cache stats after write = %#v, want second miss", got)
	}

	if err := trie.ConfigureSQLResultCache(0); err != nil {
		t.Fatal(err)
	}
	if got := trie.SQLResultCacheStats(); got != (SQLResultCacheStats{}) {
		t.Fatalf("disabled SQL result-cache stats = %#v, want zero", got)
	}
}

func TestHatTrieAutomaticSQLResultCacheDoesNotOverrideExplicitCache(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("people", `[{"id":1}]`); err != nil {
		t.Fatal(err)
	}
	if err := trie.ConfigureSQLResultCache(2); err != nil {
		t.Fatal(err)
	}
	explicit := NewSQLResultCache(1)
	options := SQLQueryOptions{ResultCache: explicit.cache}
	query := "FROM CACHE('people') AS person SELECT person.id"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, options); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, trie, nil, options); err != nil {
		t.Fatal(err)
	}
	if got := trie.SQLResultCacheStats(); got.Entries != 0 || got.Hits != 0 {
		t.Fatalf("automatic cache was used beside explicit cache: %#v", got)
	}
	if got := explicit.cache.Stats(); got.Hits != 1 {
		t.Fatalf("explicit cache stats = %#v, want one hit", got)
	}
}

func TestHatTrieAutomaticSQLResultCacheWiresNoParameterEntrypoints(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("people", `[{"id":1}]`); err != nil {
		t.Fatal(err)
	}
	if err := trie.ConfigureSQLResultCache(2); err != nil {
		t.Fatal(err)
	}
	query := "FROM CACHE('people') AS person SELECT person.id"
	if _, err := ExecuteSQLQuery(query, trie); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecuteSQLQueryContext(context.Background(), query, trie, SQLQueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := trie.SQLResultCacheStats(); got.Misses != 1 || got.Hits != 1 {
		t.Fatalf("no-parameter entrypoint stats = %#v, want one miss and one hit", got)
	}
}
