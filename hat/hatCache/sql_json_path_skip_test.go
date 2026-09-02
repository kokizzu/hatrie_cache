package hatCache

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONPathSkipIndexPreservesNestedEquality(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"profile":{"city":"Singapore"}},{"id":2,"profile":{"city":"Jakarta"}},{"id":3,"profile":{"city":"Jakarta"}},{"id":4,"profile":{"city":"Jakarta"}}]`)
	if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{
		CacheKey:       "people",
		Paths:          []string{"$.profile.city"},
		RowsPerSegment: 2,
		BitsPerSegment: 256,
	}); err != nil {
		t.Fatalf("CreateSQLJSONPathSkipIndex() error = %v", err)
	}
	candidates, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "$.profile.city", "Singapore")
	if err != nil || !available {
		t.Fatalf("ResolveSQLIndexedSource() = %v, %t, %v", candidates, available, err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates = %d, want one matching segment", len(candidates))
	}
	query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'Singapore' SELECT p.id"
	result, err := hatSql.ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("result.Rows = %#v, want id 1", result.Rows)
	}
	stats, configured, err := trie.SQLJSONIndexMaintenanceStats("people", "$.profile.city")
	if err != nil || !configured || !stats.Current {
		t.Fatalf("SQLJSONIndexMaintenanceStats() = %#v, %t, %v", stats, configured, err)
	}

	trie.UpsertString("people", `[{"id":1,"profile":{"city":"Jakarta"}},{"id":2,"profile":{"city":"Jakarta"}},{"id":3,"profile":{"city":"Jakarta"}},{"id":4,"profile":{"city":"Singapore"}}]`)
	result, err = hatSql.ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() after refresh error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != float64(4) {
		t.Fatalf("refreshed result.Rows = %#v, want id 4", result.Rows)
	}
}

func TestSQLJSONPathSkipIndexIsDefaultOff(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"profile":{"city":"Singapore"}}]`)
	candidates, available, err := trie.ResolveSQLIndexedSource("CACHE", "people", "$.profile.city", "Singapore")
	if err != nil || available || candidates != nil {
		t.Fatalf("unconfigured skip index = %#v, %t, %v", candidates, available, err)
	}
}

func TestSQLJSONPathSkipIndexValidatesBoundedConfiguration(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{CacheKey: "people", Paths: []string{"$.profile.city"}, RowsPerSegment: 0, BitsPerSegment: 0}); err != nil {
		t.Fatalf("defaulted configuration error = %v", err)
	}
	if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{CacheKey: "people", Paths: []string{"not-a-path"}}); err == nil {
		t.Fatal("invalid path unexpectedly accepted")
	}
	paths := make([]string, DefaultSQLJSONPathSkipMaxPaths+1)
	for index := range paths {
		paths[index] = "$.profile.city" + strconv.Itoa(index)
	}
	if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{CacheKey: "people", Paths: paths}); err == nil {
		t.Fatal("oversized path set unexpectedly accepted")
	}
}
