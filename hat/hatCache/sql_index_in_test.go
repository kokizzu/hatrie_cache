package hatCache

import "testing"

type sqlIndexINCountingResolver struct {
	*HatTrie
	calls int
}

func (resolver *sqlIndexINCountingResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]SQLRow, bool, error) {
	resolver.calls++
	return resolver.HatTrie.ResolveSQLIndexedSource(name, key, field, value)
}

func TestSQLJSONIndexUsesIndexForLiteralIN(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1},{"id":2},{"id":3}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	resolver := &sqlIndexINCountingResolver{HatTrie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.id IN (1, 3, 3) SELECT person.id ORDER BY person.id", resolver)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != float64(1) || result.Rows[1]["id"] != float64(3) {
		t.Fatalf("literal IN query = %#v, error %v", result, err)
	}
	if resolver.calls != 2 {
		t.Fatalf("indexed literal IN probes = %d, want 2 distinct probes", resolver.calls)
	}
}

func TestSQLJSONIndexLiteralINPreservesNullAndNotINBehavior(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1},{"id":2},{"name":"missing"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "id"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	resolver := &sqlIndexINCountingResolver{HatTrie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.id IN (1, NULL) SELECT person.id", resolver)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) || resolver.calls != 1 {
		t.Fatalf("literal IN with NULL = %#v, probes %d, error %v", result, resolver.calls, err)
	}

	resolver.calls = 0
	result, err = ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.id NOT IN (1, NULL) SELECT person.id", resolver)
	if err != nil || len(result.Rows) != 0 || resolver.calls != 0 {
		t.Fatalf("NOT IN with NULL = %#v, probes %d, error %v", result, resolver.calls, err)
	}
}
