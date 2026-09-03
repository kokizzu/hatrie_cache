package hatCache

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type borrowedPrefixTestResolver struct {
	trie    *HatTrie
	borrowed int
	cloned   int
}

func (resolver *borrowedPrefixTestResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver *borrowedPrefixTestResolver) ResolveSQLPrefixSource(name, key, field, prefix string) ([]SQLRow, bool, error) {
	resolver.cloned++
	return resolver.trie.ResolveSQLPrefixSource(name, key, field, prefix)
}

func (resolver *borrowedPrefixTestResolver) BorrowSQLPrefixSource(name, key, field, prefix string) ([]SQLRow, bool, error) {
	resolver.borrowed++
	return resolver.trie.BorrowSQLPrefixSource(name, key, field, prefix)
}

type clonedPrefixTestResolver struct {
	trie  *HatTrie
	calls int
}

func (resolver *clonedPrefixTestResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.trie.ResolveSQLSource(name, key)
}

func (resolver *clonedPrefixTestResolver) ResolveSQLPrefixSource(name, key, field, prefix string) ([]SQLRow, bool, error) {
	resolver.calls++
	return resolver.trie.ResolveSQLPrefixSource(name, key, field, prefix)
}

func TestSQLPrefixIndexUsesBorrowedCandidates(t *testing.T) {
	var _ hatSql.BorrowedPrefixIndexedSourceResolver = (*borrowedPrefixTestResolver)(nil)

	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"alpha"},{"id":2,"name":"alpine"},{"id":3,"name":"beta"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	resolver := &borrowedPrefixTestResolver{trie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.name LIKE 'al%' SELECT person.id ORDER BY person.id", resolver)
	if err != nil {
		t.Fatalf("prefix query error = %v", err)
	}
	want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("prefix query rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.borrowed == 0 || resolver.cloned != 0 {
		t.Fatalf("prefix resolver calls = borrowed %d, cloned %d; want borrowed only", resolver.borrowed, resolver.cloned)
	}
	result.Rows[0]["id"] = float64(99)
	again, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.name LIKE 'al%' SELECT person.id ORDER BY person.id", resolver)
	if err != nil {
		t.Fatalf("prefix query after result mutation error = %v", err)
	}
	if !reflect.DeepEqual(again.Rows, want) {
		t.Fatalf("prefix query after result mutation rows = %#v, want %#v", again.Rows, want)
	}
}

func TestSQLPrefixIndexRetainsClonedResolverFallback(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("people", `[{"id":1,"name":"alpha"},{"id":2,"name":"alpine"},{"id":3,"name":"beta"}]`)
	if err := trie.CreateSQLJSONFieldIndex("people", "name"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	resolver := &clonedPrefixTestResolver{trie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('people') AS person WHERE person.name LIKE 'al%' SELECT person.id ORDER BY person.id", resolver)
	if err != nil {
		t.Fatalf("cloned prefix query error = %v", err)
	}
	if want := []SQLRow{{"id": float64(1)}, {"id": float64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("cloned prefix query rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.calls != 1 {
		t.Fatalf("cloned prefix resolver calls = %d, want 1", resolver.calls)
	}
}
