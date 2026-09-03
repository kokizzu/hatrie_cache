package hatCache

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestHatTrieSQLKeysetPaginationPreservesDuplicateOrderValues(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":1},{"id":"c","score":2},{"id":"d","score":3},{"id":"e","score":3},{"id":"f","score":4}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	third, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, second.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []hatSql.Row{{"id": "a", "score": float64(1)}, {"id": "b", "score": float64(1)}}
	wantSecond := []hatSql.Row{{"id": "c", "score": float64(2)}, {"id": "d", "score": float64(3)}}
	wantThird := []hatSql.Row{{"id": "e", "score": float64(3)}, {"id": "f", "score": float64(4)}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) || !reflect.DeepEqual(third.Rows, wantThird) {
		t.Fatalf("HatTrie keyset pages = %#v / %#v / %#v", first.Rows, second.Rows, third.Rows)
	}
	if third.HasMore || third.NextCursor != "" {
		t.Fatalf("final HatTrie keyset page = has_more=%t cursor=%q", third.HasMore, third.NextCursor)
	}
}

func TestHatTrieSQLKeysetPaginationPreservesDescendingNullTail(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":1},{"id":"c","score":2},{"id":"d","score":3},{"id":"n","score":null}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score DESC NULLS LAST"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	third, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 2, second.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []hatSql.Row{{"id": "d", "score": float64(3)}, {"id": "c", "score": float64(2)}}
	wantSecond := []hatSql.Row{{"id": "a", "score": float64(1)}, {"id": "b", "score": float64(1)}}
	wantThird := []hatSql.Row{{"id": "n", "score": nil}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) || !reflect.DeepEqual(third.Rows, wantThird) {
		t.Fatalf("descending NULLS LAST pages = %#v / %#v / %#v", first.Rows, second.Rows, third.Rows)
	}
}

func TestHatTrieSQLKeysetPaginationContinuesAllNullSource(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":null},{"id":"b","score":null}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score NULLS FIRST"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, hatSql.SQLQueryOptions{}, 1, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []hatSql.Row{{"id": "a", "score": nil}}
	wantSecond := []hatSql.Row{{"id": "b", "score": nil}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) {
		t.Fatalf("all-null keyset pages = %#v / %#v", first.Rows, second.Rows)
	}
}

func TestHatTrieSQLKeysetPaginationRejectsWrappedNullTie(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":null}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	available, err := trie.StreamSQLOrderedSourceAfter(
		context.Background(), "CACHE", "events", "score", false, true, false,
		hatSql.SQLKeysetPosition{Tie: ^uint64(0), Valid: true},
		func(hatSql.Row, hatSql.SQLKeysetPosition) error { return nil },
	)
	if !available {
		t.Fatal("keyset stream unavailable")
	}
	if err == nil {
		t.Fatal("wrapped NULL tie unexpectedly accepted")
	}
}

func TestHatTrieSQLKeysetPaginationFacade(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":2}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score", trie, nil, SQLQueryOptions{}, 1, "")
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": "a"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("facade rows = %#v, want %#v", result.Rows, want)
	}
}

func TestHatTrieSQLKeysetPaginationUsesTypedInt64Index(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":2},{"id":"c","score":3}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "events", Fields: []string{"score"}, Type: SQLIndexInt64}); err != nil {
		t.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	first, err := ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": "a", "score": float64(1)}, {"id": "b", "score": float64(2)}}; !reflect.DeepEqual(first.Rows, want) {
		t.Fatalf("typed keyset first page = %#v, want %#v", first.Rows, want)
	}
	if want := []SQLRow{{"id": "c", "score": float64(3)}}; !reflect.DeepEqual(second.Rows, want) {
		t.Fatalf("typed keyset second page = %#v, want %#v", second.Rows, want)
	}
}

func TestHatTrieSQLKeysetPaginationTypedDescendingNullTail(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":"a","score":1},{"id":"b","score":1},{"id":"c","score":2},{"id":"n","score":null}]`)
	if err := trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{CacheKey: "events", Fields: []string{"score"}, Type: SQLIndexInt64}); err != nil {
		t.Fatal(err)
	}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score DESC NULLS LAST"
	first, err := ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := ExecuteSQLQueryKeysetPage(context.Background(), query, trie, nil, SQLQueryOptions{}, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := []SQLRow{{"id": "c", "score": float64(2)}, {"id": "a", "score": float64(1)}}
	wantSecond := []SQLRow{{"id": "b", "score": float64(1)}, {"id": "n", "score": nil}}
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) || second.HasMore || second.NextCursor != "" {
		t.Fatalf("typed descending keyset pages = %#v / %#v, has_more=%t cursor=%q", first.Rows, second.Rows, second.HasMore, second.NextCursor)
	}
}
