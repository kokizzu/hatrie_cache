package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlVersionedColumnarConditionResolver struct {
	batch   ColumnarBatch
	version string
}

type sqlUnversionedColumnarConditionResolver struct{ batch ColumnarBatch }

type sqlDriftingColumnarConditionResolver struct {
	batch    ColumnarBatch
	versions []string
	calls    int
}

func (resolver *sqlVersionedColumnarConditionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a columnar condition cache query")
}

func (resolver *sqlVersionedColumnarConditionResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver *sqlVersionedColumnarConditionResolver) SQLSourceVersion(string, string) (string, bool, error) {
	return resolver.version, true, nil
}

func (resolver sqlUnversionedColumnarConditionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for an unversioned columnar query")
}

func (resolver sqlUnversionedColumnarConditionResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver *sqlDriftingColumnarConditionResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a drifting columnar query")
}

func (resolver *sqlDriftingColumnarConditionResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver *sqlDriftingColumnarConditionResolver) SQLSourceVersion(string, string) (string, bool, error) {
	version := resolver.versions[resolver.calls]
	resolver.calls++
	return version, true, nil
}

func TestSQLQueryConditionCacheReusesVersionedSelection(t *testing.T) {
	resolver := &sqlVersionedColumnarConditionResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3)},
			"score": {int64(7), int64(8), int64(7)},
		}, Rows: 3},
		version: "v1",
	}
	cache := NewSQLQueryConditionCache(2, 8)
	options := SQLQueryOptions{ConditionCache: cache}
	query := "SELECT id FROM CACHE('items') WHERE score = 7"
	for iteration := 0; iteration < 2; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		if want := []SQLRow{{"id": int64(1)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("iteration %d rows = %#v, want %#v", iteration, result.Rows, want)
		}
	}
	if got, want := cache.Stats(), (SQLQueryConditionCacheStats{Entries: 1, Hits: 1, Misses: 1}); got != want {
		t.Fatalf("cache stats = %#v, want %#v", got, want)
	}

	resolver.batch.Columns["score"] = []interface{}{int64(7), int64(7), int64(8)}
	resolver.version = "v2"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	if want := []SQLRow{{"id": int64(1)}, {"id": int64(2)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("versioned rows = %#v, want %#v", result.Rows, want)
	}
	if got, want := cache.Stats(), (SQLQueryConditionCacheStats{Entries: 2, Hits: 1, Misses: 2}); got != want {
		t.Fatalf("cache stats after version change = %#v, want %#v", got, want)
	}
}

func TestSQLQueryConditionCacheStreamsVersionedSelection(t *testing.T) {
	resolver := &sqlVersionedColumnarConditionResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{
			"id":    {int64(1), int64(2), int64(3)},
			"score": {int64(7), int64(8), int64(7)},
		}, Rows: 3},
		version: "v1",
	}
	cache := NewSQLQueryConditionCache(1, 8)
	options := SQLQueryOptions{ConditionCache: cache}
	var rows []SQLRow
	for iteration := 0; iteration < 2; iteration++ {
		rows = rows[:0]
		err := ExecuteSQLQueryRows(context.Background(), "SELECT id FROM CACHE('items') WHERE score = 7", resolver, nil, options, func(_ []string, row SQLRow) error {
			rows = append(rows, row)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if want := []SQLRow{{"id": int64(1)}, {"id": int64(3)}}; !reflect.DeepEqual(rows, want) {
			t.Fatalf("iteration %d rows = %#v, want %#v", iteration, rows, want)
		}
	}
	if got, want := cache.Stats(), (SQLQueryConditionCacheStats{Entries: 1, Hits: 1, Misses: 1}); got != want {
		t.Fatalf("cache stats = %#v, want %#v", got, want)
	}
}

func TestSQLQueryConditionCacheSkipsUnversionedSource(t *testing.T) {
	resolver := sqlUnversionedColumnarConditionResolver{batch: ColumnarBatch{Columns: map[string][]interface{}{
		"id":    {int64(1)},
		"score": {int64(7)},
	}, Rows: 1}}
	cache := NewSQLQueryConditionCache(1, 8)
	if _, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') WHERE score = 7", resolver, nil, SQLQueryOptions{ConditionCache: cache}); err != nil {
		t.Fatal(err)
	}
	if got := cache.Stats(); got != (SQLQueryConditionCacheStats{}) {
		t.Fatalf("unversioned cache stats = %#v, want zero", got)
	}
}

func TestSQLQueryConditionCacheSkipsDriftingSourceVersion(t *testing.T) {
	resolver := &sqlDriftingColumnarConditionResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{
			"id":    {int64(1)},
			"score": {int64(7)},
		}, Rows: 1},
		versions: []string{"v1", "v2", "v3", "v4"},
	}
	cache := NewSQLQueryConditionCache(2, 8)
	options := SQLQueryOptions{ConditionCache: cache}
	for iteration := 0; iteration < 2; iteration++ {
		result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('items') WHERE score = 7", resolver, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		if want := []SQLRow{{"id": int64(1)}}; !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("iteration %d rows = %#v, want %#v", iteration, result.Rows, want)
		}
	}
	if got := cache.Stats(); got != (SQLQueryConditionCacheStats{}) {
		t.Fatalf("drifting cache stats = %#v, want zero", got)
	}
}

func TestSQLQueryConditionCacheBoundsEntriesAndMatches(t *testing.T) {
	cache := NewSQLQueryConditionCache(1, 1)
	first := sqlQueryConditionCacheKey{sourceKind: "CACHE", sourceKey: "items", version: "v1", predicate: "score = 7", rows: 2}
	second := sqlQueryConditionCacheKey{sourceKind: "CACHE", sourceKey: "items", version: "v2", predicate: "score = 7", rows: 2}
	cache.put(first, []int{0, 1})
	if got := cache.Stats(); got.Entries != 0 {
		t.Fatalf("oversized cache entries = %d, want 0", got.Entries)
	}
	cache.put(first, []int{0})
	cache.put(second, []int{1})
	if _, found := cache.get(first); found {
		t.Fatal("evicted cache entry remained available")
	}
	if matches, found := cache.get(second); !found || !reflect.DeepEqual(matches, []int{1}) {
		t.Fatalf("latest cache matches = %#v, found %t", matches, found)
	}
	if got, want := cache.Stats(), (SQLQueryConditionCacheStats{Entries: 1, Hits: 1, Misses: 1}); got != want {
		t.Fatalf("bounded cache stats = %#v, want %#v", got, want)
	}
}
