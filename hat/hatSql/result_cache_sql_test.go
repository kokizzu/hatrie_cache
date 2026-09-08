package hatSql

import (
	"context"
	"errors"
	"testing"
)

type sqlResultCacheVersionedResolver struct {
	rows        []Row
	version     string
	sourceCalls int
	sourceErr   error
}

func (resolver *sqlResultCacheVersionedResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.sourceCalls++
	if resolver.sourceErr != nil {
		return nil, resolver.sourceErr
	}
	rows := make([]Row, len(resolver.rows))
	for index, row := range resolver.rows {
		rows[index] = make(Row, len(row))
		for field, value := range row {
			rows[index][field] = value
		}
	}
	return rows, nil
}

func (resolver *sqlResultCacheVersionedResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "events" {
		return "", false, nil
	}
	return resolver.version, true, nil
}

func TestExecuteSQLResultCacheReusesTypedRowsAndInvalidatesBySourceVersion(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1), "payload": []byte("original")}},
		version: "v1",
	}
	options := SQLQueryOptions{ResultCache: NewSQLResultCache(2)}
	query := "FROM CACHE('events') SELECT id, payload"

	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("first query error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("cached query error = %v", err)
	}
	if resolver.sourceCalls != 1 {
		t.Fatalf("source calls after cache hit = %d, want 1", resolver.sourceCalls)
	}
	if first.Rows[0]["id"] != int64(1) || second.Rows[0]["id"] != int64(1) {
		t.Fatalf("cached ids = %#v and %#v, want int64(1)", first.Rows, second.Rows)
	}
	second.Rows[0]["id"] = int64(99)
	second.Rows[0]["payload"].([]byte)[0] = 'X'
	independent, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("independent cached query error = %v", err)
	}
	if independent.Rows[0]["id"] != int64(1) || string(independent.Rows[0]["payload"].([]byte)) != "original" {
		t.Fatalf("cached result was mutated through a returned row: %#v", independent.Rows)
	}

	resolver.rows = []Row{{"id": int64(2), "payload": []byte("updated")}}
	resolver.version = "v2"
	third, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options)
	if err != nil {
		t.Fatalf("invalidated query error = %v", err)
	}
	if resolver.sourceCalls != 2 || third.Rows[0]["id"] != int64(2) {
		t.Fatalf("invalidated query calls/rows = %d/%#v, want 2 and id 2", resolver.sourceCalls, third.Rows)
	}
}

func TestExecuteSQLResultCacheSeparatesParametersAndQueryIDs(t *testing.T) {
	resolver := &sqlResultCacheVersionedResolver{
		rows:    []Row{{"id": int64(1)}, {"id": int64(2)}},
		version: "v1",
	}
	cache := NewSQLResultCache(4)
	query := "SELECT id FROM CACHE('events') WHERE id = $1"
	firstOptions := SQLQueryOptions{QueryID: "first", ResultCache: cache}
	secondOptions := SQLQueryOptions{QueryID: "second", ResultCache: cache}
	first, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(1)}, firstOptions)
	if err != nil {
		t.Fatalf("first parameterized query error = %v", err)
	}
	second, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(2)}, secondOptions)
	if err != nil {
		t.Fatalf("second parameterized query error = %v", err)
	}
	firstAgain, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, []interface{}{int64(1)}, secondOptions)
	if err != nil {
		t.Fatalf("cached parameterized query error = %v", err)
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("parameterized source calls = %d, want 2", resolver.sourceCalls)
	}
	if len(first.Rows) != 1 || first.Rows[0]["id"] != int64(1) || len(second.Rows) != 1 || second.Rows[0]["id"] != int64(2) || len(firstAgain.Rows) != 1 || firstAgain.Rows[0]["id"] != int64(1) {
		t.Fatalf("parameterized rows = %#v, %#v, %#v", first.Rows, second.Rows, firstAgain.Rows)
	}
	if first.QueryID != "first" || second.QueryID != "second" || firstAgain.QueryID != "second" {
		t.Fatalf("query IDs = %q, %q, %q", first.QueryID, second.QueryID, firstAgain.QueryID)
	}
}

func TestExecuteSQLResultCacheBypassesUnversionedResolver(t *testing.T) {
	sourceCalls := 0
	resolver := SourceResolverFunc(func(name, key string) ([]Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		sourceCalls++
		return []Row{{"id": int64(1)}}, nil
	})
	options := SQLQueryOptions{ResultCache: NewSQLResultCache(2)}
	query := "SELECT id FROM CACHE('events')"
	for range 2 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("unversioned query error = %v", err)
		}
	}
	if sourceCalls != 2 {
		t.Fatalf("unversioned source calls = %d, want 2", sourceCalls)
	}
}

type sqlResultCacheDriftingVersionResolver struct {
	versions    []string
	versionRead int
	sourceCalls int
}

func (resolver *sqlResultCacheDriftingVersionResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.sourceCalls++
	return []Row{{"id": int64(1)}}, nil
}

func (resolver *sqlResultCacheDriftingVersionResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "events" || resolver.versionRead >= len(resolver.versions) {
		return "", false, nil
	}
	version := resolver.versions[resolver.versionRead]
	resolver.versionRead++
	return version, true, nil
}

func TestExecuteSQLResultCacheDoesNotRetainVersionChangedDuringExecution(t *testing.T) {
	resolver := &sqlResultCacheDriftingVersionResolver{versions: []string{"v1", "v2", "v2", "v2", "v2"}}
	options := SQLQueryOptions{ResultCache: NewSQLResultCache(1)}
	query := "SELECT id FROM CACHE('events')"
	for range 3 {
		if _, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
			t.Fatalf("drifting-version query error = %v", err)
		}
	}
	if resolver.sourceCalls != 2 {
		t.Fatalf("source calls after drift and hit = %d, want 2", resolver.sourceCalls)
	}
}

func TestExecuteSQLResultCacheReturnsExecutionErrors(t *testing.T) {
	wantErr := errors.New("source failed")
	resolver := &sqlResultCacheVersionedResolver{version: "v1", sourceErr: wantErr}
	result, err := ExecuteSQLQueryParameters(context.Background(), "SELECT id FROM CACHE('events')", resolver, nil, SQLQueryOptions{
		QueryID:     "failed-query",
		ResultCache: NewSQLResultCache(1),
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("query error = %v, want %v", err, wantErr)
	}
	if result.QueryID != "failed-query" {
		t.Fatalf("error query ID = %q, want failed-query", result.QueryID)
	}
}
