package hatSql_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLValidAtPredicateUsesHalfOpenIntervals(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return []hatSql.Row{
			{"id": int64(1), "valid_from": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
			{"id": int64(2), "valid_from": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "valid_to": nil},
			{"id": int64(3), "valid_from": nil, "valid_to": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
			{"id": int64(4), "valid_from": nil, "valid_to": nil},
			{"id": int64(5), "valid_from": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		}, nil
	})
	query := "SELECT id FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-01T00:00:00Z', valid_from, valid_to) ORDER BY id"

	result, err := hatSql.ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(4) {
		t.Fatalf("valid rows = %#v, want ids 1 and 4", result.Rows)
	}

	result, err = hatSql.ExecuteSQLQuery("SELECT id FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-02T00:00:00Z', valid_from, valid_to) ORDER BY id", resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() at upper boundary error = %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(2) || result.Rows[1]["id"] != int64(4) {
		t.Fatalf("upper-boundary rows = %#v, want ids 2 and 4", result.Rows)
	}
}

func TestSQLValidAtPredicateStreams(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return []hatSql.Row{
			{"id": int64(1), "valid_from": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
			{"id": int64(2), "valid_from": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "valid_to": nil},
		}, nil
	})
	var ids []int64
	err := hatSql.ExecuteSQLQueryRows(
		context.Background(),
		"SELECT id FROM CACHE('events') WHERE VALID_AT(TIMESTAMP '2026-01-01T12:00:00Z', valid_from, valid_to)",
		resolver,
		nil,
		hatSql.SQLQueryOptions{},
		func(_ []string, row hatSql.Row) error {
			ids = append(ids, row["id"].(int64))
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("streamed ids = %#v, want [1]", ids)
	}
}

func TestSQLValidAtPredicateSupportsComputedArguments(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return []hatSql.Row{
			{"id": int64(1), "as_of": nil, "valid_from": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
			{"id": int64(2), "as_of": time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC), "valid_from": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "valid_to": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)},
		}, nil
	})
	result, err := hatSql.ExecuteSQLQuery(
		"SELECT id FROM CACHE('events') WHERE VALID_AT(COALESCE(as_of, TIMESTAMP '2026-01-01T12:00:00Z'), valid_from, valid_to) ORDER BY id",
		resolver,
	)
	if err != nil {
		t.Fatalf("computed VALID_AT query error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("computed VALID_AT rows = %#v, want id 1", result.Rows)
	}
}

func TestSQLValidAtPredicateValidatesArguments(t *testing.T) {
	_, err := hatSql.ExecuteSQLQuery("FROM VALUES (1) AS src(id) SELECT VALID_AT(TIMESTAMP '2026-01-01T00:00:00Z', TIMESTAMP '2026-01-01T00:00:00Z')", nil)
	if err == nil || !strings.Contains(err.Error(), "VALID_AT expects exactly three arguments") {
		t.Fatalf("invalid VALID_AT arguments error = %v", err)
	}
}
