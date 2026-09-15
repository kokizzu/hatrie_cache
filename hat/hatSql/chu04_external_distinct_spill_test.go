package hatSql_test

import (
	"context"
	"os"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHU04ExternalDistinctUsesBoundedStreamingSpill(t *testing.T) {
	externalRows := []hatSql.Row{
		{"id": int64(1), "payload": "alpha"},
		{"id": int64(2), "payload": "bravo"},
		{"id": int64(1), "payload": "alpha"},
		{"id": int64(3), "payload": "charlie"},
		{"id": int64(2), "payload": "bravo"},
	}
	resolver := &chu02ExternalStreamResolver{rows: externalRows}
	spillDirectory := t.TempDir()
	options := hatSql.QueryOptions{
		MaxSetBytes:    64,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id, event.payload`, resolver, nil, options, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if resolver.streamCalls != 1 || resolver.materializedCalls != 0 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want stream-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result) != 3 {
		t.Fatalf("distinct result rows = %d, want 3: %#v", len(result), result)
	}
	wantIDs := []int64{1, 2, 3}
	for index, wantID := range wantIDs {
		if result[index]["id"] != wantID {
			t.Fatalf("result[%d].id = %#v, want %d", index, result[index]["id"], wantID)
		}
	}
	entries, err := os.ReadDir(spillDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %d, want cleanup", len(entries))
	}
}

func TestCHU04ExternalDistinctKeepsMaterializedFallback(t *testing.T) {
	resolver := &chu02ExternalStreamResolver{rows: []hatSql.Row{
		{"id": int64(1)},
		{"id": int64(1)},
		{"id": int64(2)},
	}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id`, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if resolver.streamCalls != 0 || resolver.materializedCalls != 1 {
		t.Fatalf("resolver calls = stream %d, materialized %d, want materialized-only", resolver.streamCalls, resolver.materializedCalls)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("distinct result = %#v, want 1 then 2", result.Rows)
	}
}

func TestCHU04ExternalDistinctCleansSpillAfterQuotaFailure(t *testing.T) {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index)}
	}
	spillDirectory := t.TempDir()
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.QueryOptions{
		MaxSetBytes:    64,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  256,
	}, func([]string, hatSql.SQLRow) error { return nil })
	if err == nil {
		t.Fatal("ExecuteSQLQueryRows() error = nil, want spill quota failure")
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after quota failure = %d, want cleanup", len(entries))
	}
}
