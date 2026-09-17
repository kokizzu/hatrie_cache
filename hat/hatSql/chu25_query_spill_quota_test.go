package hatSql_test

import (
	"context"
	"os"
	"strings"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestCHU25QuerySpillQuotaAppliesAcrossQuery(t *testing.T) {
	rows := make([]hatSql.Row, 128)
	for index := range rows {
		rows[index] = hatSql.Row{
			"id":      int64(index),
			"payload": strings.Repeat("payload-", 8),
		}
	}
	spillDirectory := t.TempDir()
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.SQLQueryOptions{
		MaxSortBytes:       128,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
		MaxQuerySpillBytes: 256,
	}, func([]string, hatSql.SQLRow) error { return nil })
	if err == nil {
		t.Fatal("ExecuteSQLQueryRows() error = nil, want query-wide spill quota failure")
	}
	if !strings.Contains(err.Error(), "query spill") {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want query spill quota diagnostic", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after query quota failure = %d, want cleanup", len(entries))
	}
}

func TestCHU25QuerySpillQuotaZeroPreservesExistingSpillBudget(t *testing.T) {
	rows := make([]hatSql.Row, 32)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index)}
	}
	spillDirectory := t.TempDir()
	var result []hatSql.Row
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT event.id
ORDER BY event.id DESC`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.SQLQueryOptions{
		MaxSortBytes:   128,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	}, func(_ []string, row hatSql.SQLRow) error {
		result = append(result, hatSql.Row(row))
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if len(result) != len(rows) || result[0]["id"] != int64(31) || result[len(result)-1]["id"] != int64(0) {
		t.Fatalf("ordered result endpoints = %#v and %#v, want 31 and 0", result[0]["id"], result[len(result)-1]["id"])
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after successful query = %d, want cleanup", len(entries))
	}
}

func TestCHU25QuerySpillQuotaAppliesToExternalDistinct(t *testing.T) {
	rows := make([]hatSql.Row, 64)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index)}
	}
	spillDirectory := t.TempDir()
	err := hatSql.ExecuteSQLQueryRows(context.Background(), `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id`, &chu02ExternalStreamResolver{rows: rows}, nil, hatSql.SQLQueryOptions{
		MaxSetBytes:        64,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
		MaxQuerySpillBytes: 1,
	}, func([]string, hatSql.SQLRow) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "query spill") {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want query spill quota failure", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("distinct spill directory entries after failure = %d, want cleanup", len(entries))
	}
}

func TestCHU25QuerySpillQuotaAppliesToGroupAggregate(t *testing.T) {
	spillDirectory := t.TempDir()
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('us'), ('eu'), ('us'), ('apac'), ('eu') AS src(region)
SELECT src.region, COUNT(*) AS total
GROUP BY src.region`, nil, hatSql.SQLQueryOptions{
		MaxGroupBytes:      1,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
		MaxQuerySpillBytes: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "query spill") {
		t.Fatalf("ExecuteSQLQueryContext() error = %v, want query spill quota failure", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("group spill directory entries after failure = %d, want cleanup", len(entries))
	}
}

func TestCHU25QuerySpillQuotaAppliesToHashJoin(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "name": "Ada"}},
	}}
	spillDirectory := t.TempDir()
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.name"
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.SQLQueryOptions{
		MaxJoinBytes:       128,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
		MaxQuerySpillBytes: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "query spill") {
		t.Fatalf("ExecuteSQLQueryContext() error = %v, want query spill quota failure", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("hash spill directory entries after failure = %d, want cleanup", len(entries))
	}
}

func TestCHU25QuerySpillQuotaRejectsNegativeValue(t *testing.T) {
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM VALUES (1) AS src(id)
SELECT src.id`, nil, hatSql.SQLQueryOptions{MaxQuerySpillBytes: -1})
	if err == nil || !strings.Contains(err.Error(), "budgets cannot be negative") {
		t.Fatalf("negative query spill quota error = %v, want validation failure", err)
	}
}
