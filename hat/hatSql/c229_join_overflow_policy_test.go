package hatSql_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestC229ExplicitSpillJoinPolicyRequiresBoundedSpill(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "name": "Ada"}},
	}}
	query := "FROM CACHE('left') AS l CROSS JOIN CACHE('right') AS r SELECT l.id, r.name"
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{
		JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
	})
	if err == nil || !strings.Contains(err.Error(), "MaxJoinBytes") {
		t.Fatalf("spill policy error = %v, want bounded spill configuration error", err)
	}
}

func TestC229ExplicitSpillJoinPolicyPreservesRowsAndCleansFiles(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left": {
			{"id": 1, "k": "a"},
			{"id": 2, "k": "b"},
			{"id": 3, "k": "a"},
		},
		"right": {
			{"k": "a", "name": "Ada"},
			{"k": "b", "name": "Bea"},
			{"k": "a", "name": "Cia"},
		},
	}}
	directory := t.TempDir()
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.name"
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{
		JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
		MaxJoinBytes:       128,
		SpillDirectory:     directory,
		MaxSpillBytes:      1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{
		{"id": 1, "name": "Ada"},
		{"id": 1, "name": "Cia"},
		{"id": 2, "name": "Bea"},
		{"id": 3, "name": "Ada"},
		{"id": 3, "name": "Cia"},
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("spilled join rows = %#v, want %#v", result.Rows, want)
	}
	for index := range want {
		if result.Rows[index]["id"] != want[index]["id"] || result.Rows[index]["name"] != want[index]["name"] {
			t.Fatalf("spilled join row %d = %#v, want %#v", index, result.Rows[index], want[index])
		}
	}
	if resolver.streamCalls != 2 {
		t.Fatalf("stream calls = %d, want two streamed join inputs", resolver.streamCalls)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries = %#v, want cleanup", entries)
	}
}

func TestC229ExplicitRejectBoundsMaterializedJoinInput(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "name": strings.Repeat("x", 512)}},
	}}
	directory := t.TempDir()
	query := "FROM CACHE('left') AS l CROSS JOIN CACHE('right') AS r SELECT l.id, r.name"
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{
		JoinOverflowPolicy: hatSql.SQLJoinOverflowReject,
		MaxJoinBytes:       128,
		SpillDirectory:     directory,
		MaxSpillBytes:      1 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "byte budget") {
		t.Fatalf("reject policy error = %v, want byte-budget rejection", err)
	}
	if resolver.streamCalls != 0 {
		t.Fatalf("stream calls = %d, want no spill stream", resolver.streamCalls)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("reject spill directory entries = %#v, want cleanup", entries)
	}
}

func TestC229ExplicitSpillRejectsUnsupportedJoinShape(t *testing.T) {
	resolver := &exactJoinPlanResolver{sources: map[string][]hatSql.Row{
		"left":  {{"id": 1, "k": "team"}},
		"right": {{"k": "team", "name": "Ada"}},
	}}
	directory := t.TempDir()
	query := "FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k WHERE l.id > 0 SELECT l.id, r.name"
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{
		JoinOverflowPolicy: hatSql.SQLJoinOverflowSpill,
		MaxJoinBytes:       128,
		SpillDirectory:     directory,
		MaxSpillBytes:      1 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "direct two-source") {
		t.Fatalf("unsupported spill policy error = %v, want shape rejection", err)
	}
	entries, readErr := os.ReadDir(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("unsupported spill directory entries = %#v, want cleanup", entries)
	}
}

func TestC229InvalidJoinOverflowPolicy(t *testing.T) {
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), "VALUES (1) SELECT *", nil, hatSql.QueryOptions{
		JoinOverflowPolicy: "truncate",
	})
	if err == nil || !strings.Contains(err.Error(), "join overflow policy") {
		t.Fatalf("invalid policy error = %v, want policy validation error", err)
	}
}
