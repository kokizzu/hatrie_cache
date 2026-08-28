package hatCache

import (
	"context"
	"testing"
	"time"
)

func TestQuerySQLTimeSeriesPrunesConfiguredTimePartitions(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	janStart := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	febStart := janStart.AddDate(0, 1, 0)
	marStart := febStart.AddDate(0, 1, 0)
	trie.UpsertString("events:2026-01", `[{"at":"2026-01-15T00:00:00Z","value":2}]`)
	trie.UpsertString("events:2026-02", `[{"at":"2026-02-15T00:00:00Z","value":8}]`)
	if err := trie.ConfigureSQLTimePartitions("events", []SQLTimePartition{
		{Key: "events:2026-01", Start: janStart, End: febStart},
		{Key: "events:2026-02", Start: febStart, End: marStart},
	}); err != nil {
		t.Fatalf("ConfigureSQLTimePartitions() error = %v", err)
	}
	plan, available := trie.SQLTimePartitionPruningPlan("events", febStart, marStart)
	if !available || len(plan.Keys) != 1 || plan.Keys[0] != "events:2026-02" {
		t.Fatalf("SQLTimePartitionPruningPlan() = %#v, %t", plan, available)
	}
	result, err := QuerySQLTimeSeries(context.Background(), "FROM CACHE('events') AS event SELECT event.at, event.value", trie, nil, SQLQueryOptions{}, SQLTimeSeriesOptions{TimeField: "at", ValueField: "value", Start: febStart, End: marStart, Interval: 24 * time.Hour})
	if err != nil {
		t.Fatalf("QuerySQLTimeSeries() error = %v", err)
	}
	if len(result.Buckets) != 28 || result.Buckets[14].Count != 1 || result.Buckets[14].Average != 8 {
		t.Fatalf("time-partitioned result = %#v", result)
	}
}

func TestConfigureSQLTimePartitionsRejectsOverlappingRanges(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	err := trie.ConfigureSQLTimePartitions("events", []SQLTimePartition{
		{Key: "events:a", Start: start, End: start.Add(24 * time.Hour)},
		{Key: "events:b", Start: start.Add(12 * time.Hour), End: start.Add(36 * time.Hour)},
	})
	if err == nil {
		t.Fatal("ConfigureSQLTimePartitions() accepted overlapping ranges")
	}
}
