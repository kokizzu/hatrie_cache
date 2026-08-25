package hatriecache

import (
	"context"
	"testing"
	"time"
)

func TestQuerySQLTimeSeriesBuildsGapBucketsAndRollingValues(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.UpsertStringChecked("measurements", `[{"at":"2026-01-01T00:10:00Z","value":2},{"at":"2026-01-01T02:10:00Z","value":8}]`); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := QuerySQLTimeSeries(context.Background(), "FROM CACHE('measurements') AS measurement SELECT measurement.at, measurement.value", trie, nil, SQLQueryOptions{}, SQLTimeSeriesOptions{TimeField: "at", ValueField: "value", Start: start, End: start.Add(3 * time.Hour), Interval: time.Hour, RollingWindow: 2})
	if err != nil || len(result.Buckets) != 3 || result.Buckets[1].Count != 0 || len(result.Rolling) != 2 || result.Rolling[1].Value != 5 {
		t.Fatalf("QuerySQLTimeSeries() = %#v, %v", result, err)
	}
}
