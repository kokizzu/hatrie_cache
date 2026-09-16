package hatSql

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestC227ExternalGroupMergeMemoryBudget(t *testing.T) {
	result, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES
	('us'), ('eu'), ('apac'), ('us'), ('eu'), ('apac'), ('sa'), ('africa')
	AS src(region)
SELECT src.region, COUNT(*) AS total
GROUP BY src.region
ORDER BY src.region`, nil, SQLQueryOptions{
		MaxGroupBytes:      128,
		MaxGroupMergeBytes: 4096,
		SpillDirectory:     t.TempDir(),
		MaxSpillBytes:      1 << 20,
	})
	if err != nil {
		t.Fatalf("bounded group merge: %v", err)
	}
	want := []SQLRow{
		{"region": "africa", "total": int64(1)},
		{"region": "apac", "total": int64(2)},
		{"region": "eu", "total": int64(2)},
		{"region": "sa", "total": int64(1)},
		{"region": "us", "total": int64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("bounded group merge rows = %#v, want %#v", result.Rows, want)
	}
}

func TestC227ExternalGroupMergeMemoryBudgetCleansSpillFiles(t *testing.T) {
	spillDirectory := t.TempDir()
	_, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES
	('us'), ('eu'), ('apac'), ('us'), ('eu'), ('apac'), ('sa'), ('africa')
	AS src(region)
SELECT src.region, COUNT(*) AS total
GROUP BY src.region
ORDER BY src.region`, nil, SQLQueryOptions{
		MaxGroupBytes:      128,
		MaxGroupMergeBytes: 256,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
	})
	if err == nil || !strings.Contains(err.Error(), "SQL group merge memory budget exceeded") {
		t.Fatalf("bounded group merge error = %v, want merge memory budget error", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatalf("read spill directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after merge budget failure = %#v, want cleanup", entries)
	}
}

func TestC227NegativeExternalGroupMergeMemoryBudgetRejected(t *testing.T) {
	_, err := ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('us') AS src(region)
SELECT src.region`, nil, SQLQueryOptions{MaxGroupMergeBytes: -1})
	if err == nil || !strings.Contains(err.Error(), "SQL query budgets cannot be negative") {
		t.Fatalf("negative merge memory budget error = %v, want validation error", err)
	}
}

func TestC227ParallelExternalGroupMergeMemoryBudgetCleansSpillFiles(t *testing.T) {
	spillDirectory := t.TempDir()
	_, err := ExecuteSQLQueryContext(context.Background(), c227GroupQuery(64), nil, SQLQueryOptions{
		MaxGroupBytes:      128,
		MaxGroupMergeBytes: 256,
		SpillDirectory:     spillDirectory,
		MaxSpillBytes:      1 << 20,
		Workers:            2,
	})
	if err == nil || !strings.Contains(err.Error(), "SQL group merge memory budget exceeded") {
		t.Fatalf("parallel bounded group merge error = %v, want merge memory budget error", err)
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatalf("read parallel spill directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("parallel spill directory entries after merge budget failure = %#v, want cleanup", entries)
	}
}
