package hatSql_test

import (
	"context"
	"os"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHG01GroupByWithoutOrderUsesExternalSpill(t *testing.T) {
	spillDirectory := t.TempDir()
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('us'), ('eu'), ('us'), ('apac'), ('eu') AS src(region)
SELECT src.region, COUNT(*) AS total
GROUP BY src.region`, nil, hatSql.SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("unordered grouped spill: %v", err)
	}
	want := []hatSql.SQLRow{
		{"region": "apac", "total": int64(1)},
		{"region": "eu", "total": int64(2)},
		{"region": "us", "total": int64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("unordered grouped spill rows = %#v, want %#v", result.Rows, want)
	}
}

func TestCHG01GroupBySpillCleansFilesAfterDiskBudgetFailure(t *testing.T) {
	spillDirectory := t.TempDir()
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM VALUES ('us'), ('eu'), ('us'), ('apac'), ('eu') AS src(region)
SELECT src.region, COUNT(*) AS total
GROUP BY src.region`, nil, hatSql.SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: spillDirectory,
		MaxSpillBytes:  1,
	})
	if err == nil {
		t.Fatal("disk budget failure = nil, want bounded spill error")
	}
	entries, readErr := os.ReadDir(spillDirectory)
	if readErr != nil {
		t.Fatalf("read spill directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("spill directory entries after failure = %#v, want cleanup", entries)
	}
}

type chg01CacheStreamResolver struct {
	rows []hatSql.SQLRow
}

func (resolver *chg01CacheStreamResolver) ResolveSQLSource(string, string) ([]hatSql.SQLRow, error) {
	return resolver.rows, nil
}

func (resolver *chg01CacheStreamResolver) StreamSQLSource(ctx context.Context, _, _ string, visit func(hatSql.SQLRow) error) error {
	for _, row := range resolver.rows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestCHG01CacheStreamGroupByWithoutOrderUsesExternalSpill(t *testing.T) {
	resolver := &chg01CacheStreamResolver{rows: []hatSql.SQLRow{
		{"region": "us"},
		{"region": "eu"},
		{"region": "us"},
		{"region": "apac"},
		{"region": "eu"},
	}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('events') AS src
SELECT src.region, COUNT(*) AS total
GROUP BY src.region`, resolver, hatSql.SQLQueryOptions{
		MaxGroupBytes:  1,
		SpillDirectory: t.TempDir(),
		MaxSpillBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("cache-stream unordered grouped spill: %v", err)
	}
	want := []hatSql.SQLRow{
		{"region": "apac", "total": int64(1)},
		{"region": "eu", "total": int64(2)},
		{"region": "us", "total": int64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("cache-stream grouped spill rows = %#v, want %#v", result.Rows, want)
	}
}
