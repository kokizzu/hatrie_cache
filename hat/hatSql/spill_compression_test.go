package hatSql_test

import (
	"context"
	"os"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCompressedSQLSortSpillExecutes(t *testing.T) {
	t.Parallel()
	inspected := false
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM VALUES ('repeat-repeat-repeat'), ('alpha') AS values(name) SELECT name ORDER BY name", hatSql.SQLSourceResolverFunc(nil), hatSql.SQLQueryOptions{
		MaxSortBytes:   1,
		MaxSpillBytes:  1 << 20,
		SpillDirectory: t.TempDir(),
		CompressSpill:  true,
		SpillFaults: &hatSql.SQLSpillFaults{BeforeRead: func(kind, path string) error {
			if kind != "sort" {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
				t.Fatalf("spill file is not gzip framed: %x", data)
			}
			inspected = true
			return nil
		}},
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	if !inspected || len(result.Rows) != 2 || result.Rows[0]["name"] != "alpha" || result.Rows[1]["name"] != "repeat-repeat-repeat" {
		t.Fatalf("compressed spill result = %#v, inspected=%v", result, inspected)
	}
}
