package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestSQLMaxGroupKeysDisablesSpillFallback(t *testing.T) {
	query := `FROM VALUES ('east', 1), ('west', 2) AS src(region, value) SELECT src.region, COUNT(*) AS total GROUP BY src.region`
	_, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{
		MaxGroupKeys:   1,
		MaxGroupBytes:  1,
		MaxSpillBytes:  1 << 20,
		SpillDirectory: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "SQL group key limit exceeded") {
		t.Fatalf("combined group budgets error = %v, want group key limit error", err)
	}
}
