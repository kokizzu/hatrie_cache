package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestSQLMaxGroupKeysRejectsHighCardinality(t *testing.T) {
	query := `FROM VALUES ('east', 1), ('west', 2) AS src(region, value) SELECT src.region, COUNT(*) AS total GROUP BY src.region`
	_, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{MaxGroupKeys: 1})
	if err == nil || !strings.Contains(err.Error(), "SQL group key limit exceeded") {
		t.Fatalf("MaxGroupKeys error = %v, want group key limit error", err)
	}
}

func TestSQLMaxGroupKeysZeroPreservesGrouping(t *testing.T) {
	query := `FROM VALUES ('east', 1), ('west', 2) AS src(region, value) SELECT src.region, COUNT(*) AS total GROUP BY src.region`
	result, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("unlimited GROUP BY error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("unlimited GROUP BY rows = %d, want 2", len(result.Rows))
	}
}

func TestSQLMaxGroupKeysRejectsNegativeOption(t *testing.T) {
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT 1", nil, SQLQueryOptions{MaxGroupKeys: -1})
	if err == nil || !strings.Contains(err.Error(), "SQL query budgets cannot be negative") {
		t.Fatalf("negative MaxGroupKeys error = %v, want negative budget error", err)
	}
}

func TestNamespaceResourceLimitsApplyMaxGroupKeys(t *testing.T) {
	limits := NamespaceResourceLimits{MaxGroupKeys: 4}
	if got := limits.Apply(SQLQueryOptions{MaxGroupKeys: 9}).MaxGroupKeys; got != 4 {
		t.Fatalf("tightened MaxGroupKeys = %d, want 4", got)
	}
	if got := limits.Apply(SQLQueryOptions{MaxGroupKeys: 2}).MaxGroupKeys; got != 2 {
		t.Fatalf("caller MaxGroupKeys = %d, want stricter 2", got)
	}
}
