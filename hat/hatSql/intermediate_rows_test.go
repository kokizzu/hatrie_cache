package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestSQLMaxIntermediateRowsRejectsJoinExpansion(t *testing.T) {
	resolver := SourceResolverFunc(func(_, key string) ([]Row, error) {
		switch key {
		case "left":
			return []Row{{"id": 1, "key": "x"}, {"id": 2, "key": "x"}}, nil
		case "right":
			return []Row{{"id": 10, "key": "x"}, {"id": 20, "key": "x"}}, nil
		default:
			return nil, nil
		}
	})
	result, err := ExecuteSQLQueryContext(context.Background(), `FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id`, resolver, SQLQueryOptions{MaxIntermediateRows: 3})
	if err == nil || !strings.Contains(err.Error(), "3 row limit") {
		t.Fatalf("join with intermediate row limit rows=%d error = %v, want 3 row limit error", len(result.Rows), err)
	}
}

func TestSQLMaxIntermediateRowsZeroPreservesJoinResults(t *testing.T) {
	resolver := SourceResolverFunc(func(_, key string) ([]Row, error) {
		if key == "left" {
			return []Row{{"id": 1, "key": "x"}, {"id": 2, "key": "x"}}, nil
		}
		return []Row{{"id": 10, "key": "x"}, {"id": 20, "key": "x"}}, nil
	})
	result, err := ExecuteSQLQueryContext(context.Background(), `FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.key = r.key SELECT l.id, r.id`, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("join with disabled intermediate row limit: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("join rows = %d, want 4", len(result.Rows))
	}
}

func TestSQLMaxIntermediateRowsRejectsNegativeOption(t *testing.T) {
	_, err := ExecuteSQLQueryContext(context.Background(), "SELECT 1", nil, SQLQueryOptions{MaxIntermediateRows: -1})
	if err == nil || !strings.Contains(err.Error(), "cannot be negative") {
		t.Fatalf("negative MaxIntermediateRows error = %v, want negative budget error", err)
	}
}

func TestSQLMaxIntermediateRowsDisablesResultCache(t *testing.T) {
	if sqlResultCacheOptionsEligible(SQLQueryOptions{MaxIntermediateRows: 1}) {
		t.Fatal("MaxIntermediateRows must bypass the result cache")
	}
}

func TestSQLMaxIntermediateRowsNamespacePolicyTightensOptions(t *testing.T) {
	policy := NamespaceResourceLimits{MaxIntermediateRows: 3}
	if got := policy.Apply(SQLQueryOptions{}).MaxIntermediateRows; got != 3 {
		t.Fatalf("default intermediate row policy = %d, want 3", got)
	}
	if got := policy.Apply(SQLQueryOptions{MaxIntermediateRows: 2}).MaxIntermediateRows; got != 2 {
		t.Fatalf("stricter intermediate row option = %d, want 2", got)
	}
}
