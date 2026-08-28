package hatSql

import (
	"context"
	"testing"
)

func TestParserPlannerMutationCorpusRejectsInvalidVariants(t *testing.T) {
	valid := "SELECT id FROM CACHE('orders') WHERE total >= 1 ORDER BY id LIMIT 2"
	if err := ValidateSQLQuery(valid); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	mutants := []string{
		"SELECT FROM CACHE('orders')",
		"SELECT id CACHE('orders')",
		"SELECT id FROM CACHE('orders') WHERE total >=",
		"SELECT id FROM CACHE('orders') ORDER id",
		"SELECT id FROM CACHE('orders') LIMIT -1",
	}
	for _, mutant := range mutants {
		if err := ValidateSQLQuery(mutant); err == nil {
			t.Errorf("mutant accepted: %q", mutant)
		}
	}
}

func TestSeededSQLCancellationWorkload(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := ExecuteSQLQueryParameters(ctx, "SELECT id FROM CACHE('orders')", SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
			t.Fatalf("seed %d resolver called after cancellation", seed)
			return nil, nil
		}), nil, SQLQueryOptions{})
		if err == nil {
			t.Fatalf("seed %d canceled query succeeded", seed)
		}
	}
}
