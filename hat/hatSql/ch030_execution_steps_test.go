//go:build !ch030baseline

package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestCH030ExecutionStepsRejectsExcessWork(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxExecutionSteps: 2})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()
	if err := control.check(); err != nil {
		t.Fatalf("first check error = %v", err)
	}
	if err := control.check(); err != nil {
		t.Fatalf("second check error = %v", err)
	}
	err = control.check()
	if !errors.Is(err, ErrSQLExecutionStepsExceeded) {
		t.Fatalf("third check error = %v, want ErrSQLExecutionStepsExceeded", err)
	}
}

func TestCH030ExecutionStepsBoundQuery(t *testing.T) {
	query := "FROM VALUES ('a'), ('b') AS events(kind) SELECT kind"
	if _, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxExecutionSteps: 1}); !errors.Is(err, ErrSQLExecutionStepsExceeded) {
		t.Fatalf("bounded query error = %v, want ErrSQLExecutionStepsExceeded", err)
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxExecutionSteps: 10000})
	if err != nil {
		t.Fatalf("generous execution budget error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("generous execution budget rows = %d, want 2", len(result.Rows))
	}
}

func TestCH030ExecutionStepsRejectsNegativeBudget(t *testing.T) {
	if _, _, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxExecutionSteps: -1}); err == nil {
		t.Fatal("negative MaxExecutionSteps was accepted")
	}
}

func TestCH030ExecutionStepsDisablesResultCache(t *testing.T) {
	if sqlResultCacheOptionsEligible(SQLQueryOptions{MaxExecutionSteps: 1}) {
		t.Fatal("bounded execution query remained eligible for result caching")
	}
}
