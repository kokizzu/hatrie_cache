package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestErrorCodeClassifiesDiagnosticsAndWrappedErrors(t *testing.T) {
	if got := ErrorCodeOf(sqlTokenDiagnostic(Token{line: 1, column: 1, endColumn: 2}, "bad syntax")); got != ErrorSyntax {
		t.Fatalf("diagnostic code = %q, want %q", got, ErrorSyntax)
	}
	wrapped := WithErrorCode(ErrorCapacity, errors.New("row limit exceeded"))
	if got := ErrorCodeOf(wrapped); got != ErrorCapacity {
		t.Fatalf("wrapped code = %q, want %q", got, ErrorCapacity)
	}
}

func TestPublicQueryAPIsClassifyRuntimeErrors(t *testing.T) {
	typeQuery := "FROM VALUES ('not-a-number') AS values(raw) SELECT CAST(raw AS NUMBER)"
	_, err := ExecuteSQLQueryContext(context.Background(), typeQuery, nil, SQLQueryOptions{})
	if got := ErrorCodeOf(err); got != ErrorType {
		t.Fatalf("materialized type error code = %q, want %q (%v)", got, ErrorType, err)
	}

	capacityQuery := "FROM VALUES (1), (2) AS values(id) SELECT id"
	err = ExecuteSQLQueryRows(context.Background(), capacityQuery, nil, nil, SQLQueryOptions{MaxRows: 1}, func([]string, SQLRow) error { return nil })
	if got := ErrorCodeOf(err); got != ErrorCapacity {
		t.Fatalf("streaming capacity error code = %q, want %q (%v)", got, ErrorCapacity, err)
	}

	prepared, err := PrepareSQLQuery("FROM VALUES ($1) AS values(id) SELECT id", []ParameterSpec{{Type: ParameterInteger}}, nil)
	if err != nil {
		t.Fatalf("PrepareSQLQuery() error = %v", err)
	}
	_, err = prepared.Execute(context.Background(), nil, []interface{}{"not-an-integer"}, SQLQueryOptions{})
	if got := ErrorCodeOf(err); got != ErrorType {
		t.Fatalf("prepared parameter type error code = %q, want %q (%v)", got, ErrorType, err)
	}

	if got := ErrorCodeOf(sqlClassifyError(errors.New("optimistic write conflict"))); got != ErrorConflict {
		t.Fatalf("conflict error code = %q, want %q", got, ErrorConflict)
	}
}
