package hatSql

import (
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
