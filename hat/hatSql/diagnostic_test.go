package hatSql_test

import (
	"errors"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestDiagnosticFormatsSourceSpanAndSuggestion(t *testing.T) {
	err := &hatSql.Diagnostic{Message: "unexpected keyword", Line: 2, Column: 3, EndColumn: 7, Suggestion: "SELECT"}
	formatted := hatSql.FormatDiagnostic("FROM x\n  SEL", err)
	for _, want := range []string{"did you mean `SELECT`", "--> query:2:3", "2 |   SEL", "^^^^"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("FormatDiagnostic() = %q, missing %q", formatted, want)
		}
	}
	if got := hatSql.FormatDiagnostic("", errors.New("other")); got != "other" {
		t.Fatalf("FormatDiagnostic(non diagnostic) = %q", got)
	}
}
