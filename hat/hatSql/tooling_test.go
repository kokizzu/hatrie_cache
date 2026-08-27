package hatSql

import "testing"

func TestFormatSQLUsesSharedParser(t *testing.T) {
	got, err := FormatSQL(" select name , age from cache('users') where age>=18 ; ")
	if err != nil {
		t.Fatalf("FormatSQL() error = %v", err)
	}
	const want = "SELECT name, age FROM CACHE('users') WHERE age >= 18;"
	if got != want {
		t.Fatalf("FormatSQL() = %q, want %q", got, want)
	}
	if _, err := FormatSQL("SELECT FROM"); err == nil {
		t.Fatal("FormatSQL(invalid) error = nil, want parser diagnostic")
	}
}

func TestLintSQLReturnsParserDiagnosticRange(t *testing.T) {
	diagnostics := LintSQL("SELECT FROM")
	if len(diagnostics) != 1 {
		t.Fatalf("LintSQL() diagnostics = %#v, want one parser diagnostic", diagnostics)
	}
	diagnostic := diagnostics[0]
	if diagnostic.Range.Start.Line != 0 || diagnostic.Range.Start.Character < 7 || diagnostic.Range.End.Character <= diagnostic.Range.Start.Character {
		t.Fatalf("LintSQL() diagnostic range = %#v, want SELECT FROM token range", diagnostic.Range)
	}
}

func TestLanguageServerCompletionAndDiagnostics(t *testing.T) {
	server := NewLanguageServer()
	items := server.Completion("SELECT name ", Position{Line: 0, Character: 12})
	if !completionContains(items, "FROM") {
		t.Fatalf("Completion() = %#v, want FROM", items)
	}
	diagnostics := server.Diagnostics("SELECT FROM")
	if len(diagnostics) != 1 || diagnostics[0].Range.Start.Line != 0 {
		t.Fatalf("Diagnostics() = %#v, want one inline parser diagnostic", diagnostics)
	}
}

func completionContains(items []CompletionItem, label string) bool {
	for _, item := range items {
		if item.Label == label {
			return true
		}
	}
	return false
}
