package hatSql_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestLexPreservesValuesAndSourceSpans(t *testing.T) {
	tokens, err := hatSql.Lex("SELECT 'O''Brien', $12\nFROM cache")
	if err != nil {
		t.Fatalf("Lex() error = %v", err)
	}

	want := []struct {
		kind      hatSql.TokenKind
		text      string
		line, col int
		endColumn int
	}{
		{hatSql.TokenIdentifier, "SELECT", 1, 1, 7},
		{hatSql.TokenString, "O'Brien", 1, 8, 18},
		{hatSql.TokenComma, ",", 1, 18, 19},
		{hatSql.TokenParameter, "12", 1, 20, 23},
		{hatSql.TokenIdentifier, "FROM", 2, 1, 5},
		{hatSql.TokenIdentifier, "cache", 2, 6, 11},
		{hatSql.TokenEOF, "", 2, 11, 11},
	}
	if len(tokens) != len(want) {
		t.Fatalf("Lex() token count = %d, want %d", len(tokens), len(want))
	}
	for index, expected := range want {
		got := tokens[index]
		if got.Kind() != expected.kind || got.Text() != expected.text || got.Line() != expected.line || got.Column() != expected.col || got.EndColumn() != expected.endColumn {
			t.Errorf("token %d = %#v, want kind=%v text=%q line=%d col=%d end=%d", index, got, expected.kind, expected.text, expected.line, expected.col, expected.endColumn)
		}
	}
}

func TestLexReturnsDiagnosticForInvalidParameter(t *testing.T) {
	_, err := hatSql.Lex("SELECT $x")
	var diagnostic *hatSql.Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("Lex() error = %T %v, want *Diagnostic", err, err)
	}
	if diagnostic.Line != 1 || diagnostic.Column != 8 || diagnostic.Suggestion != "" {
		t.Fatalf("diagnostic = %#v", diagnostic)
	}
}
