package hatSql_test

import (
	"errors"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestFormatFunctionDiagnostic(t *testing.T) {
	definition := hatSql.FunctionDefinition{
		Name:   "eligible",
		Source: "return age > 10",
	}
	err := &hatSql.FunctionError{
		Definition: definition,
		Message:    "unknown argument \"agge\"",
		Line:       1,
		Column:     8,
	}

	got := hatSql.FormatFunctionDiagnostic(hatSql.FunctionDefinition{}, err)
	for _, want := range []string{
		"error: unknown argument \"agge\"",
		"--> function eligible:1:8",
		"1 | return age > 10",
		"       ^",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("FormatFunctionDiagnostic() = %q, want %q", got, want)
		}
	}

	if got := hatSql.FormatFunctionDiagnostic(definition, errors.New("plain failure")); got != "plain failure" {
		t.Fatalf("FormatFunctionDiagnostic(non-function error) = %q", got)
	}
}

func TestFunctionResolverCanBeImplementedOutsidePackage(t *testing.T) {
	var resolver hatSql.FunctionResolver = testFunctionResolver{}
	values, err := resolver.EvaluateSQLFunction("double", []hatSql.FunctionCall{{Arguments: []interface{}{int64(21)}}})
	if err != nil {
		t.Fatalf("EvaluateSQLFunction() error = %v", err)
	}
	if len(values) != 1 || values[0] != int64(42) {
		t.Fatalf("EvaluateSQLFunction() = %#v, want [42]", values)
	}
}

type testFunctionResolver struct{}

func (testFunctionResolver) EvaluateSQLFunction(_ string, calls []hatSql.FunctionCall) ([]interface{}, error) {
	return []interface{}{calls[0].Arguments[0].(int64) * 2}, nil
}
