package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

// FunctionDefinition describes a named scalar function available to read-only
// SQL queries. Source is interpreted by the runtime selected by Language.
type FunctionDefinition struct {
	Name          string   `json:"name"`
	Arguments     []string `json:"arguments"`
	ArgumentTypes []string `json:"argument_types"`
	Language      string   `json:"language"`
	Source        string   `json:"source"`
}

// FunctionCall contains one function invocation's positional values.
type FunctionCall struct{ Arguments []interface{} }

// FunctionError preserves the source location of a function compilation or
// execution failure.
type FunctionError struct {
	Definition   FunctionDefinition
	Message      string
	Line, Column int
}

func (err *FunctionError) Error() string {
	return "SQL function " + err.Definition.Name + ": " + err.Message
}

// FormatFunctionDiagnostic renders a FunctionError with its source location.
// Other errors are returned unchanged.
func FormatFunctionDiagnostic(definition FunctionDefinition, err error) string {
	var functionError *FunctionError
	if !errors.As(err, &functionError) || functionError == nil {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	if functionError.Definition.Name != "" {
		definition = functionError.Definition
	}
	line, column := functionError.Line, functionError.Column
	if line < 1 {
		line = 1
	}
	if column < 1 {
		column = 1
	}
	sourceLines := strings.Split(definition.Source, "\n")
	if line > len(sourceLines) {
		line = 1
	}
	source := sourceLines[line-1]
	return fmt.Sprintf("error: %s\n --> function %s:%d:%d\n  |\n%d | %s\n  | %s^", functionError.Message, definition.Name, line, column, line, source, strings.Repeat(" ", column-1))
}

// FunctionResolver supplies vectorized custom SQL function results.
type FunctionResolver interface {
	EvaluateSQLFunction(name string, calls []FunctionCall) ([]interface{}, error)
}
