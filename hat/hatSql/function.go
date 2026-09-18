package hatSql

import (
	"errors"
	"fmt"
	"strings"
)

// FunctionMonotonicity describes how a function's result changes as its
// declared inputs increase. Unknown is the conservative zero value.
type FunctionMonotonicity string

const (
	FunctionMonotonicityUnknown       FunctionMonotonicity = ""
	FunctionMonotonicityConstant      FunctionMonotonicity = "constant"
	FunctionMonotonicityNonDecreasing FunctionMonotonicity = "non_decreasing"
	FunctionMonotonicityNonIncreasing FunctionMonotonicity = "non_increasing"
)

// FunctionDefinition describes a named scalar function available to read-only
// SQL queries. Source is interpreted by the runtime selected by Language.
type FunctionDefinition struct {
	Name          string   `json:"name"`
	Arguments     []string `json:"arguments"`
	ArgumentTypes []string `json:"argument_types"`
	Language      string   `json:"language"`
	Source        string   `json:"source"`
	// Deterministic is an explicit caller declaration that equal inputs always
	// produce equal outputs. The false zero value means unknown.
	Deterministic bool `json:"deterministic,omitempty"`
	// Monotonicity is an optional proof hint for planner-owned incremental
	// maintenance. It is never inferred from the function language.
	Monotonicity FunctionMonotonicity `json:"monotonicity,omitempty"`
	// Retractable declares that the caller has an inverse/update contract for
	// the function's use in a differential plan.
	Retractable bool `json:"retractable,omitempty"`
}

// NormalizeFunctionCapabilities validates and normalizes optional function
// capability metadata. Unknown metadata is the safe default; stronger claims
// require an explicit deterministic declaration.
func NormalizeFunctionCapabilities(definition *FunctionDefinition) error {
	if definition == nil {
		return fmt.Errorf("SQL function definition is required")
	}
	monotonicity := strings.ToLower(strings.TrimSpace(string(definition.Monotonicity)))
	monotonicity = strings.ReplaceAll(monotonicity, "-", "_")
	switch monotonicity {
	case "", "unknown":
		definition.Monotonicity = FunctionMonotonicityUnknown
	case string(FunctionMonotonicityConstant):
		definition.Monotonicity = FunctionMonotonicityConstant
	case string(FunctionMonotonicityNonDecreasing):
		definition.Monotonicity = FunctionMonotonicityNonDecreasing
	case string(FunctionMonotonicityNonIncreasing):
		definition.Monotonicity = FunctionMonotonicityNonIncreasing
	default:
		return fmt.Errorf("SQL function %q has unsupported monotonicity %q", definition.Name, definition.Monotonicity)
	}
	if definition.Monotonicity != FunctionMonotonicityUnknown && !definition.Deterministic {
		return fmt.Errorf("SQL function %q must be deterministic before declaring monotonicity", definition.Name)
	}
	if definition.Retractable && !definition.Deterministic {
		return fmt.Errorf("SQL function %q must be deterministic before declaring retraction", definition.Name)
	}
	return nil
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
