package hatSql

import (
	"fmt"
	"strings"
)

// Diagnostic describes a local SQL parse or compilation error. Positions are
// one-based and point into the original query.
type Diagnostic struct {
	Code       ErrorCode
	Message    string
	Line       int
	Column     int
	EndColumn  int
	Suggestion string
}

func (diagnostic *Diagnostic) Error() string {
	if diagnostic == nil {
		return ""
	}
	if diagnostic.Suggestion == "" {
		return diagnostic.Message
	}
	return diagnostic.Message + "; did you mean `" + diagnostic.Suggestion + "`?"
}

// FormatDiagnostic formats a Diagnostic with a Rust-style source span.
// Non-SQL errors are returned unchanged.
func FormatDiagnostic(source string, err error) string {
	diagnostic, ok := err.(*Diagnostic)
	if !ok || diagnostic == nil {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	line := diagnostic.Line
	if line < 1 {
		line = 1
	}
	column := diagnostic.Column
	if column < 1 {
		column = 1
	}
	lines := strings.Split(source, "\n")
	text := ""
	if line <= len(lines) {
		text = lines[line-1]
	}
	width := diagnostic.EndColumn - column
	if width < 1 {
		width = 1
	}
	return fmt.Sprintf("error: %s\n --> query:%d:%d\n  |\n%d | %s\n  | %s%s",
		diagnostic.Error(), line, column, line, text,
		strings.Repeat(" ", column-1), strings.Repeat("^", width))
}
