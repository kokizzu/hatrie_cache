package hatSql

import (
	"errors"
	"strings"
)

// Position is a zero-based source position used by editor and language-server
// integrations.
type Position struct {
	Line      int `json:"line"`
	Character int `json:"character"`
}

// Range is a half-open source span used by editor and language-server integrations.
type Range struct {
	Start Position `json:"start"`
	End   Position `json:"end"`
}

// DiagnosticSeverity classifies a SQL diagnostic for editor clients.
type DiagnosticSeverity string

const (
	DiagnosticSeverityError   DiagnosticSeverity = "error"
	DiagnosticSeverityWarning DiagnosticSeverity = "warning"
)

// InlineDiagnostic is a machine-readable diagnostic with an exact source range.
type InlineDiagnostic struct {
	Range      Range              `json:"range"`
	Severity   DiagnosticSeverity `json:"severity"`
	Code       ErrorCode          `json:"code,omitempty"`
	Message    string             `json:"message"`
	Suggestion string             `json:"suggestion,omitempty"`
}

// CompletionItem is one SQL completion candidate.
type CompletionItem struct {
	Label  string `json:"label"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

// LanguageServer exposes parser-backed editor capabilities without requiring a
// transport-specific LSP implementation.
type LanguageServer struct{}

// NewLanguageServer creates a stateless SQL language-server facade.
func NewLanguageServer() *LanguageServer { return &LanguageServer{} }

// FormatSQL validates the statement with the SQL query parser, then renders a
// canonical lexical representation. Formatting never attempts to repair an
// invalid statement, so execution and editor tooling share one grammar.
func FormatSQL(source string) (string, error) {
	if err := validateSQLToolingSource(source); err != nil {
		return "", err
	}
	tokens, err := Lex(source)
	if err != nil {
		return "", err
	}
	var formatted strings.Builder
	previous := TokenEOF
	for _, token := range tokens {
		if token.Kind() == TokenEOF {
			break
		}
		writeFormattedToken(&formatted, previous, token)
		previous = token.Kind()
	}
	return strings.TrimSpace(formatted.String()), nil
}

func writeFormattedToken(builder *strings.Builder, previous TokenKind, token Token) {
	kind := token.Kind()
	switch kind {
	case TokenComma:
		trimFormattedSpace(builder)
		builder.WriteByte(',')
		builder.WriteByte(' ')
	case TokenSemicolon:
		trimFormattedSpace(builder)
		builder.WriteByte(';')
	case TokenLeftParen:
		if previous != TokenEOF && previous != TokenIdentifier && previous != TokenRightParen {
			writeFormattedSpace(builder)
		}
		builder.WriteByte('(')
	case TokenRightParen:
		trimFormattedSpace(builder)
		builder.WriteByte(')')
	case TokenDot:
		trimFormattedSpace(builder)
		builder.WriteByte('.')
	case TokenEqual, TokenArrow, TokenPlus, TokenMinus, TokenSlash, TokenPercent, TokenLess, TokenLessEqual, TokenGreater, TokenGreaterEqual, TokenNotEqual:
		writeFormattedSpace(builder)
		builder.WriteString(token.Text())
		builder.WriteByte(' ')
	default:
		if previous != TokenEOF && previous != TokenLeftParen && previous != TokenDot && previous != TokenComma && previous != TokenBang {
			writeFormattedSpace(builder)
		}
		builder.WriteString(formatSQLToken(token))
	}
}

func trimFormattedSpace(builder *strings.Builder) {
	value := builder.String()
	if strings.HasSuffix(value, " ") {
		builder.Reset()
		builder.WriteString(strings.TrimRight(value, " "))
	}
}

func writeFormattedSpace(builder *strings.Builder) {
	if builder.Len() == 0 {
		return
	}
	value := builder.String()
	if !strings.HasSuffix(value, " ") {
		builder.WriteByte(' ')
	}
}

func formatSQLToken(token Token) string {
	if token.Kind() == TokenString {
		return "'" + strings.ReplaceAll(token.Text(), "'", "''") + "'"
	}
	if token.Kind() == TokenIdentifier && sqlToolingKeyword(token.Text()) {
		return strings.ToUpper(token.Text())
	}
	return token.Text()
}

func sqlToolingKeyword(value string) bool {
	switch strings.ToUpper(value) {
	case "SELECT", "FROM", "WHERE", "AS", "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE", "BETWEEN", "GROUP", "BY", "HAVING", "ORDER", "ASC", "DESC", "NULLS", "FIRST", "LAST", "LIMIT", "OFFSET", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "ON", "WITH", "RECURSIVE", "UNION", "INTERSECT", "EXCEPT", "ALL", "DISTINCT", "EXPLAIN", "ANALYZE", "CACHE", "KEYS", "VALUES", "TABLE", "EXTERNAL", "SAMPLE":
		return true
	default:
		return false
	}
}

// LintSQL returns syntax diagnostics from the shared parser and conservative
// style warnings that are safe to evaluate from parser tokens alone.
func LintSQL(source string) []InlineDiagnostic {
	if err := validateSQLToolingSource(source); err != nil {
		return []InlineDiagnostic{inlineDiagnosticFromError(err)}
	}
	tokens, err := Lex(source)
	if err != nil {
		return []InlineDiagnostic{inlineDiagnosticFromError(err)}
	}
	for index := 0; index+1 < len(tokens); index++ {
		if tokens[index].Kind() == TokenIdentifier && strings.EqualFold(tokens[index].Text(), "SELECT") && tokens[index+1].Kind() == TokenStar {
			star := tokens[index+1]
			return []InlineDiagnostic{{
				Range:    tokenRange(star),
				Severity: DiagnosticSeverityWarning,
				Code:     ErrorCode("STYLE_SELECT_STAR"),
				Message:  "SELECT * can make result schemas unstable; list required columns explicitly",
			}}
		}
	}
	return nil
}

func validateSQLToolingSource(source string) error {
	statement := strings.TrimSpace(source)
	if strings.HasSuffix(statement, ";") {
		statement = strings.TrimSpace(strings.TrimSuffix(statement, ";"))
	}
	return ValidateSQLQuery(statement)
}

func inlineDiagnosticFromError(err error) InlineDiagnostic {
	var diagnostic *Diagnostic
	if errors.As(err, &diagnostic) && diagnostic != nil {
		line := diagnostic.Line - 1
		column := diagnostic.Column - 1
		end := diagnostic.EndColumn - 1
		if line < 0 {
			line = 0
		}
		if column < 0 {
			column = 0
		}
		if end <= column {
			end = column + 1
		}
		return InlineDiagnostic{
			Range:      Range{Start: Position{Line: line, Character: column}, End: Position{Line: line, Character: end}},
			Severity:   DiagnosticSeverityError,
			Code:       diagnostic.Code,
			Message:    diagnostic.Message,
			Suggestion: diagnostic.Suggestion,
		}
	}
	return InlineDiagnostic{Severity: DiagnosticSeverityError, Message: err.Error()}
}

func tokenRange(token Token) Range {
	line := token.Line() - 1
	start := token.Column() - 1
	end := token.EndColumn() - 1
	if line < 0 {
		line = 0
	}
	if start < 0 {
		start = 0
	}
	if end <= start {
		end = start + 1
	}
	return Range{Start: Position{Line: line, Character: start}, End: Position{Line: line, Character: end}}
}

// Completion returns context-sensitive SQL keyword suggestions. The shared
// lexer is used whenever the document is lexically complete; incomplete input
// deliberately falls back to prefix analysis so editors can complete while a
// user is still typing.
func (server *LanguageServer) Completion(source string, position Position) []CompletionItem {
	_ = server
	prefix := sqlPrefixAtPosition(source, position)
	upper := strings.ToUpper(strings.TrimSpace(prefix))
	keywords := sqlCompletionKeywords(upper)
	items := make([]CompletionItem, 0, len(keywords))
	for _, keyword := range keywords {
		items = append(items, CompletionItem{Label: keyword, Kind: "keyword", Detail: "SQL keyword"})
	}
	return items
}

// Diagnostics returns parser-backed diagnostics suitable for publishing as LSP
// textDocument diagnostics.
func (server *LanguageServer) Diagnostics(source string) []InlineDiagnostic {
	_ = server
	return LintSQL(source)
}

func sqlPrefixAtPosition(source string, position Position) string {
	if position.Line < 0 || position.Character < 0 {
		return ""
	}
	lines := strings.Split(source, "\n")
	if position.Line >= len(lines) {
		return source
	}
	var builder strings.Builder
	for index := 0; index < position.Line; index++ {
		builder.WriteString(lines[index])
		builder.WriteByte('\n')
	}
	line := lines[position.Line]
	if position.Character > len(line) {
		position.Character = len(line)
	}
	builder.WriteString(line[:position.Character])
	return builder.String()
}

func sqlCompletionKeywords(prefix string) []string {
	switch {
	case strings.HasPrefix(prefix, "SELECT") && !strings.Contains(prefix, " FROM "):
		return []string{"FROM", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX"}
	case strings.HasSuffix(prefix, "FROM") || strings.Contains(prefix, " FROM ") && !strings.Contains(prefix, " WHERE "):
		return []string{"CACHE", "KEYS", "VALUES", "TABLE", "EXTERNAL", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "LIMIT"}
	case strings.Contains(prefix, " WHERE "):
		return []string{"AND", "OR", "ORDER BY", "GROUP BY", "LIMIT", "OFFSET"}
	default:
		return []string{"SELECT", "WITH", "EXPLAIN", "VALUES"}
	}
}
