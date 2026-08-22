package hatriecache

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	json "github.com/goccy/go-json"
)

// SQLDiagnostic describes a local SQL parse or compilation error. Positions are
// one-based and point into the original query.
type SQLDiagnostic struct {
	Message    string
	Line       int
	Column     int
	EndColumn  int
	Suggestion string
}

func (diagnostic *SQLDiagnostic) Error() string {
	if diagnostic == nil {
		return ""
	}
	if diagnostic.Suggestion == "" {
		return diagnostic.Message
	}
	return diagnostic.Message + "; did you mean `" + diagnostic.Suggestion + "`?"
}

// FormatSQLDiagnostic formats a SQLDiagnostic with a Rust-style source span.
// Non-SQL errors are returned unchanged.
func FormatSQLDiagnostic(source string, err error) string {
	diagnostic, ok := err.(*SQLDiagnostic)
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

// CompileSQL translates the SQL-like CLI language into the existing command
// request protocol. Multiple statements compile to the existing public BATCH
// command in source order.
func CompileSQL(source string) (CacheCommandRequest, error) {
	tokens, err := lexSQL(source)
	if err != nil {
		return CacheCommandRequest{}, err
	}
	parser := sqlParser{source: source, tokens: tokens}
	return parser.parseProgram()
}

type sqlTokenKind uint8

const (
	sqlTokenEOF sqlTokenKind = iota
	sqlTokenIdentifier
	sqlTokenString
	sqlTokenNumber
	sqlTokenComma
	sqlTokenSemicolon
	sqlTokenLeftParen
	sqlTokenRightParen
	sqlTokenEqual
	sqlTokenArrow
	sqlTokenPlus
	sqlTokenMinus
	sqlTokenSlash
	sqlTokenPercent
	sqlTokenBang
	sqlTokenDot
	sqlTokenStar
	sqlTokenLess
	sqlTokenLessEqual
	sqlTokenGreater
	sqlTokenGreaterEqual
	sqlTokenNotEqual
)

type sqlToken struct {
	kind      sqlTokenKind
	text      string
	line      int
	column    int
	endColumn int
}

func (token sqlToken) display() string {
	if token.kind == sqlTokenEOF {
		return "end of input"
	}
	return strconv.Quote(token.text)
}

type sqlLexer struct {
	source string
	offset int
	line   int
	column int
}

func lexSQL(source string) ([]sqlToken, error) {
	lexer := sqlLexer{source: source, line: 1, column: 1}
	tokens := make([]sqlToken, 0, len(source)/4+1)
	for {
		lexer.skipWhitespace()
		if lexer.offset >= len(lexer.source) {
			tokens = append(tokens, lexer.token(sqlTokenEOF, "", lexer.line, lexer.column))
			return tokens, nil
		}
		startLine, startColumn := lexer.line, lexer.column
		ch := lexer.source[lexer.offset]
		switch {
		case isSQLIdentifierStart(ch):
			start := lexer.offset
			for lexer.offset < len(lexer.source) && isSQLIdentifierPart(lexer.source[lexer.offset]) {
				lexer.advanceRune()
			}
			tokens = append(tokens, lexer.token(sqlTokenIdentifier, lexer.source[start:lexer.offset], startLine, startColumn))
		case ch == '\'':
			value, err := lexer.readString(startLine, startColumn)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, lexer.token(sqlTokenString, value, startLine, startColumn))
		case isSQLDigit(ch) || ch == '-' && lexer.offset+1 < len(lexer.source) && isSQLDigit(lexer.source[lexer.offset+1]):
			value, ok := lexer.readNumber()
			if !ok {
				return nil, lexer.diagnostic(startLine, startColumn, startColumn+1, "expected a number")
			}
			tokens = append(tokens, lexer.token(sqlTokenNumber, value, startLine, startColumn))
		case ch == ',':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenComma, ",", startLine, startColumn))
		case ch == ';':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenSemicolon, ";", startLine, startColumn))
		case ch == '(':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenLeftParen, "(", startLine, startColumn))
		case ch == ')':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenRightParen, ")", startLine, startColumn))
		case ch == '+':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenPlus, "+", startLine, startColumn))
		case ch == '-':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenMinus, "-", startLine, startColumn))
		case ch == '/':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenSlash, "/", startLine, startColumn))
		case ch == '%':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenPercent, "%", startLine, startColumn))
		case ch == '.':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenDot, ".", startLine, startColumn))
		case ch == '*':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(sqlTokenStar, "*", startLine, startColumn))
		case ch == '<':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(sqlTokenLessEqual, "<=", startLine, startColumn))
			} else if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '>' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(sqlTokenNotEqual, "<>", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(sqlTokenLess, "<", startLine, startColumn))
			}
		case ch == '>':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(sqlTokenGreaterEqual, ">=", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(sqlTokenGreater, ">", startLine, startColumn))
			}
		case ch == '!':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(sqlTokenNotEqual, "!=", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(sqlTokenBang, "!", startLine, startColumn))
			}
		case ch == '=':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '>' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(sqlTokenArrow, "=>", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(sqlTokenEqual, "=", startLine, startColumn))
			}
		default:
			return nil, lexer.diagnostic(startLine, startColumn, startColumn+1, fmt.Sprintf("unexpected character %q", ch))
		}
	}
}

func (lexer *sqlLexer) token(kind sqlTokenKind, text string, line int, column int) sqlToken {
	return sqlToken{kind: kind, text: text, line: line, column: column, endColumn: lexer.column}
}

func (lexer *sqlLexer) diagnostic(line int, column int, endColumn int, message string) error {
	return &SQLDiagnostic{Message: message, Line: line, Column: column, EndColumn: endColumn}
}

func (lexer *sqlLexer) skipWhitespace() {
	for lexer.offset < len(lexer.source) {
		switch lexer.source[lexer.offset] {
		case ' ', '\t', '\r', '\n':
			lexer.advanceRune()
		default:
			return
		}
	}
}

func (lexer *sqlLexer) advanceRune() {
	if lexer.offset >= len(lexer.source) {
		return
	}
	runeValue, size := utf8.DecodeRuneInString(lexer.source[lexer.offset:])
	if runeValue == utf8.RuneError && size == 1 {
		size = 1
	}
	lexer.offset += size
	if runeValue == '\n' {
		lexer.line++
		lexer.column = 1
		return
	}
	lexer.column++
}

func (lexer *sqlLexer) readString(line int, column int) (string, error) {
	lexer.advanceRune() // opening quote
	var value strings.Builder
	for lexer.offset < len(lexer.source) {
		if lexer.source[lexer.offset] != '\'' {
			runeValue, size := utf8.DecodeRuneInString(lexer.source[lexer.offset:])
			value.WriteString(lexer.source[lexer.offset : lexer.offset+size])
			lexer.offset += size
			if runeValue == '\n' {
				lexer.line++
				lexer.column = 1
			} else {
				lexer.column++
			}
			continue
		}
		lexer.advanceRune()
		if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '\'' {
			value.WriteByte('\'')
			lexer.advanceRune()
			continue
		}
		return value.String(), nil
	}
	return "", lexer.diagnostic(line, column, lexer.column, "unterminated string literal")
}

func (lexer *sqlLexer) readNumber() (string, bool) {
	start := lexer.offset
	if lexer.source[lexer.offset] == '-' {
		lexer.advanceRune()
	}
	digits := 0
	for lexer.offset < len(lexer.source) && isSQLDigit(lexer.source[lexer.offset]) {
		digits++
		lexer.advanceRune()
	}
	if digits == 0 {
		return "", false
	}
	if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '.' {
		lexer.advanceRune()
		fractionDigits := 0
		for lexer.offset < len(lexer.source) && isSQLDigit(lexer.source[lexer.offset]) {
			fractionDigits++
			lexer.advanceRune()
		}
		if fractionDigits == 0 {
			return "", false
		}
	}
	if lexer.offset < len(lexer.source) && (lexer.source[lexer.offset] == 'e' || lexer.source[lexer.offset] == 'E') {
		lexer.advanceRune()
		if lexer.offset < len(lexer.source) && (lexer.source[lexer.offset] == '+' || lexer.source[lexer.offset] == '-') {
			lexer.advanceRune()
		}
		exponentDigits := 0
		for lexer.offset < len(lexer.source) && isSQLDigit(lexer.source[lexer.offset]) {
			exponentDigits++
			lexer.advanceRune()
		}
		if exponentDigits == 0 {
			return "", false
		}
	}
	return lexer.source[start:lexer.offset], true
}

func isSQLIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isSQLIdentifierPart(value byte) bool {
	return isSQLIdentifierStart(value) || isSQLDigit(value)
}

func isSQLDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

type sqlParser struct {
	source string
	tokens []sqlToken
	index  int
}

func (parser *sqlParser) parseProgram() (CacheCommandRequest, error) {
	if parser.current().kind == sqlTokenEOF {
		return CacheCommandRequest{}, parser.expected(parser.current(), "a SQL statement", nil)
	}
	requests := make([]CacheCommandRequest, 0, 1)
	for parser.current().kind != sqlTokenEOF {
		request, err := parser.parseStatement()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		requests = append(requests, request)
		if parser.current().kind == sqlTokenSemicolon {
			parser.next()
			continue
		}
		if parser.current().kind != sqlTokenEOF {
			return CacheCommandRequest{}, parser.expected(parser.current(), "a semicolon or end of input", nil)
		}
	}
	if len(requests) == 1 {
		return requests[0], nil
	}
	return CacheCommandRequest{Command: "BATCH", Batch: requests}, nil
}

func (parser *sqlParser) parseStatement() (CacheCommandRequest, error) {
	token := parser.current()
	if token.kind != sqlTokenIdentifier {
		return CacheCommandRequest{}, parser.expected(token, "SELECT, INSERT, UPDATE, DELETE, or CALL", []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CALL"})
	}
	switch strings.ToUpper(token.text) {
	case "SELECT":
		parser.next()
		return parser.parseSelect()
	case "INSERT":
		parser.next()
		return parser.parseInsert()
	case "UPDATE":
		parser.next()
		return parser.parseUpdate()
	case "DELETE":
		parser.next()
		return parser.parseDelete()
	case "CALL":
		parser.next()
		return parser.parseCall()
	default:
		return CacheCommandRequest{}, parser.expected(token, "SELECT, INSERT, UPDATE, DELETE, or CALL", []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CALL"})
	}
}

func (parser *sqlParser) parseSelect() (CacheCommandRequest, error) {
	selector, err := parser.expectIdentifier("value, exists, ttl, or dump", []string{"value", "exists", "ttl", "dump"})
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("FROM"); err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("cache"); err != nil {
		return CacheCommandRequest{}, err
	}
	key, err := parser.parseKeyPredicate()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	command := ""
	switch strings.ToUpper(selector.text) {
	case "VALUE":
		command = "GETSTR"
	case "EXISTS":
		command = "EXISTS"
	case "TTL":
		command = "TTL"
	case "DUMP":
		command = "DUMP"
	default:
		return CacheCommandRequest{}, parser.expected(selector, "value, exists, ttl, or dump", []string{"value", "exists", "ttl", "dump"})
	}
	return CacheCommandRequest{Command: command, Key: key.text}, nil
}

func (parser *sqlParser) parseInsert() (CacheCommandRequest, error) {
	if err := parser.expectKeyword("INTO"); err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("cache"); err != nil {
		return CacheCommandRequest{}, err
	}
	columns, err := parser.parseIdentifierList()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("VALUES"); err != nil {
		return CacheCommandRequest{}, err
	}
	values, err := parser.parseScalarList()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if len(columns) != len(values) {
		return CacheCommandRequest{}, parser.diagnostic(parser.previous(), "VALUES count does not match column count")
	}
	fields := make(map[string]sqlValue, len(columns))
	for index, column := range columns {
		name := strings.ToLower(column.text)
		if _, exists := fields[name]; exists {
			return CacheCommandRequest{}, parser.diagnostic(column, "duplicate INSERT column "+strconv.Quote(column.text))
		}
		fields[name] = values[index]
	}
	return compileSQLInsert(fields)
}

func compileSQLInsert(fields map[string]sqlValue) (CacheCommandRequest, error) {
	key, ok := fields["key"]
	if !ok {
		return CacheCommandRequest{}, sqlFieldDiagnostic(fields, "key", "INSERT requires a key column")
	}
	if key.json {
		return CacheCommandRequest{}, sqlValueDiagnostic(key, "key must be a scalar")
	}
	value, hasValue := fields["value"]
	counter, hasCounter := fields["counter"]
	if hasValue == hasCounter {
		return CacheCommandRequest{}, sqlFieldDiagnostic(fields, "value", "INSERT requires exactly one of value or counter")
	}
	for name := range fields {
		switch name {
		case "key", "value", "counter", "ttl_seconds", "unix_seconds":
		default:
			return CacheCommandRequest{}, sqlFieldDiagnostic(fields, name, "unknown INSERT column "+strconv.Quote(name))
		}
	}
	request := CacheCommandRequest{Key: key.text}
	if hasCounter {
		parsed, err := sqlInt64(counter, "counter")
		if err != nil || parsed < -1<<31 || parsed > 1<<31-1 {
			return CacheCommandRequest{}, sqlValueDiagnostic(counter, "counter must be a 32-bit integer")
		}
		request.Command = "SETINT"
		request.Value = strconv.FormatInt(parsed, 10)
	} else {
		if value.json {
			return CacheCommandRequest{}, sqlValueDiagnostic(value, "value must be a scalar")
		}
		request.Command = "SETSTR"
		request.Value = value.text
	}
	if ttl, exists := fields["ttl_seconds"]; exists {
		parsed, err := sqlPositiveInt64(ttl, "ttl_seconds")
		if err != nil {
			return CacheCommandRequest{}, err
		}
		request.TTLSeconds = int64SQLPointer(parsed)
		if request.Command == "SETSTR" {
			request.Command = "SETSTRX"
		} else {
			request.Command = "SETINTX"
		}
	}
	if unix, exists := fields["unix_seconds"]; exists {
		if request.TTLSeconds != nil {
			return CacheCommandRequest{}, sqlValueDiagnostic(unix, "ttl_seconds and unix_seconds cannot be combined")
		}
		parsed, err := sqlInt64(unix, "unix_seconds")
		if err != nil {
			return CacheCommandRequest{}, err
		}
		request.UnixSeconds = int64SQLPointer(parsed)
	}
	return request, nil
}

func (parser *sqlParser) parseUpdate() (CacheCommandRequest, error) {
	if err := parser.expectKeyword("cache"); err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("SET"); err != nil {
		return CacheCommandRequest{}, err
	}
	field, err := parser.expectIdentifier("value, ttl_seconds, or unix_seconds", []string{"value", "ttl_seconds", "unix_seconds"})
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKind(sqlTokenEqual, "="); err != nil {
		return CacheCommandRequest{}, err
	}
	if strings.EqualFold(field.text, "value") && parser.current().kind == sqlTokenIdentifier && strings.EqualFold(parser.current().text, "value") && parser.peek().kind == sqlTokenPlus {
		parser.next()
		parser.next()
		delta, err := parser.parseScalar()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		parsed, err := sqlInt64(delta, "increment")
		if err != nil || parsed < -1<<31 || parsed > 1<<31-1 {
			return CacheCommandRequest{}, sqlValueDiagnostic(delta, "increment must be a 32-bit integer")
		}
		key, err := parser.parseKeyPredicate()
		if err != nil {
			return CacheCommandRequest{}, err
		}
		return CacheCommandRequest{Command: "INC", Key: key.text, Value: strconv.FormatInt(parsed, 10)}, nil
	}
	value, err := parser.parseScalar()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	key, err := parser.parseKeyPredicate()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	switch strings.ToUpper(field.text) {
	case "VALUE":
		if value.json {
			return CacheCommandRequest{}, sqlValueDiagnostic(value, "value must be a scalar")
		}
		return CacheCommandRequest{Command: "SETSTR", Key: key.text, Value: value.text}, nil
	case "TTL_SECONDS":
		ttl, err := sqlPositiveInt64(value, "ttl_seconds")
		if err != nil {
			return CacheCommandRequest{}, err
		}
		return CacheCommandRequest{Command: "EXPIRE", Key: key.text, TTLSeconds: int64SQLPointer(ttl)}, nil
	case "UNIX_SECONDS":
		unix, err := sqlInt64(value, "unix_seconds")
		if err != nil {
			return CacheCommandRequest{}, err
		}
		return CacheCommandRequest{Command: "EXPIREAT", Key: key.text, UnixSeconds: int64SQLPointer(unix)}, nil
	default:
		return CacheCommandRequest{}, parser.expected(field, "value, ttl_seconds, or unix_seconds", []string{"value", "ttl_seconds", "unix_seconds"})
	}
}

func (parser *sqlParser) parseDelete() (CacheCommandRequest, error) {
	if err := parser.expectKeyword("FROM"); err != nil {
		return CacheCommandRequest{}, err
	}
	if err := parser.expectKeyword("cache"); err != nil {
		return CacheCommandRequest{}, err
	}
	key, err := parser.parseKeyPredicate()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	return CacheCommandRequest{Command: "DEL", Key: key.text}, nil
}

func (parser *sqlParser) parseCall() (CacheCommandRequest, error) {
	commandToken, command, err := parser.parsePublicSQLCommand()
	if err != nil {
		return CacheCommandRequest{}, err
	}
	if isInternalSQLCommand(command) {
		return CacheCommandRequest{}, parser.diagnostic(commandToken, "internal replication command "+strconv.Quote(command)+" is not available through SQL")
	}
	if !isPublicSQLCommand(command) {
		return CacheCommandRequest{}, parser.expected(commandToken, "a public cache command", publicSQLCommandNames())
	}
	if command == "BATCH" {
		return CacheCommandRequest{}, parser.diagnostic(commandToken, "CALL BATCH is not supported; separate SQL statements with semicolons instead")
	}
	if err := parser.expectKind(sqlTokenLeftParen, "("); err != nil {
		return CacheCommandRequest{}, err
	}
	arguments := make([]sqlCallArgument, 0, 2)
	if parser.current().kind != sqlTokenRightParen {
		for {
			argument, err := parser.parseCallArgument()
			if err != nil {
				return CacheCommandRequest{}, err
			}
			arguments = append(arguments, argument)
			if parser.current().kind != sqlTokenComma {
				break
			}
			parser.next()
		}
	}
	if err := parser.expectKind(sqlTokenRightParen, ")"); err != nil {
		return CacheCommandRequest{}, err
	}
	return compileSQLCall(command, arguments)
}

func (parser *sqlParser) parsePublicSQLCommand() (sqlToken, string, error) {
	commandToken, err := parser.expectIdentifier("a public cache command", publicSQLCommandNames())
	if err != nil {
		return sqlToken{}, "", err
	}
	command := strings.ToUpper(commandToken.text)
	if parser.current().kind == sqlTokenDot {
		parser.next()
		suffix, suffixErr := parser.expectIdentifier("a dotted command operation", nil)
		if suffixErr != nil {
			return sqlToken{}, "", suffixErr
		}
		command += "." + strings.ToUpper(suffix.text)
		commandToken.endColumn = suffix.endColumn
	}
	return commandToken, command, nil
}

type sqlCallArgument struct {
	name  *sqlToken
	value sqlValue
}

type sqlValue struct {
	text  string
	json  bool
	token sqlToken
}

func (parser *sqlParser) parseCallArgument() (sqlCallArgument, error) {
	if parser.current().kind == sqlTokenIdentifier && parser.peek().kind == sqlTokenArrow {
		name := parser.current()
		parser.next()
		parser.next()
		value, err := parser.parseLiteral()
		if err != nil {
			return sqlCallArgument{}, err
		}
		return sqlCallArgument{name: &name, value: value}, nil
	}
	value, err := parser.parseLiteral()
	if err != nil {
		return sqlCallArgument{}, err
	}
	return sqlCallArgument{value: value}, nil
}

func compileSQLCall(command string, arguments []sqlCallArgument) (CacheCommandRequest, error) {
	named := false
	for _, argument := range arguments {
		if argument.name != nil {
			named = true
		}
	}
	if named {
		for _, argument := range arguments {
			if argument.name == nil {
				return CacheCommandRequest{}, sqlValueDiagnostic(argument.value, "named CALL arguments cannot be mixed with positional arguments")
			}
		}
		return compileSQLNamedCall(command, arguments)
	}
	return compileSQLPositionalCall(command, arguments)
}

func compileSQLNamedCall(command string, arguments []sqlCallArgument) (CacheCommandRequest, error) {
	request := CacheCommandRequest{Command: command}
	seen := make(map[string]struct{}, len(arguments))
	for _, argument := range arguments {
		name := strings.ToLower(argument.name.text)
		if _, exists := seen[name]; exists {
			return CacheCommandRequest{}, sqlTokenDiagnostic(*argument.name, "duplicate CALL argument "+strconv.Quote(argument.name.text))
		}
		seen[name] = struct{}{}
		value := argument.value
		switch name {
		case "key":
			if value.json {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "key must be a scalar")
			}
			request.Key = value.text
		case "value":
			if value.json {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "value must be a scalar")
			}
			request.Value = value.text
		case "subkey":
			if value.json {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "subkey must be a scalar")
			}
			request.Subkey = value.text
		case "values":
			if !value.json {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "values must use JSON '[...]'")
			}
			if err := json.Unmarshal([]byte(value.text), &request.Values); err != nil {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "values JSON must be an array: "+err.Error())
			}
		case "pairs":
			if !value.json {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "pairs must use JSON '{...}'")
			}
			if err := json.Unmarshal([]byte(value.text), &request.Pairs); err != nil {
				return CacheCommandRequest{}, sqlValueDiagnostic(value, "pairs JSON must be an object: "+err.Error())
			}
		case "priority":
			parsed, err := sqlInt64(value, "priority")
			if err != nil {
				return CacheCommandRequest{}, err
			}
			request.Priority = int64SQLPointer(parsed)
		case "ttl_seconds":
			parsed, err := sqlPositiveInt64(value, "ttl_seconds")
			if err != nil {
				return CacheCommandRequest{}, err
			}
			request.TTLSeconds = int64SQLPointer(parsed)
		case "unix_seconds":
			parsed, err := sqlInt64(value, "unix_seconds")
			if err != nil {
				return CacheCommandRequest{}, err
			}
			request.UnixSeconds = int64SQLPointer(parsed)
		default:
			return CacheCommandRequest{}, sqlTokenDiagnostic(*argument.name, unknownSQLCallFieldMessage(argument.name.text), nearestSQLName(argument.name.text, sqlCallFields))
		}
	}
	if request.Key == "" {
		return CacheCommandRequest{}, sqlTokenDiagnostic(sqlToken{}, "CALL "+command+" requires key => ...")
	}
	if request.TTLSeconds != nil && request.UnixSeconds != nil {
		return CacheCommandRequest{}, sqlTokenDiagnostic(sqlToken{}, "ttl_seconds and unix_seconds cannot be combined")
	}
	if err := validateSQLNamedCallFields(command, seen); err != nil {
		return CacheCommandRequest{}, err
	}
	return request, nil
}

// A named argument must be meaningful to the target command. These scalar
// commands previously accepted fields such as values=>JSON '[...]' and then
// silently dropped them before the request reached the server.
func validateSQLNamedCallFields(command string, fields map[string]struct{}) error {
	var allowed map[string]struct{}
	switch command {
	case "GET", "GETSTR", "EXISTS", "TTL", "DUMP", "DEL", "PERSIST":
		allowed = map[string]struct{}{"key": {}}
	case "SET", "SETSTR", "SETINT", "INC":
		allowed = map[string]struct{}{"key": {}, "value": {}}
	case "SETX", "SETSTRX", "SETINTX":
		allowed = map[string]struct{}{"key": {}, "value": {}, "ttl_seconds": {}}
	case "EXPIRE":
		allowed = map[string]struct{}{"key": {}, "ttl_seconds": {}}
	case "EXPIREAT":
		allowed = map[string]struct{}{"key": {}, "unix_seconds": {}}
	default:
		return nil
	}
	for field := range fields {
		if _, ok := allowed[field]; !ok {
			return sqlTokenDiagnostic(sqlToken{}, "CALL "+command+" does not accept "+strconv.Quote(field)+"; this argument would be ignored")
		}
	}
	return nil
}

func compileSQLPositionalCall(command string, arguments []sqlCallArgument) (CacheCommandRequest, error) {
	for _, argument := range arguments {
		if argument.value.json {
			return CacheCommandRequest{}, sqlValueDiagnostic(argument.value, "JSON values require named CALL arguments")
		}
	}
	request := CacheCommandRequest{Command: command}
	switch command {
	case "GET", "GETSTR", "EXISTS", "TTL", "DUMP", "DEL", "PERSIST":
		if len(arguments) != 1 {
			return CacheCommandRequest{}, sqlCallArityDiagnostic(arguments, command, "one key argument")
		}
		request.Key = arguments[0].value.text
	case "SET", "SETSTR", "SETINT", "INC":
		if len(arguments) != 2 {
			return CacheCommandRequest{}, sqlCallArityDiagnostic(arguments, command, "key and value arguments")
		}
		request.Key = arguments[0].value.text
		request.Value = arguments[1].value.text
	default:
		return CacheCommandRequest{}, sqlCallArityDiagnostic(arguments, command, "named arguments, for example key => 'cache-key'")
	}
	return request, nil
}

func (parser *sqlParser) parseKeyPredicate() (sqlValue, error) {
	if err := parser.expectKeyword("WHERE"); err != nil {
		return sqlValue{}, err
	}
	if err := parser.expectKeyword("key"); err != nil {
		return sqlValue{}, err
	}
	if err := parser.expectKind(sqlTokenEqual, "="); err != nil {
		return sqlValue{}, err
	}
	key, err := parser.parseScalar()
	if err != nil {
		return sqlValue{}, err
	}
	if key.json {
		return sqlValue{}, sqlValueDiagnostic(key, "key must be a scalar")
	}
	return key, nil
}

func (parser *sqlParser) parseIdentifierList() ([]sqlToken, error) {
	if err := parser.expectKind(sqlTokenLeftParen, "("); err != nil {
		return nil, err
	}
	values := make([]sqlToken, 0, 2)
	for {
		value, err := parser.expectIdentifier("a column name", nil)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if parser.current().kind != sqlTokenComma {
			break
		}
		parser.next()
	}
	if err := parser.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return values, nil
}

func (parser *sqlParser) parseScalarList() ([]sqlValue, error) {
	if err := parser.expectKind(sqlTokenLeftParen, "("); err != nil {
		return nil, err
	}
	values := make([]sqlValue, 0, 2)
	for {
		value, err := parser.parseScalar()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if parser.current().kind != sqlTokenComma {
			break
		}
		parser.next()
	}
	if err := parser.expectKind(sqlTokenRightParen, ")"); err != nil {
		return nil, err
	}
	return values, nil
}

func (parser *sqlParser) parseLiteral() (sqlValue, error) {
	if parser.current().kind == sqlTokenIdentifier && strings.EqualFold(parser.current().text, "JSON") {
		jsonToken := parser.current()
		parser.next()
		if parser.current().kind != sqlTokenString {
			return sqlValue{}, parser.expected(parser.current(), "a JSON string literal", nil)
		}
		value := parser.current()
		parser.next()
		return sqlValue{text: value.text, json: true, token: jsonToken}, nil
	}
	return parser.parseScalar()
}

func (parser *sqlParser) parseScalar() (sqlValue, error) {
	token := parser.current()
	switch token.kind {
	case sqlTokenString, sqlTokenNumber:
		parser.next()
		return sqlValue{text: token.text, token: token}, nil
	case sqlTokenIdentifier:
		if strings.EqualFold(token.text, "NULL") {
			parser.next()
			return sqlValue{text: "null", token: token}, nil
		}
	}
	return sqlValue{}, parser.expected(token, "a string, number, or NULL", nil)
}

func (parser *sqlParser) expectKeyword(keyword string) error {
	token := parser.current()
	if token.kind == sqlTokenIdentifier && strings.EqualFold(token.text, keyword) {
		parser.next()
		return nil
	}
	return parser.expected(token, keyword, []string{keyword})
}

func (parser *sqlParser) expectIdentifier(expected string, candidates []string) (sqlToken, error) {
	token := parser.current()
	if token.kind != sqlTokenIdentifier {
		return sqlToken{}, parser.expected(token, expected, candidates)
	}
	parser.next()
	return token, nil
}

func (parser *sqlParser) expectKind(kind sqlTokenKind, expected string) error {
	token := parser.current()
	if token.kind == kind {
		parser.next()
		return nil
	}
	return parser.expected(token, expected, nil)
}

func (parser *sqlParser) expected(token sqlToken, expected string, candidates []string) error {
	message := "unexpected " + token.display() + "; expected " + expected
	suggestion := ""
	if token.kind == sqlTokenIdentifier {
		suggestion = nearestSQLName(token.text, candidates)
	}
	return &SQLDiagnostic{Message: message, Line: token.line, Column: token.column, EndColumn: token.endColumn, Suggestion: suggestion}
}

func (parser *sqlParser) diagnostic(token sqlToken, message string) error {
	return sqlTokenDiagnostic(token, message)
}

func (parser *sqlParser) current() sqlToken {
	if parser.index >= len(parser.tokens) {
		return sqlToken{kind: sqlTokenEOF, line: 1, column: 1, endColumn: 1}
	}
	return parser.tokens[parser.index]
}

func (parser *sqlParser) previous() sqlToken {
	if parser.index == 0 {
		return parser.current()
	}
	return parser.tokens[parser.index-1]
}

func (parser *sqlParser) peek() sqlToken {
	if parser.index+1 >= len(parser.tokens) {
		return sqlToken{kind: sqlTokenEOF, line: parser.current().line, column: parser.current().endColumn, endColumn: parser.current().endColumn}
	}
	return parser.tokens[parser.index+1]
}

func (parser *sqlParser) next() {
	if parser.index < len(parser.tokens) {
		parser.index++
	}
}

func sqlInt64(value sqlValue, name string) (int64, error) {
	if value.json {
		return 0, sqlValueDiagnostic(value, name+" must be an integer")
	}
	parsed, err := strconv.ParseInt(value.text, 10, 64)
	if err != nil {
		return 0, sqlValueDiagnostic(value, name+" must be a 64-bit integer")
	}
	return parsed, nil
}

func sqlPositiveInt64(value sqlValue, name string) (int64, error) {
	parsed, err := sqlInt64(value, name)
	if err != nil {
		return 0, err
	}
	if parsed <= 0 {
		return 0, sqlValueDiagnostic(value, name+" must be positive")
	}
	return parsed, nil
}

func int64SQLPointer(value int64) *int64 {
	return &value
}

func sqlTokenDiagnostic(token sqlToken, message string, suggestion ...string) error {
	value := ""
	if len(suggestion) > 0 {
		value = suggestion[0]
	}
	return &SQLDiagnostic{Message: message, Line: token.line, Column: token.column, EndColumn: token.endColumn, Suggestion: value}
}

func sqlValueDiagnostic(value sqlValue, message string) error {
	return sqlTokenDiagnostic(value.token, message)
}

func sqlFieldDiagnostic(fields map[string]sqlValue, name string, message string) error {
	if value, ok := fields[name]; ok {
		return sqlValueDiagnostic(value, message)
	}
	return &SQLDiagnostic{Message: message, Line: 1, Column: 1, EndColumn: 1}
}

func sqlCallArityDiagnostic(arguments []sqlCallArgument, command string, expected string) error {
	token := sqlToken{line: 1, column: 1, endColumn: 1}
	if len(arguments) > 0 {
		token = arguments[0].value.token
	}
	return sqlTokenDiagnostic(token, "CALL "+command+" expects "+expected)
}

func unknownSQLCallFieldMessage(name string) string {
	return "unknown CALL field " + strconv.Quote(name) + "; expected key, value, values, subkey, pairs, priority, ttl_seconds, or unix_seconds"
}

var sqlCallFields = []string{"key", "value", "values", "subkey", "pairs", "priority", "ttl_seconds", "unix_seconds"}

func nearestSQLName(value string, candidates []string) string {
	if len(candidates) == 0 {
		return ""
	}
	value = strings.ToUpper(value)
	best := ""
	bestDistance := 3
	tied := false
	for _, candidate := range candidates {
		distance := sqlEditDistance(value, strings.ToUpper(candidate))
		if distance < bestDistance {
			best = candidate
			bestDistance = distance
			tied = false
		} else if distance == bestDistance {
			tied = true
		}
	}
	if tied || best == "" {
		return ""
	}
	return best
}

func sqlEditDistance(left string, right string) int {
	previous := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for index := range previous {
		previous[index] = index
	}
	for leftIndex := 1; leftIndex <= len(left); leftIndex++ {
		current[0] = leftIndex
		for rightIndex := 1; rightIndex <= len(right); rightIndex++ {
			cost := 0
			if left[leftIndex-1] != right[rightIndex-1] {
				cost = 1
			}
			current[rightIndex] = minSQLInt(
				previous[rightIndex]+1,
				current[rightIndex-1]+1,
				previous[rightIndex-1]+cost,
			)
		}
		previous, current = current, previous
	}
	return previous[len(right)]
}

func minSQLInt(values ...int) int {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

func publicSQLCommandNames() []string {
	return []string{
		"GET", "GETSTR", "EXISTS", "SET", "SETSTR", "SETX", "SETSTRX", "SETINT", "SETINTX", "INC", "DEL", "TTL", "EXPIRE", "EXPIREAT", "PERSIST", "DUMP",
		"PUTMAP", "PEEKMAP", "TAKEMAP", "PUSHSLICE", "POPSLICE", "SHIFTSLICE", "HEADSLICE", "TAILSLICE", "ADDSET", "REMSET", "HASSET", "GETSET",
		"PUSHPQ", "PUSHPRIORITY", "PEEKPQ", "PEEKPRIORITY", "POPPQ", "POPPRIORITY", "GETPQ", "GETPRIORITY",
		"CREATEBF", "RESERVEBF", "BFRESERVE", "ADDBF", "BFADD", "HASBF", "BFHAS", "BFEXISTS", "INFOBF", "BFINFO",
		"CREATECF", "RESERVECF", "CFRESERVE", "ADDCF", "CFADD", "HASCF", "CFHAS", "CFEXISTS", "DELCF", "REMCF", "CFDEL", "INFOCF", "CFINFO",
		"CREATEXF", "RESERVEXF", "XFRESERVE", "CREATEXOR", "ADDXF", "XFADD", "BUILDXF", "XFBUILD", "HASXF", "XFHAS", "XFEXISTS", "INFOXF", "XFINFO",
		"CREATERB", "CREATEROARING", "RBRESERVE", "ADDRB", "RBADD", "REMRB", "DELRB", "RBREM", "RBDEL", "HASRB", "RBHAS", "RBEXISTS", "COUNTRB", "RBCOUNT", "GETRB", "RBGET", "INFORB", "RBINFO",
		"CREATESB", "CREATESPARSEBITSET", "SBRESERVE", "ADDSB", "SBADD", "REMSB", "DELSB", "SBREM", "SBDEL", "HASSB", "SBHAS", "SBEXISTS", "COUNTSB", "SBCOUNT", "GETSB", "SBGET", "INFOSB", "SBINFO",
		"CREATERT", "CREATERADIX", "RTCREATE", "PUTRT", "RTPUT", "GETRT", "RTGET", "DELRT", "REMRT", "RTDEL", "RTREM", "HASRT", "RTEXISTS", "RTHAS", "PREFIXRT", "SCANRT", "RTPREFIX", "RTSCAN", "INFORT", "RTINFO",
		"CREATECMS", "RESERVECMS", "CMSRESERVE", "INCRCMS", "ADDCMS", "CMSADD", "ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT", "INFOCMS", "CMSINFO",
		"CREATEHLL", "RESERVEHLL", "HLLRESERVE", "ADDHLL", "HLLADD", "COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD", "INFOHLL", "HLLINFO",
		"CREATETOPK", "RESERVETOPK", "TOPKRESERVE", "ADDTOPK", "TOPKADD", "ESTTOPK", "QUERYTOPK", "TOPKCOUNT", "GETTOPK", "TOPK", "INFOTOPK", "TOPKINFO",
		"CREATERS", "CREATESAMPLE", "RESERVERS", "RSRESERVE", "ADDRS", "RSADD", "GETRS", "RSGET", "SAMPLE", "INFORS", "RSINFO",
		"CREATEQ", "CREATEQS", "CREATEQUANTILE", "RESERVEQ", "QSRESERVE", "ADDQ", "ADDQS", "QADD", "QSADD", "ESTQ", "QUERYQ", "QQUERY", "QSQUERY", "QUANTILE", "INFOQ", "QINFO", "INFOQS", "QSINFO",
		"CREATEFW", "CREATEFENWICK", "RESERVEFW", "FWRESERVE", "ADDFW", "FWADD", "GETFW", "FWGET", "SUMFW", "PREFIXFW", "FWPREFIX", "FWSUM", "RANGEFW", "FWRANGE", "INFOFW", "FWINFO",
		"MAP.PUT", "MAP.PEEK", "MAP.TAKE", "SLICE.PUSH", "SLICE.POP", "SLICE.SHIFT", "SLICE.HEAD", "SLICE.TAIL", "SET.ADD", "SET.REM", "SET.HAS", "SET.GET", "PQ.PUSH", "PQ.PEEK", "PQ.POP", "PQ.GET",
		"BF.CREATE", "BF.ADD", "BF.HAS", "BF.INFO", "CF.CREATE", "CF.ADD", "CF.HAS", "CF.DEL", "CF.INFO", "XF.CREATE", "XF.ADD", "XF.BUILD", "XF.HAS", "XF.INFO",
		"RB.CREATE", "RB.ADD", "RB.REM", "RB.HAS", "RB.COUNT", "RB.GET", "RB.INFO", "SB.CREATE", "SB.ADD", "SB.REM", "SB.HAS", "SB.COUNT", "SB.GET", "SB.INFO",
		"RT.CREATE", "RT.PUT", "RT.GET", "RT.DEL", "RT.HAS", "RT.PREFIX", "RT.INFO", "CMS.CREATE", "CMS.ADD", "CMS.EST", "CMS.INFO", "HLL.CREATE", "HLL.ADD", "HLL.COUNT", "HLL.INFO",
		"TOPK.CREATE", "TOPK.ADD", "TOPK.EST", "TOPK.GET", "TOPK.INFO", "RS.CREATE", "RS.ADD", "RS.GET", "RS.INFO", "Q.CREATE", "Q.ADD", "Q.EST", "Q.INFO", "FW.CREATE", "FW.ADD", "FW.GET", "FW.SUM", "FW.RANGE", "FW.INFO",
	}
}

func isPublicSQLCommand(command string) bool {
	for _, candidate := range publicSQLCommandNames() {
		if command == candidate {
			return true
		}
	}
	return false
}

func isInternalSQLCommand(command string) bool {
	switch command {
	case "INTERNALSET", "INTERNALSETV2", "INTERNALSETV3", "INTERNALDEL", "INTERNALBATCH", "INTERNALBATCHV2", "INTERNALDIGESTV1":
		return true
	default:
		return false
	}
}
