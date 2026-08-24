package hatSql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// TokenKind identifies one lexical SQL token.
type TokenKind uint8

const (
	TokenEOF TokenKind = iota
	TokenIdentifier
	TokenString
	TokenNumber
	TokenComma
	TokenSemicolon
	TokenLeftParen
	TokenRightParen
	TokenEqual
	TokenArrow
	TokenPlus
	TokenMinus
	TokenSlash
	TokenPercent
	TokenBang
	TokenDot
	TokenStar
	TokenLess
	TokenLessEqual
	TokenGreater
	TokenGreaterEqual
	TokenNotEqual
	TokenParameter
)

// Token is one SQL lexical unit. Positions are one-based and EndColumn is
// exclusive, which makes it suitable for rendering source spans directly.
type Token struct {
	kind      TokenKind
	text      string
	line      int
	column    int
	endColumn int
}

// Kind returns the token category.
func (token Token) Kind() TokenKind { return token.kind }

// Text returns the decoded token text.
func (token Token) Text() string { return token.text }

// Line returns the one-based source line.
func (token Token) Line() int { return token.line }

// Column returns the one-based source column.
func (token Token) Column() int { return token.column }

// EndColumn returns the exclusive one-based source column.
func (token Token) EndColumn() int { return token.endColumn }

// Display returns a user-facing representation of a token.
func (token Token) Display() string {
	if token.kind == TokenEOF {
		return "end of input"
	}
	return strconv.Quote(token.text)
}

// Lex tokenizes the SQL grammar shared by cache-command compilation and
// read-only queries.
func Lex(source string) ([]Token, error) {
	lexer := lexer{source: source, line: 1, column: 1}
	tokens := make([]Token, 0, len(source)/4+1)
	for {
		lexer.skipWhitespace()
		if lexer.offset >= len(lexer.source) {
			tokens = append(tokens, lexer.token(TokenEOF, "", lexer.line, lexer.column))
			return tokens, nil
		}
		startLine, startColumn := lexer.line, lexer.column
		ch := lexer.source[lexer.offset]
		switch {
		case isIdentifierStart(ch):
			start := lexer.offset
			for lexer.offset < len(lexer.source) && isIdentifierPart(lexer.source[lexer.offset]) {
				lexer.advanceRune()
			}
			tokens = append(tokens, lexer.token(TokenIdentifier, lexer.source[start:lexer.offset], startLine, startColumn))
		case ch == '\'':
			value, err := lexer.readString(startLine, startColumn)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, lexer.token(TokenString, value, startLine, startColumn))
		case isDigit(ch) || ch == '-' && lexer.offset+1 < len(lexer.source) && isDigit(lexer.source[lexer.offset+1]):
			value, ok := lexer.readNumber()
			if !ok {
				return nil, lexer.diagnostic(startLine, startColumn, startColumn+1, "expected a number")
			}
			tokens = append(tokens, lexer.token(TokenNumber, value, startLine, startColumn))
		case ch == ',':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenComma, ",", startLine, startColumn))
		case ch == ';':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenSemicolon, ";", startLine, startColumn))
		case ch == '(':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenLeftParen, "(", startLine, startColumn))
		case ch == ')':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenRightParen, ")", startLine, startColumn))
		case ch == '+':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenPlus, "+", startLine, startColumn))
		case ch == '-':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenMinus, "-", startLine, startColumn))
		case ch == '/':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenSlash, "/", startLine, startColumn))
		case ch == '%':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenPercent, "%", startLine, startColumn))
		case ch == '.':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenDot, ".", startLine, startColumn))
		case ch == '*':
			lexer.advanceRune()
			tokens = append(tokens, lexer.token(TokenStar, "*", startLine, startColumn))
		case ch == '<':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(TokenLessEqual, "<=", startLine, startColumn))
			} else if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '>' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(TokenNotEqual, "<>", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(TokenLess, "<", startLine, startColumn))
			}
		case ch == '>':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(TokenGreaterEqual, ">=", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(TokenGreater, ">", startLine, startColumn))
			}
		case ch == '!':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '=' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(TokenNotEqual, "!=", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(TokenBang, "!", startLine, startColumn))
			}
		case ch == '=':
			lexer.advanceRune()
			if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '>' {
				lexer.advanceRune()
				tokens = append(tokens, lexer.token(TokenArrow, "=>", startLine, startColumn))
			} else {
				tokens = append(tokens, lexer.token(TokenEqual, "=", startLine, startColumn))
			}
		case ch == '$':
			lexer.advanceRune()
			if lexer.offset >= len(lexer.source) || !isDigit(lexer.source[lexer.offset]) {
				return nil, lexer.diagnostic(startLine, startColumn, lexer.column, "expected a positional parameter such as $1")
			}
			start := lexer.offset
			for lexer.offset < len(lexer.source) && isDigit(lexer.source[lexer.offset]) {
				lexer.advanceRune()
			}
			tokens = append(tokens, lexer.token(TokenParameter, lexer.source[start:lexer.offset], startLine, startColumn))
		default:
			return nil, lexer.diagnostic(startLine, startColumn, startColumn+1, fmt.Sprintf("unexpected character %q", ch))
		}
	}
}

type lexer struct {
	source string
	offset int
	line   int
	column int
}

func (lexer *lexer) token(kind TokenKind, text string, line int, column int) Token {
	return Token{kind: kind, text: text, line: line, column: column, endColumn: lexer.column}
}

func (lexer *lexer) diagnostic(line int, column int, endColumn int, message string) error {
	return &Diagnostic{Message: message, Line: line, Column: column, EndColumn: endColumn}
}

func (lexer *lexer) skipWhitespace() {
	for lexer.offset < len(lexer.source) {
		switch lexer.source[lexer.offset] {
		case ' ', '\t', '\r', '\n':
			lexer.advanceRune()
		default:
			return
		}
	}
}

func (lexer *lexer) advanceRune() {
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

func (lexer *lexer) readString(line int, column int) (string, error) {
	lexer.advanceRune()
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

func (lexer *lexer) readNumber() (string, bool) {
	start := lexer.offset
	if lexer.source[lexer.offset] == '-' {
		lexer.advanceRune()
	}
	digits := 0
	for lexer.offset < len(lexer.source) && isDigit(lexer.source[lexer.offset]) {
		digits++
		lexer.advanceRune()
	}
	if digits == 0 {
		return "", false
	}
	if lexer.offset < len(lexer.source) && lexer.source[lexer.offset] == '.' {
		lexer.advanceRune()
		fractionDigits := 0
		for lexer.offset < len(lexer.source) && isDigit(lexer.source[lexer.offset]) {
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
		for lexer.offset < len(lexer.source) && isDigit(lexer.source[lexer.offset]) {
			exponentDigits++
			lexer.advanceRune()
		}
		if exponentDigits == 0 {
			return "", false
		}
	}
	return lexer.source[start:lexer.offset], true
}

func isIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

// IsIdentifierStart reports whether value can begin an unquoted SQL
// identifier.
func IsIdentifierStart(value byte) bool {
	return isIdentifierStart(value)
}

func isIdentifierPart(value byte) bool {
	return isIdentifierStart(value) || isDigit(value)
}

func isDigit(value byte) bool {
	return value >= '0' && value <= '9'
}
