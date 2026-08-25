package hatSql

import (
	"strings"
	"unicode"
)

// TextTokens normalizes text into distinct lowercase letter-or-number tokens.
// It is shared by CONTAINS evaluation and opt-in text indexes so an index can
// only narrow candidates, never change text-search semantics.
func TextTokens(value string) []string {
	parts := strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsNumber(character)
	})
	if len(parts) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(parts))
	tokens := make([]string, 0, len(parts))
	for _, part := range parts {
		if _, exists := seen[part]; exists {
			continue
		}
		seen[part] = struct{}{}
		tokens = append(tokens, part)
	}
	return tokens
}

func textContains(value, query string) bool {
	queryTokens := TextTokens(query)
	if len(queryTokens) == 0 {
		return false
	}
	valueTokens := make(map[string]struct{})
	for _, token := range TextTokens(value) {
		valueTokens[token] = struct{}{}
	}
	for _, token := range queryTokens {
		if _, exists := valueTokens[token]; !exists {
			return false
		}
	}
	return true
}
