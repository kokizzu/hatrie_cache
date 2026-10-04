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
	remaining := make(map[string]struct{}, len(queryTokens))
	for _, token := range queryTokens {
		remaining[token] = struct{}{}
	}
	for _, token := range TextTokenPositions(value) {
		if _, exists := remaining[token.Token]; exists {
			delete(remaining, token.Token)
			if len(remaining) == 0 {
				return true
			}
		}
	}
	return false
}

func textContainsPrefix(value, prefix string) bool {
	prefixTokens := TextTokens(prefix)
	if len(prefixTokens) != 1 {
		return false
	}
	for _, token := range TextTokens(value) {
		if strings.HasPrefix(token, prefixTokens[0]) {
			return true
		}
	}
	return false
}
