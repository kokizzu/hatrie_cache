package hatSql

import (
	"strings"
	"testing"
	"unicode"
)

func BenchmarkCH026PhraseBaseline(b *testing.B) {
	rows := ch026BenchmarkTextRows()
	query := "alpha beta gamma"
	b.ReportMetric(float64(len(rows)), "rows/result")
	b.ResetTimer()
	matched := 0
	for iteration := 0; iteration < b.N; iteration++ {
		for _, row := range rows {
			if ch026BaselinePhrase(row, query) {
				matched++
			}
		}
	}
	ch026BenchmarkSink = matched
}

var ch026BenchmarkSink int

func ch026BenchmarkTextRows() []string {
	rows := make([]string, 20000)
	for index := range rows {
		if index%500 == 0 {
			rows[index] = "alpha beta gamma release notes"
			continue
		}
		rows[index] = "alpha unrelated document section " + strings.Repeat("x", index%7+1)
	}
	return rows
}

func ch026BaselinePhrase(value, query string) bool {
	valueTokens := ch026BaselineTokens(value)
	queryTokens := ch026BaselineTokens(query)
	if len(queryTokens) == 0 || len(valueTokens) < len(queryTokens) {
		return false
	}
	for start := 0; start+len(queryTokens) <= len(valueTokens); start++ {
		matched := true
		for offset, token := range queryTokens {
			if valueTokens[start+offset] != token {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func ch026BaselineTokens(value string) []string {
	parts := strings.FieldsFunc(strings.ToLower(value), func(character rune) bool {
		return !unicode.IsLetter(character) && !unicode.IsNumber(character)
	})
	return parts
}
