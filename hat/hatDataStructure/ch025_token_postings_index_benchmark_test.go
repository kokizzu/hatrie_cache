package hatDataStructure

import (
	"fmt"
	"strings"
	"testing"
)

const ch025BenchmarkRows = 100_000

var ch025BenchmarkSink int

func BenchmarkCH025LinearTokenScan(b *testing.B) {
	documents := ch025BenchmarkDocuments()
	b.SetBytes(int64(len(documents) * 48))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		for _, document := range documents {
			if strings.Contains(document, " needle ") {
				matches++
			}
		}
		ch025BenchmarkSink = matches
	}
}

func BenchmarkCH025LinearTokenAnyScan(b *testing.B) {
	documents := ch025BenchmarkDocuments()
	b.SetBytes(int64(len(documents) * 48))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matches := 0
		for _, document := range documents {
			if strings.Contains(document, " needle ") || strings.Contains(document, " rare ") {
				matches++
			}
		}
		ch025BenchmarkSink = matches
	}
}

func BenchmarkCH025TokenPostings(b *testing.B) {
	documents := ch025BenchmarkDocuments()
	index := buildCH025TokenPostingsIndex(documents)
	b.Run("rows_for_token", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			ch025BenchmarkSink = len(index.RowsForToken("needle"))
		}
	})
	b.Run("visit_token", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			matches := 0
			index.VisitToken("needle", func(uint32) bool {
				matches++
				return true
			})
			ch025BenchmarkSink = matches
		}
	})
	b.Run("match_all", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			ch025BenchmarkSink = len(index.MatchAll("common needle"))
		}
	})
	b.Run("match_any", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			ch025BenchmarkSink = len(index.MatchAny("needle rare"))
		}
	})
	b.Run("build_20k", func(b *testing.B) {
		buildDocuments := documents[:20_000]
		var lastIndex *TokenPostingsIndex
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			lastIndex = buildCH025TokenPostingsIndex(buildDocuments)
			ch025BenchmarkSink = lastIndex.Len()
		}
		b.ReportMetric(float64(lastIndex.Info().EncodedBytes), "index-bytes")
	})
}

func ch025BenchmarkDocuments() []string {
	documents := make([]string, ch025BenchmarkRows)
	for row := range documents {
		markers := ""
		if row%97 == 0 {
			markers += " needle"
		}
		if row%89 == 0 {
			markers += " rare"
		}
		documents[row] = fmt.Sprintf("document %d common words%s tail", row, markers)
	}
	return documents
}

func buildCH025TokenPostingsIndex(documents []string) *TokenPostingsIndex {
	index := NewTokenPostingsIndex()
	for row, document := range documents {
		index.Upsert(uint32(row), document)
	}
	return index
}
