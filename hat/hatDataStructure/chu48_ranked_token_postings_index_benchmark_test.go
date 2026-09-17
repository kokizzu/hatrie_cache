package hatDataStructure

import (
	"strings"
	"testing"
)

func BenchmarkCHU48RankedTokenPostings(b *testing.B) {
	documents := ch025BenchmarkDocuments()
	plainIndex := buildCH025TokenPostingsIndex(documents)
	rankedIndex := buildCHU48RankedTokenPostingsIndex(documents)

	b.Run("linear_any_scan", func(b *testing.B) {
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
	})
	b.Run("match_any_exact_index", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			ch025BenchmarkSink = len(plainIndex.MatchAny("needle rare"))
		}
	})
	b.Run("match_ranked_limit_10", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			rows, err := rankedIndex.MatchRanked("needle rare", TokenPostingsRankOptions{Limit: 10})
			if err != nil {
				b.Fatal(err)
			}
			ch025BenchmarkSink = len(rows)
		}
	})
	b.Run("match_ranked_limit_100", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			rows, err := rankedIndex.MatchRanked("needle rare", TokenPostingsRankOptions{Limit: 100})
			if err != nil {
				b.Fatal(err)
			}
			ch025BenchmarkSink = len(rows)
		}
	})
	b.Run("build_20k_ranked", func(b *testing.B) {
		buildDocuments := documents[:20_000]
		var lastIndex *TokenPostingsIndex
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			lastIndex = buildCHU48RankedTokenPostingsIndex(buildDocuments)
			ch025BenchmarkSink = lastIndex.Len()
		}
		b.ReportMetric(float64(len(lastIndex.ranking.documents)), "ranked-docs")
		b.ReportMetric(float64(rankedTokenPostingsFrequencyValues(lastIndex)), "rank-frequency-values")
	})
}

func buildCHU48RankedTokenPostingsIndex(documents []string) *TokenPostingsIndex {
	index := NewRankedTokenPostingsIndex()
	for row, document := range documents {
		index.Upsert(uint32(row), document)
	}
	return index
}

func rankedTokenPostingsFrequencyValues(index *TokenPostingsIndex) int {
	if index == nil || index.ranking == nil {
		return 0
	}
	values := 0
	for _, document := range index.ranking.documents {
		values += len(document.frequencies)
	}
	return values
}
