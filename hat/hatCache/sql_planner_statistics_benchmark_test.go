package hatCache

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkSQLPlannerStatisticsAnalyze(b *testing.B) {
	trie := benchmarkSQLPlannerStatisticsTrie(b)
	defer trie.Destroy()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLPlannerStatisticsLookup(b *testing.B) {
	trie := benchmarkSQLPlannerStatisticsTrie(b)
	defer trie.Destroy()
	if _, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, available, err := trie.SQLWhatIfSourceStatistics("CACHE", "people", []string{"age", "state"}); err != nil || !available {
			b.Fatalf("SQLWhatIfSourceStatistics() = available %v, error %v", available, err)
		}
	}
}

func BenchmarkSQLPlannerStatisticsWhatIf(b *testing.B) {
	query := `FROM CACHE('people') AS p WHERE p.age = 21 SELECT p.id`
	request := SQLWhatIfRequest{Query: query, Index: SQLWhatIfIndex{Kind: SQLWhatIfIndexEquality, Fields: []string{"age"}}}
	for _, analyzed := range []bool{false, true} {
		name := "WithoutAnalyze"
		if analyzed {
			name = "WithAnalyze"
		}
		b.Run(name, func(b *testing.B) {
			trie := benchmarkSQLPlannerStatisticsTrie(b)
			defer trie.Destroy()
			if analyzed {
				if _, err := trie.AnalyzeSQLSource("CACHE", "people", "age", "state"); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if report, err := ExplainSQLWhatIf(context.Background(), request, trie); err != nil || !report.Supported {
					b.Fatalf("ExplainSQLWhatIf() = %#v, error %v", report, err)
				}
			}
		})
	}
}

func benchmarkSQLPlannerStatisticsTrie(b *testing.B) *HatTrie {
	b.Helper()
	var source strings.Builder
	source.Grow(320_000)
	source.WriteByte('[')
	for i := 0; i < 10_000; i++ {
		if i > 0 {
			source.WriteByte(',')
		}
		source.WriteString(`{"id":`)
		source.WriteString(strconv.Itoa(i))
		source.WriteString(`,"age":`)
		source.WriteString(strconv.Itoa(18 + i%64))
		source.WriteString(`,"state":"`)
		source.WriteString([]string{"open", "closed", "queued"}[i%3])
		source.WriteString(`"}`)
	}
	source.WriteByte(']')
	trie := CreateHatTrie()
	trie.UpsertString("people", source.String())
	return trie
}
