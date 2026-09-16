package hatSql

import (
	"context"
	"testing"
	"time"
)

var sqlCHU11SkipIndexAdvisorBenchmarkSink []SQLJSONPathSkipIndexRecommendation

func BenchmarkCHU11SQLIndexAdvisorSkipIndexRecommendations(b *testing.B) {
	advisor := NewSQLIndexAdvisor(128)
	for index := 0; index < 128; index++ {
		advisor.skipCounts[sqlIndexAdvisorSkipKey{
			key:   "table_" + string(rune('a'+index/26)),
			field: "doc",
			path:  "$.field_" + string(rune('a'+index%26)),
		}] = sqlIndexAdvisorSkipStats{slowQueries: uint64(index + 1), totalElapsedNanos: uint64((index + 1) * 1000)}
	}
	b.ReportAllocs()
	for b.Loop() {
		sqlCHU11SkipIndexAdvisorBenchmarkSink = advisor.SkipIndexRecommendations(16)
	}
}

func BenchmarkCHU11SQLIndexAdvisorJSONPathQuery(b *testing.B) {
	query := "FROM CACHE('people') AS person WHERE JSON_VALUE(person.doc, '$.profile.city') = 'Singapore' SELECT person.doc"
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"doc": map[string]interface{}{"profile": map[string]interface{}{"city": "Singapore"}}}}, nil
	})
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			options := QueryOptions{}
			if enabled {
				options.IndexAdvisor = NewSQLIndexAdvisor(8)
				options.SlowQueryThreshold = time.Nanosecond
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ExecuteQueryParameters(context.Background(), query, resolver, nil, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
