package hatSql

import (
	"testing"
	"time"
)

func BenchmarkSQLProjectionAdvisorSnapshotEncode(b *testing.B) {
	recommendations := benchmarkSQLProjectionAdvisorRecommendations()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := marshalSQLProjectionAdvisorSnapshot(recommendations); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLProjectionAdvisorSnapshotDecode(b *testing.B) {
	recommendations := benchmarkSQLProjectionAdvisorRecommendations()
	frame, err := marshalSQLProjectionAdvisorSnapshot(recommendations)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(frame)), "wire-bytes/op")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := unmarshalSQLProjectionAdvisorSnapshot(frame); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSQLProjectionAdvisorRecommendations() []SQLProjectionRecommendation {
	recommendations := make([]SQLProjectionRecommendation, 0, 32)
	for index := 0; index < 32; index++ {
		recommendations = append(recommendations, SQLProjectionRecommendation{
			QueryID:        "query-" + string(rune('a'+index)),
			Dependencies:   []string{"events"},
			Fields:         []string{"events.amount", "events.region"},
			FilterFields:   []string{"events.region"},
			GroupByFields:  []string{"events.region"},
			OrderByFields:  []string{"events.amount"},
			SlowQueries:    uint64(index + 1),
			TotalElapsed:   time.Duration(index+1) * time.Millisecond,
			AverageElapsed: time.Millisecond,
		})
	}
	return recommendations
}
