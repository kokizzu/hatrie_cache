package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func newCH012ProjectionAdvisorBenchmark() *SQLProjectionAdvisor {
	advisor := NewSQLProjectionAdvisor(128)
	for index := 0; index < 128; index++ {
		advisor.recordFeedback("query-"+strconv.Itoa(index), []string{"events"}, time.Duration(index+1)*time.Millisecond)
	}
	return advisor
}

func BenchmarkCH012ProjectionAdvisorObservedCost(b *testing.B) {
	advisor := newCH012ProjectionAdvisorBenchmark()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := advisor.CostRecommendations(32); len(got) != 32 {
			b.Fatalf("CostRecommendations() length = %d, want 32", len(got))
		}
	}
}

func BenchmarkCH012ProjectionAdvisorCostBased(b *testing.B) {
	advisor := newCH012ProjectionAdvisorBenchmark()
	model := SQLProjectionCostModel{
		ExpectedQueries:   100,
		ExpectedRefreshes: 5,
		QueryHitLatency:   100 * time.Microsecond,
		InitialBuildCost:  10 * time.Millisecond,
		RefreshCost:       1 * time.Millisecond,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got, err := advisor.CostBasedRecommendations(32, model); err != nil {
			b.Fatal(err)
		} else if len(got) != 32 {
			b.Fatalf("CostBasedRecommendations() length = %d, want 32", len(got))
		}
	}
}
