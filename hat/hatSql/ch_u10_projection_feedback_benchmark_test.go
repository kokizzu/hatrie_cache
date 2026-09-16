package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkSQLProjectionAdvisorCostRecommendations(b *testing.B) {
	advisor := NewSQLProjectionAdvisor(128)
	for index := 0; index < 128; index++ {
		advisor.recordFeedback("query-"+strconv.Itoa(index), []string{"events"}, time.Duration(index+1)*time.Millisecond)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if got := advisor.CostRecommendations(16); len(got) != 16 {
			b.Fatalf("CostRecommendations() length = %d, want 16", len(got))
		}
	}
}
