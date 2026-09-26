package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkCH012ProjectionAdvisorDefaultFeedback(b *testing.B) {
	advisor := NewSQLProjectionAdvisor(128)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		advisor.recordFeedback("query", []string{"events"}, time.Millisecond)
	}
}

func BenchmarkCH012ProjectionAdvisorWorkloadRecording(b *testing.B) {
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               128,
		EnableWorkloadForecast: true,
	})
	when := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		advisor.RecordWorkload("query", []string{"events"}, when)
	}
}

func BenchmarkCH012ProjectionAdvisorForecastWorkload(b *testing.B) {
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               128,
		EnableWorkloadForecast: true,
	})
	when := time.Unix(100, 0)
	for index := 0; index < 128; index++ {
		advisor.RecordWorkload("query-"+strconv.Itoa(index), []string{"events"}, when.Add(time.Duration(index)*time.Second))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		forecasts, err := advisor.ForecastWorkloadAt(when.Add(10*time.Minute), time.Hour)
		if err != nil || len(forecasts) != 128 {
			b.Fatalf("ForecastWorkloadAt() = %d, %v; want 128, nil", len(forecasts), err)
		}
	}
}

func BenchmarkCH012ProjectionAdvisorForecastCostBased(b *testing.B) {
	advisor := NewSQLProjectionAdvisorWithOptions(SQLProjectionAdvisorOptions{
		Capacity:               128,
		EnableWorkloadForecast: true,
	})
	base := time.Unix(100, 0)
	for index := 0; index < 128; index++ {
		queryID := "query-" + strconv.Itoa(index)
		advisor.recordFeedback(queryID, []string{"events"}, time.Duration(index+1)*time.Millisecond)
		advisor.RecordWorkload(queryID, []string{"events"}, base)
		advisor.RecordWorkload(queryID, []string{"events"}, base.Add(time.Hour))
	}
	model := SQLProjectionCostModel{
		ExpectedQueries:  1,
		QueryHitLatency:  100 * time.Microsecond,
		InitialBuildCost: 10 * time.Millisecond,
		RefreshCost:      time.Millisecond,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if recommendations, err := advisor.ForecastCostBasedRecommendations(32, base.Add(24*time.Hour), 24*time.Hour, model); err != nil {
			b.Fatal(err)
		} else if len(recommendations) != 32 {
			b.Fatalf("ForecastCostBasedRecommendations() length = %d, want 32", len(recommendations))
		}
	}
}
