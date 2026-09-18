package hatSql

import (
	"testing"
	"time"
)

func TestCH012ProjectionAdvisorComparesQuerySavingsWithMaintenance(t *testing.T) {
	advisor := NewSQLProjectionAdvisor(4)
	advisor.recordFeedback("hot", []string{"events"}, 10*time.Millisecond)
	advisor.recordFeedback("cold", []string{"events"}, 1100*time.Microsecond)
	recommendations, err := advisor.CostBasedRecommendations(0, SQLProjectionCostModel{
		ExpectedQueries:   100,
		ExpectedRefreshes: 5,
		QueryHitLatency:   time.Millisecond,
		InitialBuildCost:  20 * time.Millisecond,
		RefreshCost:       2 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(recommendations) != 2 {
		t.Fatalf("recommendations = %#v, want 2", recommendations)
	}
	hot := recommendations[0]
	if hot.QueryID != "hot" || hot.EstimatedQuerySavings != 900*time.Millisecond || hot.EstimatedMaintenanceCost != 30*time.Millisecond || hot.EstimatedNetBenefit != 870*time.Millisecond || !hot.WorthBuilding {
		t.Fatalf("hot recommendation = %#v", hot)
	}
	cold := recommendations[1]
	if cold.QueryID != "cold" || cold.EstimatedQuerySavings != 10*time.Millisecond || cold.EstimatedMaintenanceCost != 30*time.Millisecond || cold.EstimatedNetBenefit != -20*time.Millisecond || cold.WorthBuilding {
		t.Fatalf("cold recommendation = %#v", cold)
	}
}

func TestCH012ProjectionAdvisorRejectsInvalidCostModel(t *testing.T) {
	advisor := NewSQLProjectionAdvisor(1)
	for name, model := range map[string]SQLProjectionCostModel{
		"missing queries":  {ExpectedQueries: 0},
		"negative hit":     {ExpectedQueries: 1, QueryHitLatency: -time.Nanosecond},
		"negative build":   {ExpectedQueries: 1, InitialBuildCost: -time.Nanosecond},
		"negative refresh": {ExpectedQueries: 1, RefreshCost: -time.Nanosecond},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := advisor.CostBasedRecommendations(0, model); err == nil {
				t.Fatal("invalid cost model was accepted")
			}
		})
	}
}

func TestCH012ProjectionAdvisorBoundsAndSaturatesCostEstimates(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	advisor := NewSQLProjectionAdvisor(2)
	advisor.recordFeedback("saturated", []string{"events"}, maxDuration)
	advisor.recordFeedback("unhelpful", []string{"orders"}, time.Millisecond)

	recommendations, err := advisor.CostBasedRecommendations(1, SQLProjectionCostModel{
		ExpectedQueries:   2,
		QueryHitLatency:   time.Second,
		InitialBuildCost:  maxDuration,
		RefreshCost:       maxDuration,
		ExpectedRefreshes: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(recommendations) != 1 {
		t.Fatalf("recommendations = %#v, want one result", recommendations)
	}
	if recommendations[0].QueryID != "saturated" {
		t.Fatalf("top recommendation = %#v, want saturated", recommendations[0])
	}
	if recommendations[0].EstimatedQuerySavings != maxDuration || recommendations[0].EstimatedMaintenanceCost != maxDuration || recommendations[0].EstimatedNetBenefit != 0 || recommendations[0].WorthBuilding {
		t.Fatalf("saturated recommendation = %#v", recommendations[0])
	}
}
