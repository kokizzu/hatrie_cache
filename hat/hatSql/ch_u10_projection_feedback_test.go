package hatSql

import (
	"testing"
	"time"
)

func TestSQLProjectionAdvisorCostRecommendationsUseObservedLatency(t *testing.T) {
	advisor := NewSQLProjectionAdvisor(4)
	advisor.recordFeedback("frequent", []string{"events"}, 2*time.Millisecond)
	advisor.recordFeedback("frequent", []string{"events"}, 2*time.Millisecond)
	advisor.recordFeedback("expensive", []string{"events"}, 10*time.Millisecond)
	advisor.recordFeedback("other", []string{"orders"}, 1*time.Millisecond)

	recommendations := advisor.CostRecommendations(2)
	if len(recommendations) != 2 {
		t.Fatalf("CostRecommendations() length = %d, want 2", len(recommendations))
	}
	if recommendations[0].QueryID != "expensive" || recommendations[0].TotalElapsed != 10*time.Millisecond || recommendations[0].AverageElapsed != 10*time.Millisecond {
		t.Fatalf("top cost recommendation = %#v", recommendations[0])
	}
	if recommendations[1].QueryID != "frequent" || recommendations[1].TotalElapsed != 4*time.Millisecond || recommendations[1].AverageElapsed != 2*time.Millisecond {
		t.Fatalf("second cost recommendation = %#v", recommendations[1])
	}
}
