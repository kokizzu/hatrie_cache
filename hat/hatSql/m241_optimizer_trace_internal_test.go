package hatSql

import "testing"

func TestM241OptimizerTraceBypassesResultCache(t *testing.T) {
	if sqlResultCacheOptionsEligible(SQLQueryOptions{
		OptimizerTrace: &SQLOptimizerTraceOptions{},
	}) {
		t.Fatal("optimizer trace must bypass the result cache")
	}
}

func TestM241OptimizerTraceAttachesRejectedAlternativesAndNotices(t *testing.T) {
	trace := newSQLOptimizerTrace(&SQLOptimizerTraceOptions{MaxEntries: 2})
	result := SQLQueryResult{
		Plan: []SQLExplainStep{{
			Alternatives: []SQLExplainAlternative{
				{Expression: "selected", Selected: true},
				{Expression: "rejected", RejectedReason: "higher cost"},
			},
			Notices: []SQLExplainNotice{{Code: "RULE", Detail: "considered"}},
		}},
	}
	attachSQLOptimizerTrace(&result, trace, nil)
	if result.OptimizerTrace == nil || len(result.OptimizerTrace.RejectedAlternatives) != 1 || len(result.OptimizerTrace.Notices) != 1 {
		t.Fatalf("attached trace = %#v, want one rejected alternative and one notice", result.OptimizerTrace)
	}
	if result.OptimizerTrace.RejectedAlternatives[0].Expression != "rejected" {
		t.Fatalf("rejected alternative = %#v", result.OptimizerTrace.RejectedAlternatives[0])
	}
}
