package hatSql

import "sort"

// SQLPointLookupStrategy identifies the physical path selected for an
// equality lookup.
type SQLPointLookupStrategy string

const (
	SQLPointLookupScan        SQLPointLookupStrategy = "scan"
	SQLPointLookupArrangement SQLPointLookupStrategy = "arrangement"
)

const (
	SQLPointLookupReasonArrangementSelected = "arrangement_selected"
	SQLPointLookupReasonNoCandidate         = "no_candidate"
	SQLPointLookupReasonUnavailable         = "unavailable"
	SQLPointLookupReasonMemoryBudget        = "memory_budget"
	SQLPointLookupReasonNoReadSavings       = "no_read_savings"
	SQLPointLookupReasonInsufficientReads   = "insufficient_reads"
	SQLPointLookupReasonInsufficientBenefit = "insufficient_benefit"
)

// SQLPointLookupCandidate describes one complete-row point arrangement that
// can serve an equality lookup. The embedded cost fields use the same units
// as SQLArrangementCostModel, normally nanoseconds and bytes estimated by the
// caller from current source statistics.
type SQLPointLookupCandidate struct {
	SQLArrangementCostCandidate
	Available bool
}

// SQLPointLookupPlanOptions bounds the planner's use of maintained point
// arrangements. Zero values retain the cost model's one-unit minimum benefit
// and unlimited memory budget.
type SQLPointLookupPlanOptions struct {
	MemoryBudgetBytes      uint64
	MinimumNetBenefitNanos uint64
}

// SQLPointLookupCandidateScore explains one candidate's eligibility. The
// planner retains these bounded explanations so EXPLAIN-like callers can show
// why a scan won without retaining query text or predicate values.
type SQLPointLookupCandidateScore struct {
	Candidate SQLPointLookupCandidate
	Score     SQLArrangementCostScore
	Eligible  bool
	Reason    string
}

// SQLPointLookupPlan is the deterministic choice between a maintained point
// arrangement and a source scan. It is advisory: callers own execution and
// arrangement lifecycle.
type SQLPointLookupPlan struct {
	Strategy      SQLPointLookupStrategy
	Selected      SQLPointLookupCandidate
	SelectedScore SQLArrangementCostScore
	HasSelection  bool
	Reason        string
	Candidates    []SQLPointLookupCandidateScore
}

// PlanSQLPointLookup chooses the best currently available point arrangement
// or falls back to a scan. A candidate is selected only when modeled probe
// savings repay build and expected maintenance cost within the memory budget.
// Input candidates are never modified; returned explanations are sorted by
// eligibility, net benefit, payback, memory, and stable identity.
func PlanSQLPointLookup(candidates []SQLPointLookupCandidate, options SQLPointLookupPlanOptions) SQLPointLookupPlan {
	plan := SQLPointLookupPlan{Strategy: SQLPointLookupScan}
	if len(candidates) == 0 {
		plan.Reason = SQLPointLookupReasonNoCandidate
		return plan
	}

	model := NewSQLArrangementCostModel(SQLArrangementCostModelOptions{
		MemoryBudgetBytes:      options.MemoryBudgetBytes,
		MinimumNetBenefitNanos: options.MinimumNetBenefitNanos,
	})
	plan.Candidates = make([]SQLPointLookupCandidateScore, len(candidates))
	for index, candidate := range candidates {
		score := SQLPointLookupCandidateScore{
			Candidate: candidate,
			Score:     model.Evaluate(candidate.SQLArrangementCostCandidate),
		}
		switch {
		case !candidate.Available:
			score.Reason = SQLPointLookupReasonUnavailable
		case !score.Score.FitsMemory:
			score.Reason = SQLPointLookupReasonMemoryBudget
		case score.Score.ReadSavingsNanos == 0:
			score.Reason = SQLPointLookupReasonNoReadSavings
		case score.Score.PaybackReads > candidate.ExpectedReads:
			score.Reason = SQLPointLookupReasonInsufficientReads
		case !score.Score.Reusable:
			score.Reason = SQLPointLookupReasonInsufficientBenefit
		default:
			score.Eligible = true
			score.Reason = SQLPointLookupReasonArrangementSelected
		}
		plan.Candidates[index] = score
	}
	sort.SliceStable(plan.Candidates, func(left, right int) bool {
		first, second := plan.Candidates[left], plan.Candidates[right]
		if first.Eligible != second.Eligible {
			return first.Eligible
		}
		if first.Score.NetBenefitNanos != second.Score.NetBenefitNanos {
			return first.Score.NetBenefitNanos > second.Score.NetBenefitNanos
		}
		if first.Score.PaybackReads != second.Score.PaybackReads {
			return first.Score.PaybackReads < second.Score.PaybackReads
		}
		if first.Candidate.MemoryBytes != second.Candidate.MemoryBytes {
			return first.Candidate.MemoryBytes < second.Candidate.MemoryBytes
		}
		if first.Candidate.Key != second.Candidate.Key {
			return first.Candidate.Key < second.Candidate.Key
		}
		if first.Candidate.Field != second.Candidate.Field {
			return first.Candidate.Field < second.Candidate.Field
		}
		return first.Candidate.Kind < second.Candidate.Kind
	})

	best := plan.Candidates[0]
	if best.Eligible {
		plan.Strategy = SQLPointLookupArrangement
		plan.Selected = best.Candidate
		plan.SelectedScore = best.Score
		plan.HasSelection = true
		plan.Reason = SQLPointLookupReasonArrangementSelected
		return plan
	}
	plan.Reason = best.Reason
	return plan
}
