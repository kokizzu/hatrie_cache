package hatSql

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

var ErrSQLIndexStrategyHintUnsupported = errors.New("SQL index strategy hint unsupported")

const (
	SQLIndexStrategyReasonPriority          = "priority"
	SQLIndexStrategyReasonForced            = "forced"
	SQLIndexStrategyReasonNoForcedCandidate = "no_forced_candidate"
	SQLIndexStrategyReasonNoEligible        = "no_eligible_candidate"
	SQLIndexStrategyReasonLowerPriority     = "lower_priority"
	SQLIndexStrategyReasonUnavailable       = "unavailable"
	SQLIndexStrategyReasonFieldMismatch     = "field_mismatch"
	SQLIndexStrategyReasonKindMismatch      = "kind_mismatch"
	SQLIndexStrategyReasonForbidden         = "forbidden"
	SQLIndexStrategyReasonClusterMismatch   = "cluster_mismatch"
	SQLIndexStrategyReasonClusterRequired   = "cluster_required"
)

// SQLIndexStrategyCandidate describes one configured physical index option.
// Lower Priority, EstimatedCost, and EstimatedRows values win tie breaks in
// that order. Available is explicit so an unavailable or rebuilding index can
// be shown in an explanation instead of silently disappearing.
type SQLIndexStrategyCandidate struct {
	SQLIndexDefinition
	Priority      int
	EstimatedRows int
	EstimatedCost int
	Available     bool
}

// SQLIndexStrategyCandidateReport explains why one candidate was selected or
// rejected by ExplainSQLIndexStrategy.
type SQLIndexStrategyCandidateReport struct {
	SQLIndexStrategyCandidate
	Eligible bool
	Reason   string
}

// SQLIndexStrategyDecision is a deterministic strategy choice and its bounded
// candidate explanation.
type SQLIndexStrategyDecision struct {
	Source        string
	Field         string
	RequestedKind string
	Selected      SQLIndexStrategyCandidate
	HasSelection  bool
	Reason        string
	Candidates    []SQLIndexStrategyCandidateReport
}

// ExplainSQLIndexStrategy chooses one configured index candidate and reports
// every candidate's rejection reason. It is a planner-facing helper for
// embedded resolvers; it does not create indexes or execute a query.
func ExplainSQLIndexStrategy(source, field string, hint SQLIndexHint, candidates []SQLIndexStrategyCandidate) (SQLIndexStrategyDecision, error) {
	decision := SQLIndexStrategyDecision{
		Source: source,
		Field:  field,
	}
	field = strings.TrimSpace(field)
	decision.Field = field
	if err := hint.validate(); err != nil {
		return decision, err
	}
	if field == "" {
		return decision, fmt.Errorf("%w: field is required", ErrSQLIndexStrategyHintUnsupported)
	}
	hintKind := strings.ToUpper(strings.TrimSpace(hint.Kind))
	decision.RequestedKind = hintKind
	requestedCluster := strings.TrimSpace(hint.Cluster)
	if hint.Mode != "" && hint.Source != "" && !strings.EqualFold(strings.TrimSpace(source), strings.TrimSpace(hint.Source)) {
		decision.Reason = "source_mismatch"
		return decision, nil
	}

	reports := make([]SQLIndexStrategyCandidateReport, len(candidates))
	for index, candidate := range candidates {
		reports[index] = SQLIndexStrategyCandidateReport{
			SQLIndexStrategyCandidate: candidate,
			Reason:                    SQLIndexStrategyReasonNoEligible,
		}
	}
	sort.SliceStable(reports, func(left, right int) bool {
		leftMatches := strings.EqualFold(strings.TrimSpace(reports[left].Field), field)
		rightMatches := strings.EqualFold(strings.TrimSpace(reports[right].Field), field)
		if leftMatches != rightMatches {
			return leftMatches
		}
		if reports[left].Available != reports[right].Available {
			return reports[left].Available
		}
		if reports[left].Priority != reports[right].Priority {
			return reports[left].Priority < reports[right].Priority
		}
		if reports[left].EstimatedCost != reports[right].EstimatedCost {
			return reports[left].EstimatedCost < reports[right].EstimatedCost
		}
		if reports[left].EstimatedRows != reports[right].EstimatedRows {
			return reports[left].EstimatedRows < reports[right].EstimatedRows
		}
		if !strings.EqualFold(reports[left].Kind, reports[right].Kind) {
			return strings.ToUpper(reports[left].Kind) < strings.ToUpper(reports[right].Kind)
		}
		return reports[left].Key < reports[right].Key
	})

	for index := range reports {
		report := &reports[index]
		if !strings.EqualFold(strings.TrimSpace(report.Field), field) {
			report.Reason = SQLIndexStrategyReasonFieldMismatch
			continue
		}
		candidateCluster := report.Cluster
		if requestedCluster == "" && candidateCluster != "" {
			report.Reason = SQLIndexStrategyReasonClusterRequired
			continue
		}
		if requestedCluster != "" && candidateCluster != "" && !strings.EqualFold(strings.TrimSpace(candidateCluster), requestedCluster) {
			report.Reason = SQLIndexStrategyReasonClusterMismatch
			continue
		}
		if !report.Available {
			report.Reason = SQLIndexStrategyReasonUnavailable
			continue
		}
		kind := strings.ToUpper(strings.TrimSpace(report.Kind))
		if hintKind != "" && hint.Mode == SQLIndexHintForce && kind != hintKind {
			report.Reason = SQLIndexStrategyReasonKindMismatch
			continue
		}
		if hintKind != "" && hint.Mode == SQLIndexHintForbid && kind == hintKind {
			report.Reason = SQLIndexStrategyReasonForbidden
			continue
		}
		if hint.Mode == SQLIndexHintForbid && hintKind == "" {
			// A field-only FORBID hint rejects every candidate for this field.
			report.Reason = SQLIndexStrategyReasonForbidden
			continue
		}
		if decision.HasSelection {
			report.Reason = SQLIndexStrategyReasonLowerPriority
			continue
		}
		report.Eligible = true
		decision.Selected = report.SQLIndexStrategyCandidate
		decision.HasSelection = true
		if hint.Mode == SQLIndexHintForce {
			decision.Reason = SQLIndexStrategyReasonForced
		} else {
			decision.Reason = SQLIndexStrategyReasonPriority
		}
	}
	decision.Candidates = reports
	if !decision.HasSelection {
		if hint.Mode == SQLIndexHintForce {
			decision.Reason = SQLIndexStrategyReasonNoForcedCandidate
			return decision, fmt.Errorf("%w: no available %s index for field %q", ErrSQLIndexStrategyHintUnsupported, hintKind, field)
		}
		if decision.Reason == "" {
			decision.Reason = SQLIndexStrategyReasonNoEligible
		}
	}
	return decision, nil
}
