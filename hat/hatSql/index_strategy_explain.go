package hatSql

import (
	"sort"
	"strings"
)

type sqlExplainIndexCandidate struct {
	field      string
	estimate   int
	cost       int
	expression string
}

// sqlExplainIndexStrategy reports the same bounded equality candidates used by
// the runtime planner without probing source rows. It is intentionally limited
// to metadata-backed candidates so EXPLAIN remains read-only.
func sqlExplainIndexStrategy(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, hint SQLIndexHint) ([]SQLExplainAlternative, []SQLExplainNotice) {
	if resolver == nil || source.kind != "CACHE" {
		return nil, nil
	}
	conjuncts := make([]sqlExpr, 0, 2)
	var collect func(sqlExpr)
	collect = func(expression sqlExpr) {
		if expression.kind == "binary" && expression.op == "AND" && expression.left != nil && expression.right != nil {
			collect(*expression.left)
			collect(*expression.right)
			return
		}
		conjuncts = append(conjuncts, expression)
	}
	collect(condition)
	candidates := make([]sqlExplainIndexCandidate, 0, len(conjuncts))
	for _, conjunct := range conjuncts {
		field, ok := sqlExplainIndexedEqualityField(source, conjunct)
		if !ok {
			continue
		}
		estimate, err := sqlIndexedEqualityEstimate(source, conjunct, resolver)
		if err != nil || estimate == nil {
			continue
		}
		candidates = append(candidates, sqlExplainIndexCandidate{
			field:      field,
			estimate:   *estimate,
			cost:       sqlIndexedEqualityProbeCost(*estimate),
			expression: sqlExplainExpression(conjunct),
		})
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		return candidates[left].estimate < candidates[right].estimate
	})
	alternatives := make([]SQLExplainAlternative, 0, len(candidates))
	notices := make([]SQLExplainNotice, 0, len(candidates))
	selected := -1
	for index, candidate := range candidates {
		alternative := SQLExplainAlternative{
			Expression:    candidate.expression,
			EstimatedRows: candidate.estimate,
			EstimatedCost: candidate.cost,
		}
		reason := ""
		if hint.applies(source) {
			switch hint.Mode {
			case SQLIndexHintForbid:
				if strings.EqualFold(candidate.field, hint.Field) {
					reason = "forbidden by index hint"
				}
			case SQLIndexHintForce:
				if !strings.EqualFold(candidate.field, hint.Field) {
					reason = "not selected by forced index hint"
				}
			}
		}
		if reason == "" {
			if selected < 0 {
				selected = index
				alternative.Selected = true
			} else if candidate.cost > candidates[selected].cost {
				reason = "higher estimated cost"
			} else {
				reason = "stable tie-break after earlier candidate"
			}
		}
		if reason != "" {
			alternative.RejectedReason = reason
			notices = append(notices, SQLExplainNotice{
				Code:   "optimizer_alternative_rejected",
				Detail: candidate.expression + ": " + reason,
			})
		}
		alternatives = append(alternatives, alternative)
	}
	return alternatives, notices
}

func sqlExplainIndexedEqualityField(source sqlSource, condition sqlExpr) (string, bool) {
	if source.kind != "CACHE" || condition.kind != "binary" || condition.op != "=" || condition.left == nil || condition.right == nil {
		return "", false
	}
	left, right := *condition.left, *condition.right
	if left.kind == "field" && left.qualifier == source.alias && right.kind == "literal" {
		return left.name, true
	}
	if right.kind == "field" && right.qualifier == source.alias && left.kind == "literal" {
		return right.name, true
	}
	return "", false
}

func sqlExplainHasIndexStrategy(steps []SQLExplainStep) bool {
	for _, step := range steps {
		if len(step.Alternatives) > 0 || len(step.Notices) > 0 {
			return true
		}
	}
	return false
}
