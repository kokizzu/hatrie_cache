package hatSql

import (
	"context"
	"fmt"
	"strings"
)

// PlanGuard describes a plan property that must remain true for a known query.
// It is intended for deterministic CI tests that protect a deliberate index or
// join strategy from accidental planner regressions.
type PlanGuard struct {
	Name          string
	Query         string
	RequireNode   string
	RequireDetail string
}

// VerifyPlanGuards executes every guard through EXPLAIN ANALYZE. It returns an
// error at the first missing plan requirement so callers can fail CI with the
// named query and the observed plan.
func VerifyPlanGuards(ctx context.Context, resolver SourceResolver, options QueryOptions, guards []PlanGuard) error {
	for _, guard := range guards {
		name := strings.TrimSpace(guard.Name)
		if name == "" {
			name = "unnamed query"
		}
		query := strings.TrimSpace(guard.Query)
		if query == "" {
			return fmt.Errorf("SQL plan guard %q requires a query", name)
		}
		upper := strings.ToUpper(query)
		if !strings.HasPrefix(upper, "EXPLAIN ") {
			query = "EXPLAIN ANALYZE " + query
		}
		result, err := ExecuteQueryParameters(ctx, query, resolver, nil, options)
		if err != nil {
			return fmt.Errorf("SQL plan guard %q: %w", name, err)
		}
		if !sqlPlanHasRequirement(result.Plan, guard.RequireNode, guard.RequireDetail) {
			return fmt.Errorf("SQL plan guard %q missing required plan node %q detail %q; observed plan: %s", name, guard.RequireNode, guard.RequireDetail, formatPlanGuardSteps(result.Plan))
		}
	}
	return nil
}

func sqlPlanHasRequirement(steps []ExplainStep, requireNode, requireDetail string) bool {
	requireNode = strings.TrimSpace(requireNode)
	requireDetail = strings.TrimSpace(requireDetail)
	for _, step := range steps {
		if requireNode != "" && !strings.Contains(strings.ToUpper(step.Node), strings.ToUpper(requireNode)) {
			continue
		}
		if requireDetail != "" && !strings.Contains(strings.ToUpper(step.Detail), strings.ToUpper(requireDetail)) {
			continue
		}
		return true
	}
	return false
}

func formatPlanGuardSteps(steps []ExplainStep) string {
	if len(steps) == 0 {
		return "(empty)"
	}
	parts := make([]string, 0, len(steps))
	for _, step := range steps {
		if step.Detail == "" {
			parts = append(parts, step.Node)
			continue
		}
		parts = append(parts, step.Node+"("+step.Detail+")")
	}
	return strings.Join(parts, "; ")
}
