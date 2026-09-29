package hatSql

import "fmt"

// SQLQueryOptimizationContext is the read-only query shape presented to an
// optimizer rule. Rules may select an existing planner control through
// IndexHint; they cannot mutate SQL text or the private parsed query tree.
// Plan is derived before execution and is not used as an execution result.
type SQLQueryOptimizationContext struct {
	Source    string
	Plan      []SQLExplainStep
	IndexHint SQLIndexHint
}

// SQLQueryOptimizerRule is one opt-in planning rule. Rules run in slice order;
// a nil rule is ignored. Returning an error aborts the query before any source
// is resolved or index is used.
type SQLQueryOptimizerRule func(*SQLQueryOptimizationContext) error

// SQLQueryOptimizer owns an ordered, immutable rule list for one or more
// queries. Construct it once and share it safely between concurrent callers.
type SQLQueryOptimizer struct {
	rules []SQLQueryOptimizerRule
}

// NewSQLQueryOptimizer creates an optimizer with a private copy of rules.
func NewSQLQueryOptimizer(rules ...SQLQueryOptimizerRule) *SQLQueryOptimizer {
	return &SQLQueryOptimizer{rules: append([]SQLQueryOptimizerRule(nil), rules...)}
}

// QueryOptimizer is the package-native alias for SQLQueryOptimizer.
type QueryOptimizer = SQLQueryOptimizer

// QueryOptimizationContext is the package-native alias for
// SQLQueryOptimizationContext.
type QueryOptimizationContext = SQLQueryOptimizationContext

// QueryOptimizerRule is the package-native alias for SQLQueryOptimizerRule.
type QueryOptimizerRule = SQLQueryOptimizerRule

func applySQLQueryOptimizerRules(source string, query *sqlQuery, options SQLQueryOptions) (SQLIndexHint, error) {
	if options.Optimizer == nil || len(options.Optimizer.rules) == 0 {
		if options.optimizerTrace != nil {
			options.optimizerTrace.setFinalHint(options.IndexHint)
		}
		return options.IndexHint, nil
	}
	context := &SQLQueryOptimizationContext{
		Source:    source,
		Plan:      sqlExplainSteps(query),
		IndexHint: options.IndexHint,
	}
	for index, rule := range options.Optimizer.rules {
		before := context.IndexHint
		if rule == nil {
			appendSQLOptimizerTraceRule(options.optimizerTrace, index+1, "skipped", before, before)
			continue
		}
		if err := rule(context); err != nil {
			appendSQLOptimizerTraceRule(options.optimizerTrace, index+1, "error", before, context.IndexHint)
			if options.optimizerTrace != nil && index+1 < len(options.Optimizer.rules) && len(options.optimizerTrace.Entries) >= options.optimizerTrace.maxEntries {
				options.optimizerTrace.Truncated = true
			}
			return SQLIndexHint{}, fmt.Errorf("SQL optimizer rule %d: %w", index+1, err)
		}
		status := "no_change"
		if context.IndexHint != before {
			status = "applied"
		}
		appendSQLOptimizerTraceRule(options.optimizerTrace, index+1, status, before, context.IndexHint)
	}
	if err := context.IndexHint.validate(); err != nil {
		return SQLIndexHint{}, fmt.Errorf("SQL optimizer rule output: %w", err)
	}
	if options.IndexHint.Mode != "" {
		if options.optimizerTrace != nil {
			options.optimizerTrace.setFinalHint(options.IndexHint)
		}
		return options.IndexHint, nil
	}
	if options.optimizerTrace != nil {
		options.optimizerTrace.setFinalHint(context.IndexHint)
	}
	return context.IndexHint, nil
}
