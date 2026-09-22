package hatSql

import (
	"fmt"
	"reflect"
)

// SQLQueryOptimizationContext is the read-only query shape presented to an
// optimizer rule. Rules may select an existing planner control through
// IndexHint; they cannot mutate SQL text or the private parsed query tree.
// Plan is derived before execution and is not used as an execution result.
type SQLQueryOptimizationContext struct {
	Source    string
	Plan      []SQLExplainStep
	IndexHint SQLIndexHint

	trace *SQLQueryOptimizerTrace
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

// RejectAlternative records a strategy that the current rule considered but
// deliberately did not select. It is a no-op unless OptimizerTrace is enabled
// on the query options.
func (context *SQLQueryOptimizationContext) RejectAlternative(expression, reason string) {
	if context == nil || context.trace == nil {
		return
	}
	context.trace.pending = append(context.trace.pending, SQLQueryOptimizerTraceEvent{
		Rule:       context.trace.rule,
		Kind:       "alternative",
		Action:     "rejected",
		Expression: expression,
		Reason:     reason,
	})
}

func applySQLQueryOptimizerRules(source string, query *sqlQuery, options SQLQueryOptions) (SQLIndexHint, *SQLQueryOptimizerTrace, error) {
	if options.Optimizer == nil || len(options.Optimizer.rules) == 0 {
		return options.IndexHint, nil, nil
	}
	var trace *SQLQueryOptimizerTrace
	if options.OptimizerTrace {
		trace = &SQLQueryOptimizerTrace{
			Format: SQLQueryOptimizerTraceFormat,
			Events: make([]SQLQueryOptimizerTraceEvent, 0, len(options.Optimizer.rules)),
		}
	}
	context := &SQLQueryOptimizationContext{
		Source:    source,
		Plan:      sqlExplainSteps(query),
		IndexHint: options.IndexHint,
		trace:     trace,
	}
	for index, rule := range options.Optimizer.rules {
		if rule == nil {
			continue
		}
		if trace != nil {
			trace.rule = index + 1
		}
		before := context.IndexHint
		if err := rule(context); err != nil {
			if trace != nil {
				trace.Events = append(trace.Events, SQLQueryOptimizerTraceEvent{
					Rule:       index + 1,
					Kind:       "rule",
					Action:     "rejected",
					Expression: fmt.Sprintf("rule_%d", index+1),
					Reason:     err.Error(),
				})
				trace.Events = append(trace.Events, trace.pending...)
				trace.pending = trace.pending[:0]
			}
			return SQLIndexHint{}, trace, fmt.Errorf("SQL optimizer rule %d: %w", index+1, err)
		}
		if trace != nil {
			trace.Events = append(trace.Events, SQLQueryOptimizerTraceEvent{
				Rule:       index + 1,
				Kind:       "rule",
				Action:     "applied",
				Expression: fmt.Sprintf("rule_%d", index+1),
				Changed:    !reflect.DeepEqual(before, context.IndexHint),
			})
			trace.Events = append(trace.Events, trace.pending...)
			trace.pending = trace.pending[:0]
		}
	}
	if err := context.IndexHint.validate(); err != nil {
		return SQLIndexHint{}, trace, fmt.Errorf("SQL optimizer rule output: %w", err)
	}
	if options.IndexHint.Mode != "" {
		return options.IndexHint, trace, nil
	}
	return context.IndexHint, trace, nil
}
