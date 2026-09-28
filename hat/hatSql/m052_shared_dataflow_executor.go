package hatSql

import "fmt"

// CompileReusableDataflow binds a runner to the compiled query's memoized
// immutable fragment plan without cloning its fragment or input slices. The
// runner must treat the fragment metadata as read-only, just as it must for
// SQLDataflowExecutor.Execute. CompileDataflow remains the ownership-safe
// cloning API for callers that need an independent plan copy.
func (query *CompiledSQLQuery) CompileReusableDataflow(runner SQLDataflowFragmentRunner) (*SQLDataflowExecutor, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("compiled SQL query is required")
	}
	if runner == nil {
		return nil, ErrSQLDataflowFragmentRunnerRequired
	}
	query.dataflowOnce.Do(func() {
		query.dataflowPlan = buildSQLDataflowPlan(query.source, query.template)
	})
	if query.dataflowPlan == nil {
		return nil, ErrSQLDataflowPlanInvalid
	}
	if err := validateSQLDataflowPlan(*query.dataflowPlan); err != nil {
		return nil, err
	}
	return &SQLDataflowExecutor{
		plan:   *query.dataflowPlan,
		runner: runner,
	}, nil
}
