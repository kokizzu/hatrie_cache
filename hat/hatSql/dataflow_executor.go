package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrSQLDataflowExecutorNil            = errors.New("sql dataflow executor is nil")
	ErrSQLDataflowContextRequired        = errors.New("sql dataflow execution context is required")
	ErrSQLDataflowFragmentRunnerRequired = errors.New("sql dataflow fragment runner is required")
	ErrSQLDataflowPlanInvalid            = errors.New("sql dataflow plan is invalid")
)

// SQLDataflowFragmentInputs exposes the already-produced outputs of a
// fragment's inputs without allocating a per-fragment input slice. The
// returned rows and fragment metadata are owned by the caller and must be
// treated as read-only by a runner.
type SQLDataflowFragmentInputs struct {
	initial []SQLRow
	outputs [][]SQLRow
	ids     []int
}

// Initial returns the input supplied to Execute. A source fragment normally
// consumes this value; it is also available to runners for custom plans.
func (inputs SQLDataflowFragmentInputs) Initial() []SQLRow {
	return inputs.initial
}

// Len returns the number of upstream fragments for the current fragment.
func (inputs SQLDataflowFragmentInputs) Len() int {
	return len(inputs.ids)
}

// FragmentID returns an upstream fragment ID, or -1 for an invalid index.
func (inputs SQLDataflowFragmentInputs) FragmentID(index int) int {
	if index < 0 || index >= len(inputs.ids) {
		return -1
	}
	return inputs.ids[index]
}

// Rows returns the output rows produced by an upstream fragment, or nil for
// an invalid index.
func (inputs SQLDataflowFragmentInputs) Rows(index int) []SQLRow {
	if index < 0 || index >= len(inputs.ids) {
		return nil
	}
	return inputs.outputs[inputs.ids[index]]
}

// SQLDataflowFragmentRunner executes one lowered fragment. The fragment and
// its input view must be treated as immutable; the runner owns the returned
// row slice and may reuse it on the next execution if it remains isolated.
type SQLDataflowFragmentRunner func(context.Context, SQLDataflowFragment, SQLDataflowFragmentInputs) ([]SQLRow, error)

// SQLDataflowExecutor runs a validated reusable fragment plan. It does not
// replace the SQL executor: callers supply the semantics for each fragment,
// which lets typed or differential operators share one plan without making
// the existing SQL execution path pay for this boundary.
type SQLDataflowExecutor struct {
	plan        SQLDataflowPlan
	runner      SQLDataflowFragmentRunner
	nativeQuery *sqlQuery
}

// CompileSQLDataflow validates and snapshots a reusable fragment plan.
func CompileSQLDataflow(plan SQLDataflowPlan, runner SQLDataflowFragmentRunner) (*SQLDataflowExecutor, error) {
	if runner == nil {
		return nil, ErrSQLDataflowFragmentRunnerRequired
	}
	if err := validateSQLDataflowPlan(plan); err != nil {
		return nil, err
	}
	return &SQLDataflowExecutor{
		plan:   cloneSQLDataflowPlan(plan),
		runner: runner,
	}, nil
}

// CompileDataflow lowers a compiled SQL query and binds a caller-owned
// fragment runner to the resulting reusable plan.
func (query *CompiledSQLQuery) CompileDataflow(runner SQLDataflowFragmentRunner) (*SQLDataflowExecutor, error) {
	if query == nil || query.template == nil {
		return nil, fmt.Errorf("compiled SQL query is required")
	}
	return CompileSQLDataflow(query.LowerDataflow(), runner)
}

// Plan returns an independent snapshot of the executor's validated plan.
func (executor *SQLDataflowExecutor) Plan() SQLDataflowPlan {
	if executor == nil {
		return SQLDataflowPlan{Format: sqlDataflowPlanFormat, Root: -1}
	}
	return cloneSQLDataflowPlan(executor.plan)
}

// Execute runs every fragment in validated ID order and returns the root
// output. The runner controls operator semantics; this method only supplies
// deterministic wiring, input views, and cancellation/error boundaries.
func (executor *SQLDataflowExecutor) Execute(ctx context.Context, initial []SQLRow) ([]SQLRow, error) {
	if executor == nil {
		return nil, ErrSQLDataflowExecutorNil
	}
	if ctx == nil {
		return nil, ErrSQLDataflowContextRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if executor.nativeQuery != nil {
		return executeNativeSQLDataflow(ctx, executor.nativeQuery, initial)
	}
	if executor.runner == nil {
		return nil, ErrSQLDataflowFragmentRunnerRequired
	}
	if len(executor.plan.Fragments) == 0 {
		return nil, nil
	}

	outputs := make([][]SQLRow, len(executor.plan.Fragments))
	for index, fragment := range executor.plan.Fragments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inputs := SQLDataflowFragmentInputs{
			initial: initial,
			outputs: outputs,
			ids:     fragment.Inputs,
		}
		rows, err := executor.runner(ctx, fragment, inputs)
		if err != nil {
			return nil, fmt.Errorf("sql dataflow fragment %d (%s): %w", fragment.ID, fragment.Kind, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		outputs[index] = rows
	}
	return outputs[executor.plan.Root], nil
}

func validateSQLDataflowPlan(plan SQLDataflowPlan) error {
	if plan.Format != sqlDataflowPlanFormat {
		return fmt.Errorf("%w: format %q", ErrSQLDataflowPlanInvalid, plan.Format)
	}
	if len(plan.Fragments) == 0 {
		if plan.Root != -1 {
			return fmt.Errorf("%w: empty plan root %d", ErrSQLDataflowPlanInvalid, plan.Root)
		}
		return nil
	}
	if plan.Root < 0 || plan.Root >= len(plan.Fragments) {
		return fmt.Errorf("%w: root %d", ErrSQLDataflowPlanInvalid, plan.Root)
	}
	for index, fragment := range plan.Fragments {
		if fragment.ID != index || strings.TrimSpace(fragment.Kind) == "" {
			return fmt.Errorf("%w: fragment %d has id=%d kind=%q", ErrSQLDataflowPlanInvalid, index, fragment.ID, fragment.Kind)
		}
		for _, input := range fragment.Inputs {
			if input < 0 || input >= index {
				return fmt.Errorf("%w: fragment %d input %d", ErrSQLDataflowPlanInvalid, index, input)
			}
		}
	}
	return nil
}
