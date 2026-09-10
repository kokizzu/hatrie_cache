package hatSql

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestCompileSQLDataflowExecutesReusableFragments(t *testing.T) {
	plan := SQLDataflowPlan{
		Format: sqlDataflowPlanFormat,
		Root:   2,
		Fragments: []SQLDataflowFragment{
			{ID: 0, Kind: "SOURCE"},
			{ID: 1, Kind: "FILTER", Inputs: []int{0}},
			{ID: 2, Kind: "PROJECT", Inputs: []int{1}},
		},
	}
	seen := make([]string, 0, len(plan.Fragments))
	executor, err := CompileSQLDataflow(plan, func(_ context.Context, fragment SQLDataflowFragment, inputs SQLDataflowFragmentInputs) ([]SQLRow, error) {
		seen = append(seen, fragment.Kind)
		if fragment.ID == 0 {
			return inputs.Initial(), nil
		}
		if inputs.Len() != 1 || inputs.FragmentID(0) != fragment.ID-1 {
			return nil, fmt.Errorf("fragment %d inputs = %#v", fragment.ID, fragment.Inputs)
		}
		return inputs.Rows(0), nil
	})
	if err != nil {
		t.Fatalf("compile dataflow: %v", err)
	}
	input := []SQLRow{{"id": int64(1)}}
	rows, err := executor.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("execute dataflow: %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != int64(1) {
		t.Fatalf("rows = %#v, want input row", rows)
	}
	if got, want := seen, []string{"SOURCE", "FILTER", "PROJECT"}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("seen fragments = %#v, want %#v", got, want)
	}

	second, err := executor.Execute(context.Background(), []SQLRow{{"id": int64(2)}})
	if err != nil {
		t.Fatalf("reuse dataflow: %v", err)
	}
	if len(second) != 1 || second[0]["id"] != int64(2) {
		t.Fatalf("second rows = %#v, want second input row", second)
	}
	plan.Fragments[0].Kind = "MUTATED"
	if executor.Plan().Fragments[0].Kind != "SOURCE" {
		t.Fatalf("executor plan was not isolated from input mutation")
	}
	snapshot := executor.Plan()
	snapshot.Fragments[1].Inputs[0] = 99
	if executor.Plan().Fragments[1].Inputs[0] != 0 {
		t.Fatalf("executor plan snapshot shares input metadata")
	}
}

func TestCompileSQLDataflowValidatesPlansAndRunner(t *testing.T) {
	valid := SQLDataflowPlan{
		Format: sqlDataflowPlanFormat,
		Root:   1,
		Fragments: []SQLDataflowFragment{
			{ID: 0, Kind: "SOURCE"},
			{ID: 1, Kind: "PROJECT", Inputs: []int{0}},
		},
	}
	if _, err := CompileSQLDataflow(valid, nil); !errors.Is(err, ErrSQLDataflowFragmentRunnerRequired) {
		t.Fatalf("nil runner error = %v, want %v", err, ErrSQLDataflowFragmentRunnerRequired)
	}
	invalidPlans := []SQLDataflowPlan{
		{Format: "wrong", Root: -1},
		{Format: sqlDataflowPlanFormat, Root: 0, Fragments: []SQLDataflowFragment{{ID: 1, Kind: "SOURCE"}}},
		{Format: sqlDataflowPlanFormat, Root: 1, Fragments: []SQLDataflowFragment{{ID: 0, Kind: "SOURCE"}, {ID: 1, Kind: "FILTER", Inputs: []int{2}}}},
		{Format: sqlDataflowPlanFormat, Root: 3, Fragments: []SQLDataflowFragment{{ID: 0, Kind: "SOURCE"}}},
	}
	for index, plan := range invalidPlans {
		if _, err := CompileSQLDataflow(plan, func(context.Context, SQLDataflowFragment, SQLDataflowFragmentInputs) ([]SQLRow, error) {
			return nil, nil
		}); !errors.Is(err, ErrSQLDataflowPlanInvalid) {
			t.Errorf("invalid plan %d error = %v, want %v", index, err, ErrSQLDataflowPlanInvalid)
		}
	}
}

func TestSQLDataflowExecutorPropagatesCancellationAndRunnerErrors(t *testing.T) {
	plan := SQLDataflowPlan{
		Format:    sqlDataflowPlanFormat,
		Root:      0,
		Fragments: []SQLDataflowFragment{{ID: 0, Kind: "SOURCE"}},
	}
	runnerError := errors.New("runner failed")
	executor, err := CompileSQLDataflow(plan, func(context.Context, SQLDataflowFragment, SQLDataflowFragmentInputs) ([]SQLRow, error) {
		return nil, runnerError
	})
	if err != nil {
		t.Fatalf("compile dataflow: %v", err)
	}
	if _, err := executor.Execute(context.Background(), nil); !errors.Is(err, runnerError) {
		t.Fatalf("runner error = %v, want %v", err, runnerError)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := executor.Execute(canceled, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v, want %v", err, context.Canceled)
	}
}

func TestSQLDataflowExecutorChecksCancellationAfterRunner(t *testing.T) {
	plan := SQLDataflowPlan{
		Format:    sqlDataflowPlanFormat,
		Root:      0,
		Fragments: []SQLDataflowFragment{{ID: 0, Kind: "SOURCE"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	executor, err := CompileSQLDataflow(plan, func(context.Context, SQLDataflowFragment, SQLDataflowFragmentInputs) ([]SQLRow, error) {
		cancel()
		return nil, nil
	})
	if err != nil {
		t.Fatalf("compile dataflow: %v", err)
	}
	if _, err := executor.Execute(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-run cancellation error = %v, want %v", err, context.Canceled)
	}
}

func TestCompiledSQLQueryCompilesExecutableDataflow(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM VALUES (1) AS src(id) SELECT src.id")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var fragments int
	executor, err := compiled.CompileDataflow(func(_ context.Context, _ SQLDataflowFragment, inputs SQLDataflowFragmentInputs) ([]SQLRow, error) {
		fragments++
		if inputs.Len() == 0 {
			return inputs.Initial(), nil
		}
		return inputs.Rows(0), nil
	})
	if err != nil {
		t.Fatalf("compile executable dataflow: %v", err)
	}
	if _, err := executor.Execute(context.Background(), []SQLRow{{"id": int64(1)}}); err != nil {
		t.Fatalf("execute compiled dataflow: %v", err)
	}
	if fragments == 0 {
		t.Fatal("compiled dataflow did not execute any fragments")
	}
}
