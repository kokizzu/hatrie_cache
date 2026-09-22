package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestM064RecursiveDataflowComputesOrderedFixedPoint(t *testing.T) {
	edges := map[string][]string{
		"a": {"b", "c"},
		"b": {"c", "d"},
		"c": {"d"},
		"d": {"a"},
	}
	flow, err := NewSQLRecursiveDataflow([]string{"a"}, func(_ context.Context, _ []string, delta []string) ([]string, error) {
		var next []string
		for _, source := range delta {
			next = append(next, edges[source]...)
		}
		return next, nil
	}, SQLRecursiveDataflowOptions{})
	if err != nil {
		t.Fatalf("NewSQLRecursiveDataflow() error = %v", err)
	}
	got, err := flow.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Run() = %#v, want %#v", got, want)
	}
	got[0] = "mutated"
	again, err := flow.Run(context.Background())
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("second Run() = %#v/%v, want independent %#v", again, err, want)
	}
}

func TestM064RecursiveDataflowEnforcesBoundsAndCancellation(t *testing.T) {
	step := func(_ context.Context, _ []int, delta []int) ([]int, error) {
		next := make([]int, 0, len(delta))
		for _, value := range delta {
			next = append(next, value+1)
		}
		return next, nil
	}
	for name, options := range map[string]SQLRecursiveDataflowOptions{
		"row limit":       {MaxRows: 2},
		"iteration limit": {MaxIterations: 1},
	} {
		flow, err := NewSQLRecursiveDataflow([]int{0}, step, options)
		if err != nil {
			t.Fatalf("%s NewSQLRecursiveDataflow() error = %v", name, err)
		}
		if _, err := flow.Run(context.Background()); !errors.Is(err, ErrSQLRecursiveDataflowLimit) {
			t.Fatalf("%s Run() error = %v, want limit", name, err)
		}
	}

	flow, err := NewSQLRecursiveDataflow([]int{0}, func(ctx context.Context, _ []int, _ []int) ([]int, error) {
		return nil, ctx.Err()
	}, SQLRecursiveDataflowOptions{})
	if err != nil {
		t.Fatalf("cancellation flow error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := flow.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Run() error = %v, want context.Canceled", err)
	}
}

func TestM064RecursiveDataflowRejectsInvalidInputs(t *testing.T) {
	step := func(context.Context, []int, []int) ([]int, error) { return nil, nil }
	for name, options := range map[string]SQLRecursiveDataflowOptions{
		"negative iterations": {MaxIterations: -1},
		"negative rows":       {MaxRows: -1},
	} {
		if _, err := NewSQLRecursiveDataflow([]int{1}, step, options); !errors.Is(err, ErrSQLRecursiveDataflowInvalid) {
			t.Fatalf("%s error = %v, want invalid", name, err)
		}
	}
	if _, err := NewSQLRecursiveDataflow([]int{1}, nil, SQLRecursiveDataflowOptions{}); !errors.Is(err, ErrSQLRecursiveDataflowInvalid) {
		t.Fatalf("nil step error = %v, want invalid", err)
	}
	var nilFlow *SQLRecursiveDataflow[int]
	if _, err := nilFlow.Run(context.Background()); !errors.Is(err, ErrSQLRecursiveDataflowInvalid) {
		t.Fatalf("nil flow error = %v, want invalid", err)
	}
}
