package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMZ039OperatorYieldFuelAndCancellation(t *testing.T) {
	ctx, cancelContext := context.WithCancel(context.Background())
	control, cancel, err := newSQLExecutionControl(ctx, SQLQueryOptions{OperatorYieldEvery: 2})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	for range 5 {
		if err := control.check(); err != nil {
			t.Fatalf("check() error = %v", err)
		}
	}
	if got := control.yields.Load(); got != 2 {
		t.Fatalf("yield count = %d, want 2", got)
	}
	cancelContext()
	cancel()
	if err := control.check(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled check() error = %v, want %v", err, context.Canceled)
	}
}

func TestMZ039OperatorYieldDisabledByDefault(t *testing.T) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()
	for range 5 {
		if err := control.check(); err != nil {
			t.Fatalf("check() error = %v", err)
		}
	}
	if got := control.yields.Load(); got != 0 {
		t.Fatalf("disabled yield count = %d, want 0", got)
	}
}

func TestMZ039OperatorYieldValidatesAndPreservesResults(t *testing.T) {
	if _, _, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{OperatorYieldEvery: -1}); err == nil {
		t.Fatal("negative OperatorYieldEvery was accepted")
	}
	query := "FROM VALUES (1, 'east'), (2, 'west'), (3, 'east') AS src(id, region) SELECT src.id, src.region WHERE src.id > 0"
	baseline, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("baseline query error = %v", err)
	}
	yielded, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{OperatorYieldEvery: 1})
	if err != nil {
		t.Fatalf("yielded query error = %v", err)
	}
	if !reflect.DeepEqual(yielded, baseline) {
		t.Fatalf("yielded result = %#v, want baseline %#v", yielded, baseline)
	}
}

func TestMZ039OperatorYieldReachesNativeDataflow(t *testing.T) {
	query, resolver := mz039BenchmarkFixture(t)
	options := SQLQueryOptions{OperatorYieldEvery: 1}
	control, cancel, err := newSQLExecutionControl(context.Background(), options)
	if err != nil {
		t.Fatalf("newSQLExecutionControl() error = %v", err)
	}
	defer cancel()
	if _, handled, err := executeSQLAutoNativeDataflow(control.ctx, query, resolver, options, control, false); err != nil {
		t.Fatalf("executeSQLAutoNativeDataflow() error = %v", err)
	} else if !handled {
		t.Fatal("executeSQLAutoNativeDataflow() did not handle the eligible query")
	}
	if got := control.yields.Load(); got < 4 {
		t.Fatalf("native dataflow yield count = %d, want at least four operator yields", got)
	}
}
