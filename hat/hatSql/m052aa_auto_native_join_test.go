package hatSql

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func TestM052AANativeJoinMatchesFallbackAndIsSelected(t *testing.T) {
	left := []SQLRow{
		{"id": int64(1), "k": "a"},
		{"id": int64(2), "k": "b"},
		{"id": int64(3), "k": "a"},
		{"id": int64(4), "k": nil},
	}
	right := []SQLRow{
		{"k": "a", "value": "x"},
		{"k": "a", "value": "y"},
		{"k": "b", "value": "z"},
		{"k": nil, "value": "null-never-matches"},
	}
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		switch key {
		case "left":
			return left, nil
		case "right":
			return right, nil
		default:
			t.Fatalf("unexpected source key %q", key)
			return nil, nil
		}
	})
	compiled, err := CompileSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}

	var event SQLQueryEvent
	automatic, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("automatic native join: %v", err)
	}
	fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback join: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	want := []SQLRow{
		{"id": int64(1), "value": "x"},
		{"id": int64(1), "value": "y"},
		{"id": int64(2), "value": "z"},
		{"id": int64(3), "value": "x"},
		{"id": int64(3), "value": "y"},
	}
	if !reflect.DeepEqual(automatic.Rows, want) {
		t.Fatalf("join rows = %#v, want %#v", automatic.Rows, want)
	}
	if !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", event.Operators)
	}
}

func TestM052AANativeJoinMatchesFallbackWithFilterAndProjection(t *testing.T) {
	left := []SQLRow{
		{"id": int64(1), "k": int64(1)},
		{"id": int64(2), "k": int64(2)},
		{"id": int64(3), "k": int64(1)},
	}
	right := []SQLRow{
		{"k": int64(1), "value": int64(4)},
		{"k": int64(1), "value": int64(8)},
		{"k": int64(2), "value": int64(2)},
	}
	resolver := m052AANativeJoinResolver(t, left, right)
	compiled, err := CompileSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k WHERE r.value >= 4 SELECT l.id, l.id + r.value AS total")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var event SQLQueryEvent
	automatic, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) { event = observed }),
	})
	if err != nil {
		t.Fatalf("automatic filtered join: %v", err)
	}
	fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback filtered join: %v", err)
	}
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	want := []SQLRow{
		{"id": int64(1), "total": int64(5)},
		{"id": int64(1), "total": int64(9)},
		{"id": int64(3), "total": int64(7)},
		{"id": int64(3), "total": int64(11)},
	}
	if !reflect.DeepEqual(automatic.Rows, want) {
		t.Fatalf("filtered join rows = %#v, want %#v", automatic.Rows, want)
	}
	if !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("filtered join operators = %#v, want NATIVE DATAFLOW", event.Operators)
	}
}

func TestM052AANativeJoinFallsBackForUnsupportedShapes(t *testing.T) {
	resolver := m052AANativeJoinResolver(t,
		[]SQLRow{{"id": int64(1), "k": "a"}},
		[]SQLRow{{"k": "a", "value": "x"}},
	)
	queries := []string{
		"FROM CACHE('left') AS l LEFT JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value",
		"FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value ORDER BY l.id LIMIT 1",
	}
	for _, source := range queries {
		t.Run(source, func(t *testing.T) {
			compiled, err := CompileSQLQuery(source)
			if err != nil {
				t.Fatalf("compile SQL: %v", err)
			}
			var event SQLQueryEvent
			automatic, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
				Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) { event = observed }),
			})
			if err != nil {
				t.Fatalf("automatic fallback: %v", err)
			}
			fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil {
				t.Fatalf("disabled fallback: %v", err)
			}
			if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
				t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
			}
			if sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
				t.Fatalf("unsupported shape unexpectedly used native dataflow: %#v", event.Operators)
			}
		})
	}
}

func TestM052AANativeJoinTypedKeysMatchFallback(t *testing.T) {
	left := []SQLRow{
		{"id": "zero", "k": math.Copysign(0, -1)},
		{"id": "number", "k": int64(1)},
		{"id": "boolean", "k": true},
		{"id": "string", "k": "1"},
	}
	right := []SQLRow{
		{"k": float64(0), "value": "zero"},
		{"k": float64(1), "value": "number"},
		{"k": true, "value": "boolean"},
		{"k": "1", "value": "string"},
	}
	resolver := m052AANativeJoinResolver(t, left, right)
	compiled, err := CompileSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	var event SQLQueryEvent
	automatic, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) { event = observed }),
	})
	if err != nil {
		t.Fatalf("automatic typed join: %v", err)
	}
	fallback, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback typed join: %v", err)
	}
	if !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic typed rows = %#v, fallback = %#v", automatic.Rows, fallback.Rows)
	}
	if !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("typed join operators = %#v, want NATIVE DATAFLOW", event.Operators)
	}
}

func TestM052AANativeJoinHonorsContextAndBudgets(t *testing.T) {
	resolver := m052AANativeJoinResolver(t,
		[]SQLRow{{"id": int64(1), "k": "a"}, {"id": int64(2), "k": "a"}},
		[]SQLRow{{"k": "a", "value": int64(1)}, {"k": "a", "value": int64(2)}},
	)
	compiled, err := CompileSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := compiled.Execute(canceled, resolver, nil, SQLQueryOptions{}); err == nil {
		t.Fatal("canceled native join unexpectedly succeeded")
	}
	if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{MaxRows: 3}); err == nil {
		t.Fatal("native join exceeded MaxRows without an error")
	}
	if _, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{MaxJoinWork: 1}); err == nil {
		t.Fatal("native join exceeded MaxJoinWork without an error")
	}
}

func m052AANativeJoinResolver(t *testing.T, left, right []SQLRow) SQLSourceResolver {
	t.Helper()
	return SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		switch key {
		case "left":
			return left, nil
		case "right":
			return right, nil
		default:
			t.Fatalf("unexpected source key %q", key)
			return nil, nil
		}
	})
}

var m052AANativeJoinBenchmarkSink SQLQueryResult

func BenchmarkCompiledSQLAutomaticNativeJoinFallback(b *testing.B) {
	compiled, resolver := m052AANativeJoinBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{DisableNativeDataflow: true})
		if err != nil {
			b.Fatal(err)
		}
		m052AANativeJoinBenchmarkSink = result
	}
}

func BenchmarkCompiledSQLAutomaticNativeJoin(b *testing.B) {
	compiled, resolver := m052AANativeJoinBenchmarkSetup(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		m052AANativeJoinBenchmarkSink = result
	}
}

func m052AANativeJoinBenchmarkSetup(b *testing.B) (*CompiledSQLQuery, SQLSourceResolver) {
	b.Helper()
	const (
		leftCount  = 4096
		rightCount = 256
		keyCount   = 128
	)
	left := make([]SQLRow, leftCount)
	for index := range left {
		left[index] = SQLRow{"id": int64(index), "k": int64(index % keyCount)}
	}
	right := make([]SQLRow, rightCount)
	for index := range right {
		right[index] = SQLRow{"k": int64(index % keyCount), "value": int64(index)}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('left') AS l JOIN CACHE('right') AS r ON l.k = r.k SELECT l.id, r.value")
	if err != nil {
		b.Fatalf("compile benchmark SQL: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(_ string, key string) ([]SQLRow, error) {
		if key == "left" {
			return left, nil
		}
		return right, nil
	})
	return compiled, resolver
}
