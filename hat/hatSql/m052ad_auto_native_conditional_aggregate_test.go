package hatSql

import (
	"context"
	"reflect"
	"testing"
)

var m052adConditionalAggregateSink SQLQueryResult

func TestCompiledSQLAutomaticNativeConditionalAggregates(t *testing.T) {
	rows := []SQLRow{
		{"bucket": "a", "amount": int64(1), "active": true},
		{"bucket": "a", "amount": int64(3), "active": false},
		{"bucket": "a", "amount": int64(5), "active": true},
		{"bucket": "b", "amount": int64(7), "active": false},
		{"bucket": "b", "amount": int64(11), "active": true},
	}
	query := "FROM CACHE('items') AS src SELECT src.bucket, COUNT_IF(src.active) AS count_true, COUNTIF(src.active) AS count_compact, SUM_IF(src.amount, src.active) AS sum_true, AVG_IF(src.amount, src.active) AS avg_true, MIN_IF(src.amount, src.active) AS min_true, MAX_IF(src.amount, src.active) AS max_true GROUP BY src.bucket"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		t.Fatalf("compile conditional aggregate query: %v", err)
	}
	if _, err := compiled.CompileNativeDataflow(); err != nil {
		t.Fatalf("compile native conditional aggregate query: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic conditional aggregate query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("conditional aggregate fallback: %v", err)
	}
	want := []SQLRow{
		{"bucket": "a", "count_true": int64(2), "count_compact": int64(2), "sum_true": float64(6), "avg_true": float64(3), "min_true": float64(1), "max_true": float64(5)},
		{"bucket": "b", "count_true": int64(1), "count_compact": int64(1), "sum_true": float64(11), "avg_true": float64(11), "min_true": float64(11), "max_true": float64(11)},
	}
	if !reflect.DeepEqual(auto.Rows, want) {
		t.Fatalf("automatic rows = %#v, want %#v", auto.Rows, want)
	}
	if !reflect.DeepEqual(fallback.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", fallback.Rows, want)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLAutomaticNativeConditionalAggregateKeepsExplicitFilterOnFallback(t *testing.T) {
	rows := []SQLRow{
		{"bucket": "a", "active": true},
		{"bucket": "a", "active": false},
		{"bucket": "b", "active": true},
	}
	query := "FROM CACHE('items') AS src SELECT src.bucket, COUNT(*) FILTER (WHERE src.active) AS count_true GROUP BY src.bucket"
	result, err := ExecuteSQLQueryContext(context.Background(), query, SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	}), SQLQueryOptions{})
	if err != nil {
		t.Fatalf("explicit aggregate filter query: %v", err)
	}
	want := []SQLRow{
		{"bucket": "a", "count_true": int64(1)},
		{"bucket": "b", "count_true": int64(1)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("explicit filter rows = %#v, want %#v", result.Rows, want)
	}
	if m052qPlanHasNode(result.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("explicit filter unexpectedly used automatic native dataflow: %#v", result.Plan)
	}
}

func BenchmarkCompiledSQLAutomaticNativeConditionalAggregates(b *testing.B) {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"bucket": index % 257,
			"amount": int64(index % 97),
			"active": index%3 != 0,
		}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.bucket, COUNT_IF(src.active) AS count_true, SUM_IF(src.amount, src.active) AS sum_true GROUP BY src.bucket")
	if err != nil {
		b.Fatal(err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	for _, variant := range []struct {
		name    string
		options SQLQueryOptions
	}{
		{name: "fallback", options: SQLQueryOptions{DisableNativeDataflow: true}},
		{name: "automatic", options: SQLQueryOptions{}},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := compiled.Execute(context.Background(), resolver, nil, variant.options)
				if err != nil {
					b.Fatal(err)
				}
				m052adConditionalAggregateSink = result
			}
		})
	}
}
