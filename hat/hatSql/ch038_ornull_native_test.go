package hatSql

import (
	"context"
	"reflect"
	"testing"
)

var ch038OrNullNativeSink SQLQueryResult

func TestCH038OrNullNativeAggregateDispatch(t *testing.T) {
	arguments := []sqlExpr{{kind: "field", name: "amount"}}
	for _, name := range []string{"SUM_OR_NULL", "AVG_OR_NULL", "MIN_OR_NULL", "MAX_OR_NULL"} {
		aggregate, ok := nativeSQLDataflowAggregateExpression(sqlExpr{kind: "func", name: name, args: arguments})
		if !ok {
			t.Fatalf("native aggregate %s was rejected", name)
		}
		if aggregate.arg == nil {
			t.Fatalf("native aggregate %s lost its argument", name)
		}
	}
	aggregate, ok := nativeSQLDataflowAggregateExpression(sqlExpr{kind: "func", name: "COUNT_OR_NULL", args: []sqlExpr{{kind: "star"}}})
	if !ok {
		t.Fatal("native aggregate COUNT_OR_NULL was rejected")
	}
	if aggregate.arg != nil {
		t.Fatalf("COUNT_OR_NULL(*) unexpectedly retained an argument: %#v", aggregate.arg)
	}
}

func TestCH038OrNullNativeMatchesFallbackAndPreservesEmptyNulls(t *testing.T) {
	rows := []SQLRow{
		{"amount": int64(2), "active": true},
		{"amount": nil, "active": true},
		{"amount": int64(5), "active": true},
		{"amount": int64(9), "active": false},
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	query := "FROM CACHE('items') AS src SELECT COUNT_OR_NULL(*) AS count_value, COUNT_OR_NULL(src.amount) AS count_amount, SUM_OR_NULL(src.amount) AS sum_value, AVG_OR_NULL(src.amount) AS average_value, MIN_OR_NULL(src.amount) AS minimum_value, MAX_OR_NULL(src.amount) AS maximum_value WHERE src.active"
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic OrNull query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback OrNull query: %v", err)
	}
	want := []SQLRow{{
		"count_value":   int64(3),
		"count_amount":  int64(2),
		"sum_value":     float64(7),
		"average_value": float64(3.5),
		"minimum_value": float64(2),
		"maximum_value": float64(5),
	}}
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

	emptyQuery := "FROM CACHE('items') AS src SELECT COUNT_OR_NULL(*) AS count_value, SUM_OR_NULL(src.amount) AS sum_value, AVG_OR_NULL(src.amount) AS average_value, MIN_OR_NULL(src.amount) AS minimum_value, MAX_OR_NULL(src.amount) AS maximum_value WHERE src.active = true AND src.active = false"
	empty, err := ExecuteSQLQueryContext(context.Background(), emptyQuery, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic empty OrNull query: %v", err)
	}
	emptyWant := []SQLRow{{
		"count_value":   nil,
		"sum_value":     nil,
		"average_value": nil,
		"minimum_value": nil,
		"maximum_value": nil,
	}}
	if !reflect.DeepEqual(empty.Rows, emptyWant) {
		t.Fatalf("empty automatic rows = %#v, want %#v", empty.Rows, emptyWant)
	}
	if !m052qPlanHasNode(empty.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("empty automatic plan = %#v, want NATIVE DATAFLOW", empty.Plan)
	}
}

func TestCH038OrNullNativeGroupedMatchesFallback(t *testing.T) {
	rows := []SQLRow{
		{"bucket": "a", "amount": int64(2)},
		{"bucket": "a", "amount": nil},
		{"bucket": "b", "amount": nil},
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	query := "FROM CACHE('items') AS src SELECT src.bucket, COUNT_OR_NULL(*) AS count_value, SUM_OR_NULL(src.amount) AS sum_value, AVG_OR_NULL(src.amount) AS average_value, MIN_OR_NULL(src.amount) AS minimum_value, MAX_OR_NULL(src.amount) AS maximum_value GROUP BY src.bucket"
	auto, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic grouped OrNull query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback grouped OrNull query: %v", err)
	}
	want := []SQLRow{
		{"bucket": "a", "count_value": int64(2), "sum_value": float64(2), "average_value": float64(2), "minimum_value": float64(2), "maximum_value": float64(2)},
		{"bucket": "b", "count_value": int64(1), "sum_value": nil, "average_value": nil, "minimum_value": nil, "maximum_value": nil},
	}
	if !reflect.DeepEqual(auto.Rows, want) {
		t.Fatalf("automatic grouped rows = %#v, want %#v", auto.Rows, want)
	}
	if !reflect.DeepEqual(fallback.Rows, want) {
		t.Fatalf("fallback grouped rows = %#v, want %#v", fallback.Rows, want)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic grouped rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic grouped plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func BenchmarkCH038AutomaticNativeOrNull(b *testing.B) {
	rows := make([]SQLRow, 20_000)
	for index := range rows {
		rows[index] = SQLRow{
			"amount": int64(index % 97),
			"active": index%5 != 0,
		}
	}
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT COUNT_OR_NULL(*) AS count_value, SUM_OR_NULL(src.amount) AS sum_value, AVG_OR_NULL(src.amount) AS average_value, MIN_OR_NULL(src.amount) AS minimum_value, MAX_OR_NULL(src.amount) AS maximum_value WHERE src.active")
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
				ch038OrNullNativeSink = result
			}
		})
	}
}
