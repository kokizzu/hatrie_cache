package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestCompiledSQLAutomaticNativeGroupedApproximateAggregates(t *testing.T) {
	rows := []SQLRow{
		{"region": "us", "visitor": "a", "latency": 10.0},
		{"region": "us", "visitor": "a", "latency": 20.0},
		{"region": "us", "visitor": "b", "latency": 30.0},
		{"region": "eu", "visitor": "x", "latency": 100.0},
		{"region": "eu", "visitor": "y", "latency": 200.0},
		{"region": "eu", "visitor": "x", "latency": 300.0},
	}
	query := "FROM CACHE('events') AS src SELECT src.region, APPROX_COUNT_DISTINCT(src.visitor, 10) AS visitors, APPROX_PERCENTILE(src.latency, 0.5, 0.01) AS p50 GROUP BY src.region"
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	var automaticEvent SQLQueryEvent
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
			automaticEvent = event
		}),
	})
	if err != nil {
		t.Fatalf("automatic grouped approximate query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback grouped approximate query: %v", err)
	}
	sort.Slice(automatic.Rows, func(i, j int) bool {
		return automatic.Rows[i]["region"].(string) < automatic.Rows[j]["region"].(string)
	})
	sort.Slice(fallback.Rows, func(i, j int) bool {
		return fallback.Rows[i]["region"].(string) < fallback.Rows[j]["region"].(string)
	})
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
	}
}

func TestCompiledSQLAutomaticNativeGroupedApproximateAggregatesAcrossCompositeKeys(t *testing.T) {
	rows := []SQLRow{
		{"region": "us", "service": "api", "zone": "a", "tier": "gold", "visitor": "a"},
		{"region": "us", "service": "api", "zone": "a", "tier": "gold", "visitor": "b"},
		{"region": "us", "service": "web", "zone": "b", "tier": "silver", "visitor": "a"},
		{"region": "us", "service": "web", "zone": "b", "tier": "silver", "visitor": "c"},
		{"region": "eu", "service": "api", "zone": "c", "tier": "gold", "visitor": "d"},
		{"region": "eu", "service": "api", "zone": "c", "tier": "gold", "visitor": "d"},
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	for _, keys := range [][]string{
		{"region", "service"},
		{"region", "service", "zone"},
		{"region", "service", "zone", "tier"},
	} {
		t.Run(fmt.Sprintf("%d_keys", len(keys)), func(t *testing.T) {
			query := fmt.Sprintf("FROM CACHE('events') AS src SELECT %s, APPROX_COUNT_DISTINCT(src.visitor, 10) AS visitors GROUP BY %s", qualifyCH039Fields(keys), qualifyCH039Fields(keys))
			var automaticEvent SQLQueryEvent
			automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
				Observer: SQLQueryObserverFunc(func(event SQLQueryEvent) {
					automaticEvent = event
				}),
			})
			if err != nil {
				t.Fatalf("automatic grouped approximate query: %v", err)
			}
			fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
			if err != nil {
				t.Fatalf("fallback grouped approximate query: %v", err)
			}
			sortCH039RowsByKeys(automatic.Rows, keys)
			sortCH039RowsByKeys(fallback.Rows, keys)
			if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
				t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
			}
			if !sqlQueryEventHasOperator(automaticEvent, "NATIVE DATAFLOW") {
				t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", automaticEvent.Operators)
			}
		})
	}
}

func qualifyCH039Fields(fields []string) string {
	qualified := make([]string, len(fields))
	for index, field := range fields {
		qualified[index] = "src." + field
	}
	return strings.Join(qualified, ", ")
}

func sortCH039RowsByKeys(rows []SQLRow, keys []string) {
	sort.Slice(rows, func(left, right int) bool {
		for _, key := range keys {
			leftValue, rightValue := fmt.Sprint(rows[left][key]), fmt.Sprint(rows[right][key])
			if leftValue != rightValue {
				return leftValue < rightValue
			}
		}
		return false
	})
}
