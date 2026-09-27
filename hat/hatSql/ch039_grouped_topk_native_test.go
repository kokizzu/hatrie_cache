package hatSql

import (
	"context"
	"reflect"
	"sort"
	"testing"
)

func TestCH039GroupedApproxTopKUsesNativeDataflow(t *testing.T) {
	rows := []SQLRow{
		{"region": "us", "state": "queued"},
		{"region": "us", "state": "queued"},
		{"region": "us", "state": "running"},
		{"region": "us", "state": "queued"},
		{"region": "us", "state": "done"},
		{"region": "eu", "state": "failed"},
		{"region": "eu", "state": "failed"},
		{"region": "eu", "state": "queued"},
	}
	query := "FROM CACHE('events') AS src SELECT src.region, APPROX_TOP_K(src.state, 2) AS states GROUP BY src.region"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		t.Fatalf("compile query: %v", err)
	}
	if _, err := compiled.CompileNativeDataflow(); err != nil {
		t.Fatalf("compile native grouped top-k: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	var event SQLQueryEvent
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(observed SQLQueryEvent) {
			event = observed
		}),
	})
	if err != nil {
		t.Fatalf("automatic grouped top-k: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback grouped top-k: %v", err)
	}
	sort.Slice(automatic.Rows, func(left, right int) bool {
		return automatic.Rows[left]["region"].(string) < automatic.Rows[right]["region"].(string)
	})
	sort.Slice(fallback.Rows, func(left, right int) bool {
		return fallback.Rows[left]["region"].(string) < fallback.Rows[right]["region"].(string)
	})
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic result = %#v, fallback = %#v", automatic, fallback)
	}
	if !sqlQueryEventHasOperator(event, "NATIVE DATAFLOW") {
		t.Fatalf("automatic operators = %#v, want NATIVE DATAFLOW", event.Operators)
	}
}

func TestCH039GroupedApproxTopKIsolatedAcrossCompositeGroups(t *testing.T) {
	rows := []SQLRow{
		{"region": "us", "service": "api", "state": "queued"},
		{"region": "us", "service": "api", "state": "queued"},
		{"region": "us", "service": "api", "state": "running"},
		{"region": "us", "service": "web", "state": "failed"},
		{"region": "us", "service": "web", "state": "failed"},
		{"region": "eu", "service": "api", "state": "queued"},
	}
	query := "FROM CACHE('events') AS src SELECT src.region, src.service, APPROX_TOP_K(src.state, 1) AS states GROUP BY src.region, src.service"
	compiled, err := CompileSQLQuery(query)
	if err != nil {
		t.Fatalf("compile composite query: %v", err)
	}
	if _, err := compiled.CompileNativeDataflow(); err != nil {
		t.Fatalf("compile native composite grouped top-k: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	automatic, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("automatic composite grouped top-k: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{DisableNativeDataflow: true})
	if err != nil {
		t.Fatalf("fallback composite grouped top-k: %v", err)
	}
	sort.Slice(automatic.Rows, func(left, right int) bool {
		return automatic.Rows[left]["region"].(string)+automatic.Rows[left]["service"].(string) < automatic.Rows[right]["region"].(string)+automatic.Rows[right]["service"].(string)
	})
	sort.Slice(fallback.Rows, func(left, right int) bool {
		return fallback.Rows[left]["region"].(string)+fallback.Rows[left]["service"].(string) < fallback.Rows[right]["region"].(string)+fallback.Rows[right]["service"].(string)
	})
	if !reflect.DeepEqual(automatic.Columns, fallback.Columns) || !reflect.DeepEqual(automatic.Rows, fallback.Rows) {
		t.Fatalf("automatic composite result = %#v, fallback = %#v", automatic, fallback)
	}
}
