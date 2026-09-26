package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func m052aeNativeTripleGroupRows() []SQLRow {
	return []SQLRow{
		{"region": "apac", "tier": int64(1), "channel": "web", "value": int64(2)},
		{"region": "apac", "tier": int64(1), "channel": "web", "value": int64(3)},
		{"region": "apac", "tier": int64(1), "channel": "store", "value": int64(4)},
		{"region": "apac", "tier": int64(2), "channel": "web", "value": int64(5)},
		{"region": nil, "tier": int64(2), "channel": "web", "value": int64(6)},
		{"region": nil, "tier": int64(2), "channel": "web", "value": nil},
		{"region": "", "tier": int64(0), "channel": "", "value": int64(1)},
	}
}

func m052aeNativeTripleGroupQuery() string {
	return "FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region, src.tier, src.channel"
}

func TestCompiledSQLNativeDataflowTripleGroupMatchesOrdinaryRows(t *testing.T) {
	rows := m052aeNativeTripleGroupRows()
	compiled, err := CompileSQLQuery(m052aeNativeTripleGroupQuery())
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native triple-group dataflow: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	ordinary, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute ordinary triple-group query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native triple-group query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowTripleGroupChecksContextAndRuntimeKey(t *testing.T) {
	compiled, err := CompileSQLQuery(m052aeNativeTripleGroupQuery())
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native triple-group dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, m052aeNativeTripleGroupRows()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
	unsupported := m052aeNativeTripleGroupRows()
	unsupported[0]["channel"] = []byte("web")
	if _, err := native.Execute(context.Background(), unsupported); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
		t.Fatalf("unsupported native triple GROUP BY key error = %v, want %v", err, ErrSQLNativeDataflowUnsupported)
	}
}

func TestAutomaticNativeDataflowTripleGroupMatchesFallback(t *testing.T) {
	rows := m052aeNativeTripleGroupRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), m052aeNativeTripleGroupQuery(), resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic triple-group query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), m052aeNativeTripleGroupQuery(), resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("triple-group fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLNativeDataflowTripleGroupRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel HAVING COUNT(*) > 1",
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.value AS value, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.value",
	}
	for _, source := range queries {
		compiled, err := CompileSQLQuery(source)
		if err != nil {
			t.Fatalf("compile %q: %v", source, err)
		}
		if _, err := compiled.CompileNativeDataflow(); !errors.Is(err, ErrSQLNativeDataflowUnsupported) {
			t.Errorf("CompileNativeDataflow(%q) error = %v, want %v", source, err, ErrSQLNativeDataflowUnsupported)
		}
	}
}
