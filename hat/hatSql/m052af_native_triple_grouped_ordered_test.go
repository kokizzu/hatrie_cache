package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func m052afNativeTripleGroupedOrderedRows() []SQLRow {
	return []SQLRow{
		{"region": "apac", "tier": int64(1), "channel": "web", "value": int64(10)},
		{"region": "apac", "tier": int64(1), "channel": "web", "value": int64(5)},
		{"region": "apac", "tier": int64(1), "channel": "store", "value": int64(100)},
		{"region": "apac", "tier": int64(2), "channel": "web", "value": int64(20)},
		{"region": "apac", "tier": int64(2), "channel": "web", "value": int64(15)},
		{"region": "apac", "tier": int64(2), "channel": "web", "value": nil},
		{"region": "emea", "tier": int64(1), "channel": "web", "value": int64(30)},
		{"region": "emea", "tier": int64(1), "channel": "web", "value": int64(25)},
		{"region": nil, "tier": int64(2), "channel": "web", "value": int64(40)},
		{"region": "us", "tier": int64(1), "channel": "web", "value": int64(5)},
		{"region": "us", "tier": int64(1), "channel": "web", "value": int64(5)},
	}
}

func m052afNativeTripleGroupedOrderedQuery() string {
	return "FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region, src.tier, src.channel HAVING COUNT(*) > 1 ORDER BY total_value DESC, region ASC LIMIT 3 OFFSET 1"
}

func TestCompiledSQLNativeDataflowTripleGroupedOrderedMatchesOrdinaryRows(t *testing.T) {
	rows := m052afNativeTripleGroupedOrderedRows()
	compiled, err := CompileSQLQuery(m052afNativeTripleGroupedOrderedQuery())
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native triple grouped ordered dataflow: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	ordinary, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute ordinary triple grouped ordered query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native triple grouped ordered query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowTripleGroupedOrderedChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel ORDER BY total DESC LIMIT 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native triple grouped ordered dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, m052afNativeTripleGroupedOrderedRows()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}

func TestAutomaticNativeDataflowTripleGroupedOrderedMatchesFallback(t *testing.T) {
	rows := m052afNativeTripleGroupedOrderedRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), m052afNativeTripleGroupedOrderedQuery(), resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic triple grouped ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), m052afNativeTripleGroupedOrderedQuery(), resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("triple grouped ordered fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLNativeDataflowTripleGroupedOrderedRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel ORDER BY COUNT(*) DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.value AS value, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.value, src.segment ORDER BY total DESC LIMIT 1",
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
