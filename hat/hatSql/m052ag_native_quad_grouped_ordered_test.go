package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func m052agNativeQuadGroupedOrderedRows() []SQLRow {
	return []SQLRow{
		{"region": "apac", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(10)},
		{"region": "apac", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(5)},
		{"region": "apac", "tier": int64(1), "channel": "web", "segment": nil, "value": int64(100)},
		{"region": "apac", "tier": int64(2), "channel": "store", "segment": "returning", "value": int64(20)},
		{"region": "apac", "tier": int64(2), "channel": "store", "segment": "returning", "value": int64(15)},
		{"region": "emea", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(30)},
		{"region": "emea", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(25)},
		{"region": nil, "tier": int64(2), "channel": "web", "segment": "new", "value": int64(40)},
		{"region": "us", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(5)},
		{"region": "us", "tier": int64(1), "channel": "web", "segment": "new", "value": int64(5)},
	}
}

func m052agNativeQuadGroupedOrderedQuery() string {
	return "FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.segment AS segment, COUNT(*) AS total, SUM(src.value) AS total_value GROUP BY src.region, src.tier, src.channel, src.segment HAVING COUNT(*) > 1 ORDER BY total_value DESC, region ASC LIMIT 3 OFFSET 1"
}

func TestCompiledSQLNativeDataflowQuadGroupedOrderedMatchesOrdinaryRows(t *testing.T) {
	rows := m052agNativeQuadGroupedOrderedRows()
	compiled, err := CompileSQLQuery(m052agNativeQuadGroupedOrderedQuery())
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native quad grouped ordered dataflow: %v", err)
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	ordinary, err := compiled.Execute(context.Background(), resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute ordinary quad grouped ordered query: %v", err)
	}
	actual, err := native.Execute(context.Background(), rows)
	if err != nil {
		t.Fatalf("execute native quad grouped ordered query: %v", err)
	}
	if !reflect.DeepEqual(actual, ordinary.Rows) {
		t.Fatalf("native rows = %#v, ordinary rows = %#v", actual, ordinary.Rows)
	}
}

func TestCompiledSQLNativeDataflowQuadGroupedOrderedChecksContext(t *testing.T) {
	compiled, err := CompileSQLQuery("FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.segment AS segment, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.segment ORDER BY total DESC LIMIT 1")
	if err != nil {
		t.Fatalf("compile SQL: %v", err)
	}
	native, err := compiled.CompileNativeDataflow()
	if err != nil {
		t.Fatalf("compile native quad grouped ordered dataflow: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := native.Execute(canceled, m052agNativeQuadGroupedOrderedRows()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled native execution error = %v, want %v", err, context.Canceled)
	}
}

func TestAutomaticNativeDataflowQuadGroupedOrderedMatchesFallback(t *testing.T) {
	rows := m052agNativeQuadGroupedOrderedRows()
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})
	auto, err := ExecuteSQLQueryContext(context.Background(), m052agNativeQuadGroupedOrderedQuery(), resolver, SQLQueryOptions{
		Observer: SQLQueryObserverFunc(func(SQLQueryEvent) {}),
	})
	if err != nil {
		t.Fatalf("automatic quad grouped ordered query: %v", err)
	}
	fallback, err := ExecuteSQLQueryContext(context.Background(), m052agNativeQuadGroupedOrderedQuery(), resolver, SQLQueryOptions{
		DisableNativeDataflow: true,
	})
	if err != nil {
		t.Fatalf("quad grouped ordered fallback: %v", err)
	}
	if !reflect.DeepEqual(auto.Rows, fallback.Rows) {
		t.Fatalf("automatic rows = %#v, fallback rows = %#v", auto.Rows, fallback.Rows)
	}
	if !m052qPlanHasNode(auto.Plan, "NATIVE DATAFLOW") {
		t.Fatalf("automatic plan = %#v, want NATIVE DATAFLOW", auto.Plan)
	}
}

func TestCompiledSQLNativeDataflowQuadGroupedOrderedRejectsUnsupportedShapes(t *testing.T) {
	queries := []string{
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.segment AS segment, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.segment ORDER BY COUNT(*) DESC LIMIT 1",
		"FROM CACHE('items') AS src SELECT src.region AS region, src.tier AS tier, src.channel AS channel, src.segment AS segment, src.value AS value, COUNT(*) AS total GROUP BY src.region, src.tier, src.channel, src.segment, src.value ORDER BY total DESC LIMIT 1",
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
