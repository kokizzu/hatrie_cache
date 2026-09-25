package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestCH041GroupingSetsKeepsOnePassTemplate(t *testing.T) {
	query, err := parseSQLQuery(`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5) AS src(region, product, amount) SELECT src.region, src.product, GROUPING(src.region) AS region_grouped, SUM(src.amount) AS total GROUP BY CUBE(src.region, src.product)`)
	if err != nil {
		t.Fatalf("parseSQLQuery() error = %v", err)
	}
	if query.groupingSetsTemplate == nil {
		t.Fatalf("groupingSetsTemplate = nil, want the unexpanded one-pass template")
	}
	if !sqlGroupingSetsOnePassEligible(query.groupingSetsTemplate) {
		t.Fatalf("one-pass eligibility = false, want true")
	}
}

func TestCH041GroupingSetsOnePassMatchesExpandedExecution(t *testing.T) {
	queries := []string{
		`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5), ('west', 'pen', 7) AS src(region, product, amount) SELECT src.region, src.product, GROUPING(src.region) AS region_grouped, COUNT(*) AS count, SUM(src.amount) AS total, AVG(src.amount) AS average, MIN(src.amount) AS minimum, MAX(src.amount) AS maximum GROUP BY CUBE(src.region, src.product)`,
		`FROM VALUES ('east', 'book', 2), ('east', 'pen', 3), ('west', 'book', 5), ('west', 'pen', 7) AS src(region, product, amount) WHERE src.amount >= 3 SELECT src.region, src.product, GROUPING(src.product) AS product_grouped, COUNT(*) AS count, SUM(src.amount) AS total GROUP BY ROLLUP(src.region, src.product)`,
	}
	for _, source := range queries {
		t.Run(source, func(t *testing.T) {
			query, err := parseSQLQuery(source)
			if err != nil {
				t.Fatalf("parseSQLQuery() error = %v", err)
			}
			control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
			if err != nil {
				t.Fatalf("newSQLExecutionControl() error = %v", err)
			}
			fast, err := executeSQLQueryWithMetrics(query, nil, nil, nil, control)
			cancel()
			if err != nil {
				t.Fatalf("one-pass execution error = %v", err)
			}

			legacyQuery := *query
			legacyQuery.groupingSetsTemplate = nil
			control, cancel, err = newSQLExecutionControl(context.Background(), SQLQueryOptions{})
			if err != nil {
				t.Fatalf("new legacy execution control: %v", err)
			}
			legacy, err := executeSQLQueryWithMetrics(&legacyQuery, nil, nil, nil, control)
			cancel()
			if err != nil {
				t.Fatalf("expanded execution error = %v", err)
			}
			if !reflect.DeepEqual(fast.Columns, legacy.Columns) || !reflect.DeepEqual(fast.Rows, legacy.Rows) {
				t.Fatalf("one-pass result = %#v, expanded result = %#v", fast, legacy)
			}
		})
	}
}
