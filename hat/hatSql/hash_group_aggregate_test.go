package hatSql

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

type hashGroupAggregateBenchmarkResolver struct {
	rows []SQLRow
}

func (resolver hashGroupAggregateBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

type hashGroupAggregateStreamResolver struct {
	rows     []SQLRow
	streamed bool
	resolved bool
}

func (resolver *hashGroupAggregateStreamResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	resolver.resolved = true
	return resolver.rows, nil
}

func (resolver *hashGroupAggregateStreamResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(SQLRow) error) error {
	resolver.streamed = true
	for _, row := range resolver.rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func TestSQLHashGroupAggregatePlanAcceptsSimpleGroupedAggregates(t *testing.T) {
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE"},
		groupBy: []sqlExpr{{kind: "field", name: "region"}},
		selects: []sqlSelectItem{
			{expr: sqlExpr{kind: "field", name: "region"}},
			{expr: sqlExpr{kind: "func", name: "COUNT", args: []sqlExpr{{kind: "star"}}}},
			{expr: sqlExpr{kind: "func", name: "SUM", args: []sqlExpr{{kind: "field", name: "score"}}}},
		},
	}
	if !sqlHashGroupAggregatePlan(query) {
		t.Fatal("simple grouped aggregate was not accepted by the hash plan")
	}
}

func TestSQLHashGroupAggregatePreservesNullsOrderAndNumericSemantics(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 10),
		('eu', NULL),
		('us', 7),
		(NULL, 5),
		(NULL, 4)
	AS values(region, score)
	SELECT region, COUNT(*) AS rows, COUNT(score) AS scored,
		SUM(score) AS total, MIN(score) AS minimum, MAX(score) AS maximum
	GROUP BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "rows": int64(2), "scored": int64(2), "total": float64(17), "minimum": float64(7), "maximum": float64(10)},
		{"region": "eu", "rows": int64(1), "scored": int64(0), "total": nil, "minimum": nil, "maximum": nil},
		{"region": nil, "rows": int64(2), "scored": int64(2), "total": float64(9), "minimum": float64(4), "maximum": float64(5)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLHashGroupAggregateFallsBackForHaving(t *testing.T) {
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE"},
		groupBy: []sqlExpr{{kind: "field", name: "region"}},
		having:  sqlExpr{kind: "literal", value: true},
		selects: []sqlSelectItem{{expr: sqlExpr{kind: "field", name: "region"}}},
	}
	if sqlHashGroupAggregatePlan(query) {
		t.Fatal("HAVING query should retain the materialized executor")
	}
}

func TestSQLHashGroupAggregateUsesStreamResolver(t *testing.T) {
	resolver := &hashGroupAggregateStreamResolver{rows: []SQLRow{
		{"region": "us", "score": int64(10)},
		{"region": "eu", "score": int64(8)},
		{"region": "us", "score": int64(7)},
	}}
	result, err := ExecuteSQLQuery(`FROM CACHE('events')
		SELECT region, COUNT(*) AS rows, SUM(score) AS total
		GROUP BY region`, resolver)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "rows": int64(2), "total": float64(17)},
		{"region": "eu", "rows": int64(1), "total": float64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if !resolver.streamed || resolver.resolved {
		t.Fatalf("resolver calls = streamed:%v resolved:%v, want stream-only", resolver.streamed, resolver.resolved)
	}
}

func TestSQLHashGroupAggregateRespectsFilterOffsetAndLimit(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 10),
		('us', 7),
		('eu', 8),
		('apac', 4)
	AS values(region, score)
	SELECT region, COUNT(*) AS rows, SUM(score) AS total
	WHERE score >= 5
	GROUP BY region
	LIMIT 1 OFFSET 1`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{{"region": "eu", "rows": int64(1), "total": float64(8)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLHashGroupAggregateUsesCollationForKeys(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES
		('A', 10),
		('a', 7),
		('B', 3)
	AS values(region, score)
	SELECT region, COUNT(*) AS rows, SUM(score) AS total
	GROUP BY region`, nil, nil, SQLQueryOptions{Collation: SQLCollationUnicodeCI})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{
		{"region": "A", "rows": int64(2), "total": float64(17)},
		{"region": "B", "rows": int64(1), "total": float64(3)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLHashGroupAggregateRespectsGroupBudgets(t *testing.T) {
	query := `FROM VALUES
		('us', 10),
		('us', 7),
		('eu', 8)
	AS values(region, score)
	SELECT region, COUNT(*) AS rows
	GROUP BY region`
	_, err := ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxGroupRowsPerKey: 1})
	if err == nil {
		t.Fatal("expected MaxGroupRowsPerKey error")
	}
	_, err = ExecuteSQLQueryParameters(context.Background(), query, nil, nil, SQLQueryOptions{MaxGroupBytes: 1})
	if err == nil {
		t.Fatal("expected MaxGroupBytes error from the established fallback")
	}
}

func TestSQLHashGroupAggregateFallsBackForComputedAggregate(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 10),
		('us', 7),
		('eu', 8)
	AS values(region, score)
	SELECT region, SUM(score + 1) AS total
	GROUP BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "total": float64(19)},
		{"region": "eu", "total": float64(9)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func BenchmarkSQLHashGroupAggregate(b *testing.B) {
	rows := make([]SQLRow, 20000)
	for index := range rows {
		rows[index] = SQLRow{
			"region": "region-" + strconv.Itoa(index%100),
			"score":  int64((index * 7919) % 100000),
		}
	}
	resolver := hashGroupAggregateBenchmarkResolver{rows: rows}
	queries := []struct {
		name  string
		query string
	}{
		{name: "baseline", query: `FROM CACHE('events') SELECT region, COUNT(*) AS rows, SUM(score) AS total GROUP BY region HAVING COUNT(*) > 0`},
		{name: "hash", query: `FROM CACHE('events') SELECT region, COUNT(*) AS rows, SUM(score) AS total GROUP BY region`},
	}
	for _, benchmark := range queries {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(benchmark.query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 100 {
					b.Fatalf("result rows = %d, want 100", len(result.Rows))
				}
			}
		})
	}
}
