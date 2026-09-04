package hatSql

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

type limitByBenchmarkResolver struct {
	rows []SQLRow
}

func (resolver limitByBenchmarkResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func TestSQLLimitByKeepsGlobalOrderAndCapsEachGroup(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 10),
		('eu', 8),
		('us', 7),
		('eu', 6),
		('us', 5),
		('apac', 4),
		('eu', 3)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 2 BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "score": int64(10)},
		{"region": "eu", "score": int64(8)},
		{"region": "us", "score": int64(7)},
		{"region": "eu", "score": int64(6)},
		{"region": "apac", "score": int64(4)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitBySupportsCompositeKeysAndGlobalLimit(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 'a', 10),
		('us', 'a', 9),
		('us', 'b', 8),
		('us', 'b', 7),
		('eu', 'a', 6),
		('eu', 'a', 5)
	AS values(region, kind, score)
	SELECT region, kind, score
	ORDER BY score DESC
	LIMIT 1 BY region, kind
	LIMIT 3`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "kind": "a", "score": int64(10)},
		{"region": "us", "kind": "b", "score": int64(8)},
		{"region": "eu", "kind": "a", "score": int64(6)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitByTreatsNullAsOneGroup(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		(NULL, 10),
		(NULL, 9),
		('us', 8),
		('us', 7)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 1 BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"region": nil, "score": int64(10)},
		{"region": "us", "score": int64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitByUsesSourceFieldsNotSelectedColumns(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('us', 10),
		('us', 9),
		('eu', 8),
		('eu', 7)
	AS values(region, score)
	SELECT score
	ORDER BY score DESC
	LIMIT 1 BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	want := []SQLRow{
		{"score": int64(10)},
		{"score": int64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitByUsesQueryCollation(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES
		('A', 10),
		('a', 9),
		('B', 8)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 1 BY region`, nil, nil, SQLQueryOptions{Collation: SQLCollationUnicodeCI})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{
		{"region": "A", "score": int64(10)},
		{"region": "B", "score": int64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitByCompositeKeyDoesNotUseValueDelimiters(t *testing.T) {
	result, err := ExecuteSQLQuery(`FROM VALUES
		('a\x00b', 'c', 10),
		('a', 'b\x00c', 9)
	AS values(first, second, score)
	SELECT first, second
	ORDER BY score DESC
	LIMIT 1 BY first, second`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("rows = %#v, want two distinct composite groups", result.Rows)
	}
}

func TestSQLLimitByExplainIncludesOperator(t *testing.T) {
	result, err := ExecuteSQLQuery(`EXPLAIN FROM VALUES ('us', 10) AS values(region, score) SELECT region, score LIMIT 1 BY region`, nil)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	for _, row := range result.Rows {
		if row["node"] == "LIMIT BY" {
			return
		}
	}
	t.Fatalf("EXPLAIN rows = %#v, missing LIMIT BY", result.Rows)
}

func TestSQLLimitByBindsPreparedGroupExpressions(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES
		('us', 10),
		('us', 9),
		('eu', 8),
		('eu', 7)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 1 BY $1, region`, nil, []interface{}{"constant"}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "score": int64(10)},
		{"region": "eu", "score": int64(8)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLLimitByStreamsThroughRowCallback(t *testing.T) {
	var got []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), `FROM VALUES
		('us', 10),
		('us', 9),
		('eu', 8),
		('eu', 7)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 1 BY region`, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "score": int64(10)},
		{"region": "eu", "score": int64(8)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

func TestSQLLimitByWorksWithExternalSort(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM VALUES
		('us', 10),
		('eu', 8),
		('us', 7),
		('eu', 6),
		('apac', 4)
	AS values(region, score)
	SELECT region, score
	ORDER BY score DESC
	LIMIT 1 BY region`, nil, nil, SQLQueryOptions{
		MaxSortBytes:   1,
		MaxSpillBytes:  1 << 20,
		SpillDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{
		{"region": "us", "score": int64(10)},
		{"region": "eu", "score": int64(8)},
		{"region": "apac", "score": int64(4)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func BenchmarkSQLLimitBy(b *testing.B) {
	rows := make([]SQLRow, 10000)
	for index := range rows {
		rows[index] = SQLRow{
			"region": "region-" + strconv.Itoa(index%100),
			"score":  int64((index * 7919) % 100000),
		}
	}
	resolver := limitByBenchmarkResolver{rows: rows}
	queries := []struct {
		name  string
		query string
	}{
		{name: "baseline", query: `FROM CACHE('events') SELECT region, score ORDER BY score DESC`},
		{name: "limit_by", query: `FROM CACHE('events') SELECT region, score ORDER BY score DESC LIMIT 2 BY region`},
	}
	for _, benchmark := range queries {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				result, err := ExecuteSQLQuery(benchmark.query, resolver)
				if err != nil {
					b.Fatal(err)
				}
				if benchmark.name == "limit_by" && len(result.Rows) != 200 {
					b.Fatalf("result rows = %d, want 200", len(result.Rows))
				}
			}
		})
	}
}
