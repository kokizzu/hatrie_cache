package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlColumnarGroupAggregateResolver struct {
	batch ColumnarBatch
	calls int
}

func (resolver *sqlColumnarGroupAggregateResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a columnar grouped aggregate")
}

func (resolver *sqlColumnarGroupAggregateResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	resolver.calls++
	return resolver.batch, true, nil
}

func TestSQLColumnarDictionaryGroupAggregate(t *testing.T) {
	resolver := &sqlColumnarGroupAggregateResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), `
		SELECT team, COUNT(*) AS total, SUM(score) AS sum, AVG(score) AS average
		FROM CACHE('items')
		WHERE score >= 10
		GROUP BY team
		ORDER BY team
	`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{
		{"team": "core", "total": int64(2), "sum": float64(30), "average": float64(15)},
		{"team": "data", "total": int64(1), "sum": float64(12), "average": float64(12)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryGroupAggregateAfterDictionaryFilter(t *testing.T) {
	resolver := &sqlColumnarGroupAggregateResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), `
		SELECT team, COUNT(*) AS total, SUM(score) AS sum, AVG(score) AS average
		FROM CACHE('items')
		WHERE team = 'core'
		GROUP BY team
		ORDER BY team
	`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{{"team": "core", "total": int64(2), "sum": float64(30), "average": float64(15)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryGroupAggregateAfterDictionaryInequalityFilter(t *testing.T) {
	resolver := &sqlColumnarGroupAggregateResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(), `
		SELECT team, COUNT(*) AS total, SUM(score) AS sum
		FROM CACHE('items')
		WHERE team != 'ops'
		GROUP BY team
		ORDER BY team
	`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if resolver.calls != 1 {
		t.Fatalf("columnar calls = %d, want 1", resolver.calls)
	}
	want := []SQLRow{
		{"team": "core", "total": int64(2), "sum": float64(30)},
		{"team": "data", "total": int64(1), "sum": float64(12)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryGroupAggregateMatchesRowExecutor(t *testing.T) {
	query := `
		SELECT team, COUNT(*) AS total, SUM(score) AS sum, MIN(score) AS minimum, MAX(score) AS maximum
		FROM CACHE('items')
		WHERE score >= 10
		GROUP BY team
		ORDER BY team DESC
	`
	rows := []Row{
		{"team": "ops", "score": int64(7)},
		{"team": "core", "score": int64(10)},
		{"team": "data", "score": int64(12)},
		{"team": "core", "score": int64(20)},
	}
	rowResult, err := ExecuteSQLQueryParameters(context.Background(), query, SourceResolverFunc(func(string, string) ([]Row, error) {
		return rows, nil
	}), nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	columnar := &sqlColumnarGroupAggregateResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{
			"score": {int64(7), int64(10), int64(12), int64(20)},
		},
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"ops", "core", "data"}, Codes: []uint32{0, 1, 2, 1}},
		},
		Rows: 4,
	}}
	columnarResult, err := ExecuteSQLQueryParameters(context.Background(), query, columnar, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columnarResult, rowResult) {
		t.Fatalf("columnar result = %#v, want row result %#v", columnarResult, rowResult)
	}
}

func TestSQLColumnarDictionaryGroupAggregateRejectsNonBinaryCollation(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT team, COUNT(*) FROM CACHE('items') GROUP BY team ORDER BY team", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if _, _, _, _, _, ok := sqlColumnarDictionaryGroupAggregatePlan(query, nil); ok {
		t.Fatal("Unicode case-insensitive grouped aggregate was accepted by the binary dictionary path")
	}
}
