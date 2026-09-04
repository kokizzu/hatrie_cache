package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type sqlVectorColumnarResolver struct {
	batch         ColumnarBatch
	rows          []Row
	columnarCalls int
	rowCalls      int
}

func (resolver *sqlVectorColumnarResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.rowCalls++
	return resolver.rows, nil
}

func (resolver *sqlVectorColumnarResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	resolver.columnarCalls++
	return resolver.batch, true, nil
}

func TestSQLColumnarVectorGroupAggregatePlan(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT region, COUNT(*) AS n, SUM(score) AS total, AVG(score) AS average, MIN(score) AS minimum, MAX(score) AS maximum FROM CACHE('items') WHERE active >= 1 GROUP BY region", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := sqlColumnarVectorGroupAggregatePlan(query, nil); !ok {
		t.Fatal("plain-key columnar grouping was not admitted to the vectorized plan")
	}
}

func TestSQLColumnarVectorGroupAggregateMatchesRowSemantics(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	query := "SELECT region, COUNT(*) AS n, SUM(score) AS total, AVG(score) AS average, MIN(score) AS minimum, MAX(score) AS maximum FROM CACHE('items') WHERE active >= 1 GROUP BY region"
	want := []SQLRow{
		{"region": "west", "n": int64(2), "total": float64(10), "average": float64(10), "minimum": float64(10), "maximum": float64(10)},
		{"region": "east", "n": int64(1), "total": float64(7), "average": float64(7), "minimum": float64(7), "maximum": float64(7)},
		{"region": nil, "n": int64(1), "total": float64(4), "average": float64(4), "minimum": float64(4), "maximum": float64(4)},
	}
	result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	baseline, err := ExecuteSQLQueryParameters(context.Background(), query, sqlVectorRowOnlyResolver{rows: resolver.rows}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, baseline.Rows) {
		t.Fatalf("vectorized rows = %#v, row executor rows = %#v", result.Rows, baseline.Rows)
	}
	if resolver.columnarCalls == 0 {
		t.Fatal("vectorized query did not request the columnar source")
	}
	if resolver.rowCalls != 0 {
		t.Fatalf("vectorized query resolved row source %d times", resolver.rowCalls)
	}
}

func TestSQLColumnarVectorGroupAggregateStreamsRows(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	query := "SELECT region, COUNT(*) AS n FROM CACHE('items') WHERE active >= 1 GROUP BY region"
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), query, resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"region": "west", "n": int64(2)},
		{"region": "east", "n": int64(1)},
		{"region": nil, "n": int64(1)},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
	if resolver.columnarCalls == 0 || resolver.rowCalls != 0 {
		t.Fatalf("source calls = columnar %d, row %d; want columnar only", resolver.columnarCalls, resolver.rowCalls)
	}
}

func TestSQLColumnarVectorGroupAggregatePreservesFallback(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	query := "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region ORDER BY region"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"region": nil, "n": int64(1)},
		{"region": "east", "n": int64(2)},
		{"region": "west", "n": int64(2)},
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.rowCalls == 0 {
		t.Fatal("unsupported ordered grouping did not use the established fallback")
	}
}

func TestSQLColumnarVectorGroupAggregateHonorsGroupSkewLimit(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	_, err := ExecuteSQLQueryParameters(context.Background(), "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", resolver, nil, SQLQueryOptions{MaxGroupRowsPerKey: 1})
	if err == nil {
		t.Fatal("expected group skew limit error")
	}
}

func TestSQLColumnarVectorGroupAggregatePreservesPaginationAndMemoryFallback(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	query := "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region LIMIT 1 OFFSET 1"
	result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"region": "east", "n": int64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.rowCalls != 0 {
		t.Fatalf("ordinary vectorized query resolved row source %d times", resolver.rowCalls)
	}

	result, err = ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{MaxGroupBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("fallback rows = %#v, want %#v", result.Rows, want)
	}
	if resolver.rowCalls == 0 {
		t.Fatal("MaxGroupBytes did not preserve the established row executor fallback")
	}
}

func TestSQLColumnarVectorGroupAggregateRejectsRicherShapes(t *testing.T) {
	queries := []string{
		"SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region HAVING COUNT(*) > 1",
		"SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region ORDER BY region",
		"SELECT region, active, COUNT(*) AS n FROM CACHE('items') GROUP BY region, active",
	}
	for _, source := range queries {
		query, err := parseSQLQueryWithCache(source, nil, nil)
		if err != nil {
			t.Fatalf("parse %q: %v", source, err)
		}
		if _, _, _, ok := sqlColumnarVectorGroupAggregatePlan(query, nil); ok {
			t.Fatalf("richer query was admitted to vectorized plan: %q", source)
		}
	}
}

func TestSQLColumnarVectorGroupAggregateCrossesBlockBoundaries(t *testing.T) {
	rows := 2051
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"region": make([]interface{}, rows),
		"score":  make([]interface{}, rows),
	}, Rows: rows}
	rowValues := make([]Row, rows)
	for index := 0; index < rows; index++ {
		region := "region-" + string(rune('a'+index%3))
		score := int64(index + 1)
		batch.Columns["region"][index] = region
		batch.Columns["score"][index] = score
		rowValues[index] = Row{"region": region, "score": score}
	}
	resolver := &sqlVectorColumnarResolver{batch: batch, rows: rowValues}
	query := "SELECT region, COUNT(*) AS n, SUM(score) AS total FROM CACHE('items') WHERE score >= 0 GROUP BY region"
	vectorized, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := ExecuteSQLQueryParameters(context.Background(), query, sqlVectorRowOnlyResolver{rows: rowValues}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vectorized.Rows, baseline.Rows) {
		t.Fatalf("vectorized rows = %#v, row executor rows = %#v", vectorized.Rows, baseline.Rows)
	}
}

func TestSQLColumnarVectorGroupAggregateReadsDictionaryGroupKey(t *testing.T) {
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"score": {int64(3), int64(4), int64(5), int64(6)}},
		Dictionaries: map[string]DictionaryColumn{
			"region": {Values: []string{"west", "east"}, Codes: []uint32{0, 1, 0, 1}},
		},
		Rows: 4,
	}
	rows := []Row{
		{"region": "west", "score": int64(3)},
		{"region": "east", "score": int64(4)},
		{"region": "west", "score": int64(5)},
		{"region": "east", "score": int64(6)},
	}
	resolver := &sqlVectorColumnarResolver{batch: batch, rows: rows}
	var got []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), "SELECT region, COUNT(*) AS n, SUM(score) AS total FROM CACHE('items') GROUP BY region", resolver, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		got = append(got, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"region": "west", "n": int64(2), "total": float64(8)},
		{"region": "east", "n": int64(2), "total": float64(10)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

func TestSQLColumnarVectorGroupAggregatePreservesCallbackAndResultErrors(t *testing.T) {
	resolver := newSQLVectorColumnarResolver()
	visitErr := errors.New("stop vectorized rows")
	err := ExecuteSQLQueryRows(context.Background(), "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", resolver, nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
		return visitErr
	})
	if !errors.Is(err, visitErr) {
		t.Fatalf("callback error = %v, want %v", err, visitErr)
	}

	_, err = ExecuteSQLQueryParameters(context.Background(), "SELECT region, COUNT(*) AS n FROM CACHE('items') GROUP BY region", newSQLVectorColumnarResolver(), nil, SQLQueryOptions{MaxResultBytes: 1})
	if err == nil {
		t.Fatal("expected result byte budget error")
	}
}

func newSQLVectorColumnarResolver() *sqlVectorColumnarResolver {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"region": {"west", "east", "west", "east", nil},
		"score":  {int64(10), int64(20), nil, int64(7), int64(4)},
		"active": {int64(1), int64(0), int64(2), int64(1), int64(1)},
	}, Rows: 5}
	rows := make([]Row, batch.Rows)
	for index := 0; index < batch.Rows; index++ {
		rows[index] = Row{
			"region": batch.Columns["region"][index],
			"score":  batch.Columns["score"][index],
			"active": batch.Columns["active"][index],
		}
	}
	return &sqlVectorColumnarResolver{batch: batch, rows: rows}
}
