package hatSql

import (
	"context"
	"math"
	"testing"
)

func TestCH048NumericNotBetweenPredicateRecognizer(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 1024 AND 3071 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	predicate, ok := sqlColumnarNumericNotBetweenPredicate(query.where, query.from.alias)
	if !ok {
		t.Fatal("sqlColumnarNumericNotBetweenPredicate() unexpectedly rejected literal NOT BETWEEN")
	}
	if predicate.field != "value" || predicate.lower != 1024 || predicate.upper != 3071 {
		t.Fatalf("numeric NOT BETWEEN predicate = %#v", predicate)
	}
}

func TestCH048NumericNotBetweenMatcherSemantics(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"value": {nil, int64(1023), int64(1024), int64(2048), int64(3071), int64(3072)},
	}, Rows: 6}
	batch.PackNumericColumns()
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 1024 AND 3071 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	want := []bool{false, true, false, false, false, true}
	for row, expected := range want {
		got, err := matcher(row)
		if err != nil {
			t.Fatalf("matcher(%d) error = %v", row, err)
		}
		if got != expected {
			t.Fatalf("matcher(%d) = %t, want %t", row, got, expected)
		}
	}
}

func TestCH048NumericNotBetweenColumnarMaterialization(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"value": {nil, int64(1023), int64(1024), int64(2048), int64(3071), int64(3072)},
	}, Rows: 6}
	batch.PackNumericColumns()
	resolver := ch048NotBetweenColumnarResolver{batch: batch}
	result, err := ExecuteSQLQueryContext(context.Background(), `FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 1024 AND 3071 SELECT src.value`, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() error = %v", err)
	}
	want := []SQLRow{{"value": int64(1023)}, {"value": int64(3072)}}
	if len(result.Rows) != len(want) {
		t.Fatalf("result rows = %#v, want %#v", result.Rows, want)
	}
	for row, expected := range want {
		if result.Rows[row]["value"] != expected["value"] {
			t.Fatalf("result row %d = %#v, want %#v", row, result.Rows[row], expected)
		}
	}
}

func TestCH048StringNotBetweenColumnarMaterialization(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"name": {"a", "b", "m", "y", "z", nil},
	}, Rows: 6}
	resolver := ch048NotBetweenColumnarResolver{batch: batch}
	result, err := ExecuteSQLQueryContext(context.Background(), `FROM CACHE('events') AS src WHERE src.name NOT BETWEEN 'b' AND 'y' SELECT src.name`, resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryContext() string error = %v", err)
	}
	want := []SQLRow{{"name": "a"}, {"name": "z"}}
	if len(result.Rows) != len(want) {
		t.Fatalf("string result rows = %#v, want %#v", result.Rows, want)
	}
	for row, expected := range want {
		if result.Rows[row]["name"] != expected["name"] {
			t.Fatalf("string result row %d = %#v, want %#v", row, result.Rows[row], expected)
		}
	}
}

func TestCH048NumericNotBetweenEdgeSemantics(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"value": {math.NaN(), float64(-1), float64(0), float64(10), float64(11), nil},
	}, Rows: 6}
	batch.PackNumericColumns()
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 0 AND 10 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	want := []bool{true, true, false, false, true, false}
	for row, expected := range want {
		got, err := matcher(row)
		if err != nil {
			t.Fatalf("matcher(%d) error = %v", row, err)
		}
		if got != expected {
			t.Fatalf("matcher(%d) = %t, want %t", row, got, expected)
		}
	}

	query, err = parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 10 AND 0 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() reversed error = %v", err)
	}
	matcher = sqlColumnarQueryRowsMatcher(query, batch, nil)
	for row := 0; row < batch.Rows-1; row++ {
		got, err := matcher(row)
		if err != nil {
			t.Fatalf("reversed matcher(%d) error = %v", row, err)
		}
		if !got {
			t.Fatalf("reversed matcher(%d) = false, want true", row)
		}
	}
	if got, err := matcher(batch.Rows - 1); err != nil || got {
		t.Fatalf("reversed matcher(NULL) = %t, %v; want false, nil", got, err)
	}
}

func TestCH048NumericNotBetweenFallbackAndDynamicBounds(t *testing.T) {
	batch := ColumnarBatch{Columns: map[string][]interface{}{
		"value": {int64(-1), int64(5), int64(11)},
		"lower": {int64(0), int64(0), int64(0)},
	}, Rows: 3}
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN src.lower AND 10 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if _, ok := sqlColumnarNumericNotBetweenPredicate(query.where, query.from.alias); ok {
		t.Fatal("dynamic bounds unexpectedly selected the literal NOT BETWEEN fast path")
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	want := []bool{true, false, true}
	for row, expected := range want {
		got, err := matcher(row)
		if err != nil {
			t.Fatalf("fallback matcher(%d) error = %v", row, err)
		}
		if got != expected {
			t.Fatalf("fallback matcher(%d) = %t, want %t", row, got, expected)
		}
	}
}

type ch048NotBetweenColumnarResolver struct {
	batch ColumnarBatch
}

func (resolver ch048NotBetweenColumnarResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver ch048NotBetweenColumnarResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func benchmarkCH048NumericNotBetweenBatch(rows int) ColumnarBatch {
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = int64(row % 4096)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: rows}
	batch.PackNumericColumns()
	return batch
}

var ch048NumericNotBetweenBenchmarkSink int

func BenchmarkCH048NumericNotBetweenMatcher(b *testing.B) {
	batch := benchmarkCH048NumericNotBetweenBatch(99_840)
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 1024 AND 3071 SELECT src.value`, nil)
	if err != nil {
		b.Fatal(err)
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		matched := 0
		for row := 0; row < batch.Rows; row++ {
			ok, err := matcher(row)
			if err != nil {
				b.Fatal(err)
			}
			if ok {
				matched++
			}
		}
		ch048NumericNotBetweenBenchmarkSink = matched
	}
}
