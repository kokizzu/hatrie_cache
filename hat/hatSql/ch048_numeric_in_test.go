package hatSql

import (
	"context"
	"testing"
)

func TestCH048NumericINPredicateRecognizer(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	field, values, ok := sqlColumnarNumericINPredicate(query.where, query.from.alias)
	if !ok {
		t.Fatal("numeric IN predicate was not recognized")
	}
	if field != "value" || len(values) != 3 {
		t.Fatalf("predicate = (%q, %#v), want value and three values", field, values)
	}
	for index, want := range []float64{1, 7, 42} {
		got, ok := sqlNumber(values[index])
		if !ok || got != want {
			t.Fatalf("value %d = %#v, want %v", index, values[index], want)
		}
	}
}

func TestCH048NumericINPredicateRecognizerRejectsUnsafeShapes(t *testing.T) {
	sources := []string{
		`FROM CACHE('events') AS src WHERE src.value NOT IN (1, 7) SELECT src.value`,
		`FROM CACHE('events') AS src WHERE src.value IN (1, NULL) SELECT src.value`,
		`FROM CACHE('events') AS src WHERE src.value IN (src.other) SELECT src.value`,
		`FROM CACHE('events') AS src WHERE other.value IN (1, 7) SELECT src.value`,
	}
	for _, source := range sources {
		query, err := parseSQLQuery(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := sqlColumnarNumericINPredicate(query.where, query.from.alias); ok {
			t.Fatalf("unsafe predicate was recognized: %s", source)
		}
	}
}

func TestCH048NumericINPredicateMatcher(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	for _, packed := range []bool{false, true} {
		matcher := sqlColumnarQueryRowsMatcher(query, newCH048NumericINBatch(packed), nil)
		matched := 0
		for row := 0; row < 4096; row++ {
			ok, err := matcher(row)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				matched++
			}
		}
		if matched != 3 {
			t.Fatalf("packed=%v matched rows = %d, want 3", packed, matched)
		}
	}
}

func TestCH048NumericINPredicateFloatAndNullable(t *testing.T) {
	floatQuery, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1.5, 7.5, 42.5) SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	floatBatch := newCH048FloatINBatch()
	floatMatcher := sqlColumnarQueryRowsMatcher(floatQuery, floatBatch, nil)
	floatMatched := 0
	for row := 0; row < floatBatch.Rows; row++ {
		ok, err := floatMatcher(row)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			floatMatched++
		}
	}
	if floatMatched != 3 {
		t.Fatalf("float matched rows = %d, want 3", floatMatched)
	}

	nullQuery, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	nullBatch := newCH048NullableNumericINBatch()
	nullMatcher := sqlColumnarQueryRowsMatcher(nullQuery, nullBatch, nil)
	nullMatched := 0
	for row := 0; row < nullBatch.Rows; row++ {
		ok, err := nullMatcher(row)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			nullMatched++
		}
	}
	if nullMatched != 3 {
		t.Fatalf("nullable matched rows = %d, want 3", nullMatched)
	}
}

func TestCH048NumericINPredicateFallbackSemantics(t *testing.T) {
	batch := newCH048NumericINBatch(true)
	for source, want := range map[string]int{
		`FROM CACHE('events') AS src WHERE src.value NOT IN (1, 7, 42) SELECT src.value`: 4093,
		`FROM CACHE('events') AS src WHERE src.value IN (1, NULL) SELECT src.value`:      1,
	} {
		query, err := parseSQLQuery(source)
		if err != nil {
			t.Fatal(err)
		}
		matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
		matched := 0
		for row := 0; row < batch.Rows; row++ {
			ok, err := matcher(row)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				matched++
			}
		}
		if matched != want {
			t.Fatalf("%s matched rows = %d, want %d", source, matched, want)
		}
	}
}

func TestCH048NumericINPredicateKernelRejectsMalformedColumn(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7) SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	batch := ColumnarBatch{
		NumericColumns: map[string]ColumnarNumericColumn{
			"value": {Kind: ColumnarNumericInt64, Data: make([]byte, 8), Rows: 2},
		},
		Rows: 2,
	}
	if _, ok := sqlColumnarNumericINPredicateKernelForBatch(query.where, query.from.alias, batch); ok {
		t.Fatal("malformed numeric column was accepted")
	}
}

func TestCH048NumericINPredicateQuery(t *testing.T) {
	result, err := ExecuteSQLQueryParameters(context.Background(), `SELECT value FROM CACHE('events') WHERE value IN (1, 7, 42)`, packedNullableQueryResolver{batch: newCH048NumericINBatch(true)}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(result.Rows); got != 3 {
		t.Fatalf("result rows = %d, want 3", got)
	}
}

func newCH048NumericINBatch(packed bool) ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		values[row] = int64(row)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	if packed {
		batch.PackNumericColumns()
	}
	return batch
}

func newCH048FloatINBatch() ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		values[row] = float64(row) + 0.5
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return batch
}

func newCH048NullableNumericINBatch() ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		if row&3 != 0 {
			values[row] = int64(row)
		}
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return batch
}

var ch048NumericINBenchmarkSink int

func BenchmarkCH048NumericINBaseline(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		b.Fatal(err)
	}
	batch := newCH048NumericINBatch(true)
	b.ReportMetric(float64(batch.Rows), "rows/op")
	b.ResetTimer()
	matched := 0
	for iteration := 0; iteration < b.N; iteration++ {
		for row := 0; row < batch.Rows; row++ {
			value, err := evalSQLStreamExpr(query.where, newSQLColumnarSourceExecRow(query.from.alias, &batch, row), nil)
			if err != nil {
				b.Fatal(err)
			}
			if sqlTruthy(value) {
				matched++
			}
		}
	}
	ch048NumericINBenchmarkSink = matched
}

func BenchmarkCH048NumericINFallback(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		b.Fatal(err)
	}
	batch := newCH048NumericINBatch(true)
	b.ReportMetric(float64(batch.Rows), "rows/op")
	b.ResetTimer()
	matched := 0
	for iteration := 0; iteration < b.N; iteration++ {
		for row := 0; row < batch.Rows; row++ {
			value, err := evalSQLStreamExpr(query.where, newSQLColumnarSourceExecRow(query.from.alias, &batch, row), nil)
			if err != nil {
				b.Fatal(err)
			}
			if sqlTruthy(value) {
				matched++
			}
		}
	}
	ch048NumericINBenchmarkSink = matched
}

func BenchmarkCH048NumericINFastPath(b *testing.B) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IN (1, 7, 42) SELECT src.value`)
	if err != nil {
		b.Fatal(err)
	}
	batch := newCH048NumericINBatch(true)
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	b.ReportMetric(float64(batch.Rows), "rows/op")
	b.ResetTimer()
	matched := 0
	for iteration := 0; iteration < b.N; iteration++ {
		for row := 0; row < batch.Rows; row++ {
			ok, err := matcher(row)
			if err != nil {
				b.Fatal(err)
			}
			if ok {
				matched++
			}
		}
	}
	ch048NumericINBenchmarkSink = matched
}
