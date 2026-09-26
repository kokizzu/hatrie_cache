package hatSql

import (
	"testing"
)

func TestCH048NullPredicateRecognizer(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		field   string
		notNull bool
	}{
		{
			name:  "unqualified is null",
			query: `FROM CACHE('events') AS src WHERE value IS NULL SELECT src.value`,
			field: "value",
		},
		{
			name:    "qualified is not null",
			query:   `FROM CACHE('events') AS src WHERE src.value IS NOT NULL SELECT src.value`,
			field:   "value",
			notNull: true,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQuery(test.query)
			if err != nil {
				t.Fatal(err)
			}
			field, notNull, ok := sqlColumnarNullPredicate(query.where, query.from.alias)
			if !ok {
				t.Fatal("NULL predicate was not recognized")
			}
			if field != test.field || notNull != test.notNull {
				t.Fatalf("predicate = (%q, %v), want (%q, %v)", field, notNull, test.field, test.notNull)
			}
		})
	}
}

func TestCH048NullPredicateRecognizerRejectsUnsafeShapes(t *testing.T) {
	sources := []string{
		`FROM CACHE('events') AS src WHERE src.value IS NULL OR src.other = 1 SELECT src.value`,
		`FROM CACHE('events') AS src WHERE src.value = 1 SELECT src.value`,
	}
	for _, source := range sources {
		query, err := parseSQLQuery(source)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, ok := sqlColumnarNullPredicate(query.where, query.from.alias); ok {
			t.Fatalf("unsafe predicate was recognized: %s", source)
		}
	}
}

func TestCH048NullPredicateMatcher(t *testing.T) {
	cases := []struct {
		name  string
		query string
		batch ColumnarBatch
		want  int
	}{
		{
			name:  "numeric packed is null",
			query: `FROM CACHE('events') AS src WHERE src.value IS NULL SELECT src.value`,
			batch: newCH048NullNumericBatch(true),
			want:  1024,
		},
		{
			name:  "numeric packed is not null",
			query: `FROM CACHE('events') AS src WHERE src.value IS NOT NULL SELECT src.value`,
			batch: newCH048NullNumericBatch(true),
			want:  3072,
		},
		{
			name:  "numeric legacy is null",
			query: `FROM CACHE('events') AS src WHERE src.value IS NULL SELECT src.value`,
			batch: newCH048NullNumericBatch(false),
			want:  1024,
		},
		{
			name:  "numeric packed all non-null",
			query: `FROM CACHE('events') AS src WHERE src.value IS NOT NULL SELECT src.value`,
			batch: newCH048NonNullNumericBatch(),
			want:  4096,
		},
		{
			name:  "nullable packed is null",
			query: `FROM CACHE('events') AS src WHERE src.value IS NULL SELECT src.value`,
			batch: newCH048NullablePackedBatch(),
			want:  1024,
		},
		{
			name:  "boolean packed is null",
			query: `FROM CACHE('events') AS src WHERE src.active IS NULL SELECT src.active`,
			batch: newCH048NullBooleanBatch(true),
			want:  1024,
		},
		{
			name:  "boolean packed is not null",
			query: `FROM CACHE('events') AS src WHERE src.active IS NOT NULL SELECT src.active`,
			batch: newCH048NullBooleanBatch(true),
			want:  3072,
		},
		{
			name:  "boolean legacy is null",
			query: `FROM CACHE('events') AS src WHERE src.active IS NULL SELECT src.active`,
			batch: newCH048NullBooleanBatch(false),
			want:  1024,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQuery(test.query)
			if err != nil {
				t.Fatal(err)
			}
			matcher := sqlColumnarQueryRowsMatcher(query, test.batch, nil)
			matched := 0
			for row := 0; row < test.batch.Rows; row++ {
				ok, err := matcher(row)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					matched++
				}
			}
			if matched != test.want {
				t.Fatalf("matched rows = %d, want %d", matched, test.want)
			}
		})
	}
}

func TestCH048NullPredicateKernelRejectsMalformedTypedColumns(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IS NULL SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	cases := []ColumnarBatch{
		{NumericColumns: map[string]ColumnarNumericColumn{"value": {Kind: ColumnarNumericInt64, Data: make([]byte, 8), Validity: []byte{}, Rows: 1}}, Rows: 1},
		{BoolColumns: map[string]ColumnarBoolColumn{"value": {Bits: []byte{}, Rows: 1}}, Rows: 1},
		{PackedColumns: map[string]ColumnarPackedColumn{"value": {Validity: []byte{}, Ranks: []uint32{0, 0}, Rows: 1}}, Rows: 1},
	}
	for index, batch := range cases {
		if _, ok := sqlColumnarNullPredicateKernelForBatch(query.where, query.from.alias, batch); ok {
			t.Fatalf("case %d unexpectedly accepted malformed column", index)
		}
	}
}

func TestCH048NullPredicateCompoundFallback(t *testing.T) {
	query, err := parseSQLQuery(`FROM CACHE('events') AS src WHERE src.value IS NULL OR src.value = 1 SELECT src.value`)
	if err != nil {
		t.Fatal(err)
	}
	matcher := sqlColumnarQueryRowsMatcher(query, newCH048NullNumericBatch(false), nil)
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
	if matched != 1025 {
		t.Fatalf("compound fallback matched rows = %d, want 1025", matched)
	}
}

func TestCH048NullPredicateQuery(t *testing.T) {
	batch := newCH048NullNumericBatch(true)
	result, err := ExecuteSQLQueryParameters(nil, `SELECT value FROM CACHE('events') WHERE value IS NULL`, packedNullableQueryResolver{batch: batch}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(result.Rows); got != 1024 {
		t.Fatalf("result rows = %d, want 1024", got)
	}
}

func newCH048NullNumericBatch(packed bool) ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		if row&3 != 0 {
			values[row] = int64(row)
		}
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	if packed {
		batch.PackNumericColumns()
	}
	return batch
}

func newCH048NonNullNumericBatch() ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		values[row] = int64(row)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNumericColumns()
	return batch
}

func newCH048NullablePackedBatch() ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		if row&3 != 0 {
			values[row] = int64(row)
		}
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: len(values)}
	batch.PackNullableColumns()
	return batch
}

func newCH048NullBooleanBatch(packed bool) ColumnarBatch {
	values := make([]interface{}, 4096)
	for row := range values {
		if row&3 != 0 {
			values[row] = row&1 == 0
		}
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"active": values}, Rows: len(values)}
	if packed {
		batch.PackBooleanColumns()
	}
	return batch
}

type ch048NullBenchmarkCase struct {
	name  string
	query string
	batch ColumnarBatch
}

func ch048NullBenchmarkCases() []ch048NullBenchmarkCase {
	return []ch048NullBenchmarkCase{
		{name: "numeric_is_null", query: `FROM CACHE('events') AS src WHERE src.value IS NULL SELECT src.value`, batch: newCH048NullNumericBatch(true)},
		{name: "numeric_is_not_null", query: `FROM CACHE('events') AS src WHERE src.value IS NOT NULL SELECT src.value`, batch: newCH048NullNumericBatch(true)},
		{name: "boolean_is_null", query: `FROM CACHE('events') AS src WHERE src.active IS NULL SELECT src.active`, batch: newCH048NullBooleanBatch(true)},
		{name: "boolean_is_not_null", query: `FROM CACHE('events') AS src WHERE src.active IS NOT NULL SELECT src.active`, batch: newCH048NullBooleanBatch(true)},
	}
}

var ch048NullBenchmarkSink int

func BenchmarkCH048NullPredicateFallback(b *testing.B) {
	for _, test := range ch048NullBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			query, err := parseSQLQuery(test.query)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(test.batch.Rows), "rows/op")
			b.ResetTimer()
			matched := 0
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < test.batch.Rows; row++ {
					value, err := evalSQLStreamExpr(query.where, newSQLColumnarSourceExecRow(query.from.alias, &test.batch, row), nil)
					if err != nil {
						b.Fatal(err)
					}
					if sqlTruthy(value) {
						matched++
					}
				}
			}
			ch048NullBenchmarkSink = matched
		})
	}
}

func BenchmarkCH048NullPredicateFastPath(b *testing.B) {
	for _, test := range ch048NullBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			query, err := parseSQLQuery(test.query)
			if err != nil {
				b.Fatal(err)
			}
			matcher := sqlColumnarQueryRowsMatcher(query, test.batch, nil)
			b.ReportMetric(float64(test.batch.Rows), "rows/op")
			b.ResetTimer()
			matched := 0
			for iteration := 0; iteration < b.N; iteration++ {
				for row := 0; row < test.batch.Rows; row++ {
					ok, err := matcher(row)
					if err != nil {
						b.Fatal(err)
					}
					if ok {
						matched++
					}
				}
			}
			ch048NullBenchmarkSink = matched
		})
	}
}
