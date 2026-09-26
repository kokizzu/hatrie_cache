package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

var ch048BetweenBenchmarkSink int

func TestCH048DictionaryBetweenPredicateRecognizer(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name BETWEEN 'name-16' AND 'name-47' SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	dictionary := newCH048BetweenDictionaryBatch(false).Dictionaries["name"]
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"name": dictionary}, Rows: len(dictionary.Codes)}
	gotDictionary, gotCodes, ok := sqlColumnarDictionaryBetweenPredicate(query.where, query.from.alias, batch)
	if !ok {
		t.Fatal("sqlColumnarDictionaryBetweenPredicate() unexpectedly rejected binary BETWEEN")
	}
	if len(gotDictionary.Values) != len(dictionary.Values) {
		t.Fatalf("dictionary values = %d, want %d", len(gotDictionary.Values), len(dictionary.Values))
	}
	for code := uint32(0); code < uint32(len(dictionary.Values)); code++ {
		want := code >= 16 && code <= 47
		if got := gotCodes.matches(code); got != want {
			t.Fatalf("dictionary code %d = %t, want %t", code, got, want)
		}
	}

	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if _, _, ok := sqlColumnarDictionaryBetweenPredicate(query.where, query.from.alias, batch); ok {
		t.Fatal("UnicodeCI dictionary BETWEEN unexpectedly took the binary fast path")
	}
	for _, source := range []string{
		`FROM CACHE('events') AS src WHERE src.name BETWEEN src.lower AND 'name-47' SELECT src.name`,
		`FROM CACHE('events') AS src WHERE src.name NOT BETWEEN 'name-16' AND 'name-47' SELECT src.name`,
	} {
		query, err := parseSQLQueryParameters(source, nil)
		if err != nil {
			t.Fatalf("parseSQLQueryParameters(%q) error = %v", source, err)
		}
		if _, _, ok := sqlColumnarDictionaryBetweenPredicate(query.where, query.from.alias, batch); ok {
			t.Fatalf("unsafe dictionary BETWEEN shape %q unexpectedly encoded", source)
		}
	}
}

func TestCH048NumericBetweenPredicateRecognizer(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN 1024 AND 3071 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	filters, ok := sqlColumnarNumericBetweenPredicate(query.where, query.from.alias)
	if !ok || len(filters) != 2 {
		t.Fatalf("sqlColumnarNumericBetweenPredicate() = %#v, %t; want two filters", filters, ok)
	}
	if filters[0].field != "value" || filters[0].operator != ">=" || filters[0].value != 1024 || filters[1].field != "value" || filters[1].operator != "<=" || filters[1].value != 3071 {
		t.Fatalf("numeric BETWEEN filters = %#v", filters)
	}
	for _, source := range []string{
		`FROM CACHE('events') AS src WHERE src.value NOT BETWEEN 1024 AND 3071 SELECT src.value`,
		`FROM CACHE('events') AS src WHERE src.value BETWEEN src.lower AND 3071 SELECT src.value`,
	} {
		query, err := parseSQLQueryParameters(source, nil)
		if err != nil {
			t.Fatalf("parseSQLQueryParameters(%q) error = %v", source, err)
		}
		if _, ok := sqlColumnarNumericBetweenPredicate(query.where, query.from.alias); ok {
			t.Fatalf("unsafe numeric BETWEEN shape %q unexpectedly encoded", source)
		}
	}
}

func TestCH048BetweenMatchersPreservePackedResults(t *testing.T) {
	for _, test := range []struct {
		name  string
		query string
		batch ColumnarBatch
		want  int
	}{
		{
			name:  "dictionary",
			query: `FROM CACHE('events') AS src WHERE src.name BETWEEN 'name-16' AND 'name-47' SELECT src.name`,
			batch: newCH048BetweenDictionaryBatch(true),
			want:  2048,
		},
		{
			name:  "numeric",
			query: `FROM CACHE('events') AS src WHERE src.value BETWEEN 1024 AND 3071 SELECT src.value`,
			batch: newCH048BetweenNumericBatch(true),
			want:  2048,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQueryParameters(test.query, nil)
			if err != nil {
				t.Fatalf("parseSQLQueryParameters() error = %v", err)
			}
			matcher := sqlColumnarQueryRowsMatcher(query, test.batch, nil)
			matched := 0
			for row := 0; row < test.batch.Rows; row++ {
				ok, err := matcher(row)
				if err != nil {
					t.Fatalf("matcher(%d) error = %v", row, err)
				}
				if ok {
					matched++
				}
			}
			if matched != test.want {
				t.Fatalf("matched = %d, want %d", matched, test.want)
			}
		})
	}
}

func TestCH048BetweenFallbackSemantics(t *testing.T) {
	dictionaryBatch := newCH048BetweenDictionaryBatch(true)
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name NOT BETWEEN 'name-16' AND 'name-47' SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	matcher := sqlColumnarQueryRowsMatcher(query, dictionaryBatch, nil)
	matched := 0
	for row := 0; row < dictionaryBatch.Rows; row++ {
		ok, err := matcher(row)
		if err != nil {
			t.Fatalf("dictionary matcher(%d) error = %v", row, err)
		}
		if ok {
			matched++
		}
	}
	if matched != 2048 {
		t.Fatalf("NOT BETWEEN matched = %d, want %d", matched, 2048)
	}

	numericBatch := ColumnarBatch{Columns: map[string][]interface{}{"value": {nil, int64(1), int64(2), int64(3), int64(4)}}, Rows: 5}
	numericBatch.PackNumericColumns()
	query, err = parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value BETWEEN 1 AND 3 SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() numeric error = %v", err)
	}
	matcher = sqlColumnarQueryRowsMatcher(query, numericBatch, nil)
	matched = 0
	for row := 0; row < numericBatch.Rows; row++ {
		ok, err := matcher(row)
		if err != nil {
			t.Fatalf("numeric matcher(%d) error = %v", row, err)
		}
		if ok {
			matched++
		}
	}
	if matched != 3 {
		t.Fatalf("nullable BETWEEN matched = %d, want %d", matched, 3)
	}
}

func TestCH048BetweenQueryExecutionPreservesResults(t *testing.T) {
	dictionary := DictionaryColumn{Values: []string{"a", "m", "z"}, Codes: []uint32{0, 1, 2, 1, 0}}
	dictionaryBatch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"name": dictionary}, Rows: len(dictionary.Codes)}
	dictionaryBatch.PackDictionaryCodes()
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('events') AS src WHERE src.name BETWEEN 'm' AND 'z' SELECT src.name`, packedDictionaryQueryResolver{batch: dictionaryBatch}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("dictionary ExecuteSQLQueryParameters() error = %v", err)
	}
	if want := []SQLRow{{"name": "m"}, {"name": "z"}, {"name": "m"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("dictionary rows = %#v, want %#v", result.Rows, want)
	}

	numericBatch := ColumnarBatch{Columns: map[string][]interface{}{"value": {int64(0), int64(1), int64(2), int64(3), int64(4)}}, Rows: 5}
	numericBatch.PackNumericColumns()
	result, err = ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('events') AS src WHERE src.value BETWEEN 1 AND 3 SELECT src.value`, packedDictionaryQueryResolver{batch: numericBatch}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("numeric ExecuteSQLQueryParameters() error = %v", err)
	}
	if want := []SQLRow{{"value": int64(1)}, {"value": int64(2)}, {"value": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("numeric rows = %#v, want %#v", result.Rows, want)
	}
}

type ch048BetweenBenchmarkCase struct {
	name  string
	query string
	batch ColumnarBatch
	want  int
}

func ch048BetweenBenchmarkCases() []ch048BetweenBenchmarkCase {
	return []ch048BetweenBenchmarkCase{
		{
			name:  "dictionary_packed",
			query: `FROM CACHE('events') AS src WHERE src.name BETWEEN 'name-16' AND 'name-47' SELECT src.name`,
			batch: newCH048BetweenDictionaryBatch(true),
			want:  2048,
		},
		{
			name:  "numeric_packed",
			query: `FROM CACHE('events') AS src WHERE src.value BETWEEN 1024 AND 3071 SELECT src.value`,
			batch: newCH048BetweenNumericBatch(true),
			want:  2048,
		},
	}
}

func benchmarkCH048Between(b *testing.B, matcherFactory func(*sqlQuery, ColumnarBatch) func(int) (bool, error)) {
	for _, test := range ch048BetweenBenchmarkCases() {
		b.Run(test.name, func(b *testing.B) {
			query, err := parseSQLQueryParameters(test.query, nil)
			if err != nil {
				b.Fatal(err)
			}
			matcher := matcherFactory(query, test.batch)
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				matched := 0
				for row := 0; row < test.batch.Rows; row++ {
					ok, err := matcher(row)
					if err != nil {
						b.Fatal(err)
					}
					if ok {
						matched++
					}
				}
				if matched != test.want {
					b.Fatalf("matched = %d, want %d", matched, test.want)
				}
				ch048BetweenBenchmarkSink = matched
			}
			b.ReportMetric(float64(test.batch.Rows), "rows/op")
		})
	}
}

func BenchmarkCH048BetweenFallback(b *testing.B) {
	benchmarkCH048Between(b, func(query *sqlQuery, batch ColumnarBatch) func(int) (bool, error) {
		return func(rowIndex int) (bool, error) {
			value, err := evalSQLStreamExpr(query.where, newSQLColumnarSourceExecRow(query.from.alias, &batch, rowIndex), nil)
			if err != nil {
				return false, err
			}
			return sqlTruthy(value), nil
		}
	})
}

func BenchmarkCH048BetweenFastPath(b *testing.B) {
	benchmarkCH048Between(b, func(query *sqlQuery, batch ColumnarBatch) func(int) (bool, error) {
		return sqlColumnarQueryRowsMatcher(query, batch, nil)
	})
}

func newCH048BetweenDictionaryBatch(packed bool) ColumnarBatch {
	const rows = 4096
	const values = 64
	codes := make([]uint32, rows)
	dictionaryValues := make([]string, values)
	for index := range dictionaryValues {
		dictionaryValues[index] = fmt.Sprintf("name-%02d", index)
	}
	for index := range codes {
		codes[index] = uint32(index % values)
	}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"name": {Values: dictionaryValues, Codes: codes},
	}, Rows: rows}
	if packed {
		batch.PackDictionaryCodes()
	}
	return batch
}

func newCH048BetweenNumericBatch(packed bool) ColumnarBatch {
	const rows = 4096
	values := make([]interface{}, rows)
	for index := range values {
		values[index] = int64(index)
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"value": values}, Rows: rows}
	if packed {
		batch.PackNumericColumns()
	}
	return batch
}
