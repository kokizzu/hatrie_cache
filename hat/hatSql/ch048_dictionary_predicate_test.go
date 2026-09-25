package hatSql

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

func TestCH048DictionaryOrderedPredicateRecognizerAndCodes(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE 'm' <= src.name SELECT src.name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	dictionary := DictionaryColumn{Values: []string{"a", "m", "z"}, Codes: []uint32{0, 1, 2, 1}}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"name": dictionary}, Rows: len(dictionary.Codes)}
	gotDictionary, gotCodes, ok := sqlColumnarDictionaryOrderedPredicate(query.where, query.from.alias, batch)
	if !ok {
		t.Fatal("sqlColumnarDictionaryOrderedPredicate() unexpectedly rejected binary ordering")
	}
	if !reflect.DeepEqual(gotDictionary.Values, dictionary.Values) {
		t.Fatalf("dictionary values = %#v, want %#v", gotDictionary.Values, dictionary.Values)
	}
	for code, want := range []bool{false, true, true} {
		if got := gotCodes.matches(uint32(code)); got != want {
			t.Fatalf("dictionary code %d = %t, want %t", code, got, want)
		}
	}
	if gotCodes.wide != nil {
		t.Fatalf("small dictionary used wide code mask: %#v", gotCodes)
	}

	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if _, _, ok := sqlColumnarDictionaryOrderedPredicate(query.where, query.from.alias, batch); ok {
		t.Fatal("UnicodeCI dictionary ordering unexpectedly took the binary fast path")
	}
}

func TestCH048DictionaryOrderedPredicateSupportsWideDictionaries(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') WHERE name >= 'name-64' SELECT name`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	values := make([]string, 65)
	for index := range values {
		values[index] = "name-" + strconv.Itoa(index)
	}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{
		"name": {Values: values, Codes: []uint32{0, 64}},
	}, Rows: 2}
	_, mask, ok := sqlColumnarDictionaryOrderedPredicate(query.where, "", batch)
	if !ok {
		t.Fatal("wide dictionary ordering unexpectedly rejected valid values")
	}
	if len(mask.wide) != len(values) {
		t.Fatalf("wide dictionary mask length = %d, want %d", len(mask.wide), len(values))
	}
	if mask.matches(0) || !mask.matches(64) {
		t.Fatalf("wide dictionary mask matches = %#v, want only code 64", mask)
	}
}

func TestCH048PackedDictionaryOrderedQueriesPreserveResults(t *testing.T) {
	legacy := DictionaryColumn{Values: []string{"a", "m", "z"}, Codes: []uint32{0, 1, 2, 1}}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"name": legacy}, Rows: len(legacy.Codes)}
	batch.PackDictionaryCodes()
	queries := []struct {
		query string
		want  []bool
	}{
		{query: `FROM CACHE('events') AS src WHERE src.name < 'm' SELECT src.name`, want: []bool{true, false, false, false}},
		{query: `FROM CACHE('events') AS src WHERE src.name <= 'm' SELECT src.name`, want: []bool{true, true, false, true}},
		{query: `FROM CACHE('events') AS src WHERE src.name > 'm' SELECT src.name`, want: []bool{false, false, true, false}},
		{query: `FROM CACHE('events') AS src WHERE src.name >= 'm' SELECT src.name`, want: []bool{false, true, true, true}},
		{query: `FROM CACHE('events') AS src WHERE 'm' <= src.name SELECT src.name`, want: []bool{false, true, true, true}},
	}
	for _, test := range queries {
		t.Run(test.query, func(t *testing.T) {
			query, err := parseSQLQueryParameters(test.query, nil)
			if err != nil {
				t.Fatalf("parseSQLQueryParameters() error = %v", err)
			}
			matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
			got := make([]bool, batch.Rows)
			for row := range got {
				got[row], err = matcher(row)
				if err != nil {
					t.Fatalf("matcher(%d) error = %v", row, err)
				}
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("matches = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestCH048PackedDictionaryOrderedQueryExecution(t *testing.T) {
	legacy := DictionaryColumn{Values: []string{"a", "m", "z"}, Codes: []uint32{0, 1, 2, 1}}
	batch := ColumnarBatch{Dictionaries: map[string]DictionaryColumn{"name": legacy}, Rows: len(legacy.Codes)}
	batch.PackDictionaryCodes()
	resolver := packedDictionaryQueryResolver{batch: batch}
	result, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('events') AS src WHERE src.name >= 'm' SELECT src.name`, resolver, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	want := []SQLRow{{"name": "m"}, {"name": "z"}, {"name": "m"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func BenchmarkCH048DictionaryOrderedComparison(b *testing.B) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.name >= 'name-4' SELECT src.name`, nil)
	if err != nil {
		b.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	for _, packed := range []bool{false, true} {
		name := "legacy_uint32"
		if packed {
			name = "packed_uint8"
		}
		b.Run(name, func(b *testing.B) {
			batch := newCH048DictionaryOrderedBatch(packed)
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
				if matched != batch.Rows/2 {
					b.Fatalf("matched = %d, want %d", matched, batch.Rows/2)
				}
			}
			b.ReportMetric(float64(batch.Rows), "rows/op")
		})
	}
}

func newCH048DictionaryOrderedBatch(packed bool) ColumnarBatch {
	const rows = 4096
	const values = 8
	codes := make([]uint32, rows)
	dictionaryValues := make([]string, values)
	for index := range dictionaryValues {
		dictionaryValues[index] = "name-" + strconv.Itoa(index)
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
