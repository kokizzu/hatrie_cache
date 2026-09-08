package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func sqlTrustedDictionary(values []string, codes []uint32) DictionaryColumn {
	return DictionaryColumn{Values: values, Codes: codes, codesTrusted: true}
}

type sqlColumnarDictionaryCountMetadataResolver struct {
	batch ColumnarBatch
}

func (resolver sqlColumnarDictionaryCountMetadataResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for dictionary count metadata")
}

func (resolver sqlColumnarDictionaryCountMetadataResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func TestSQLColumnarDictionaryCountMetadataHandlesMissingEquality(t *testing.T) {
	resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": sqlTrustedDictionary([]string{"red", "blue"}, []uint32{0, 1, 0, 1}),
		},
		Rows: 4,
	}}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'green'", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(0)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryCountMetadataHandlesMissingInequality(t *testing.T) {
	resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": sqlTrustedDictionary([]string{"red", "blue"}, []uint32{0, 1, 0, 1}),
		},
		Rows: 4,
	}}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team != 'green'", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(4)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryCountMetadataHandlesMissingINValues(t *testing.T) {
	resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": sqlTrustedDictionary([]string{"red", "blue"}, []uint32{0, 1, 0, 1}),
		},
		Rows: 4,
	}}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team IN ('green', 'yellow')", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(0)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryCountMetadataFallsBackForMatchedValue(t *testing.T) {
	resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": sqlTrustedDictionary([]string{"red", "blue"}, []uint32{0, 1, 0, 1}),
		},
		Rows: 4,
	}}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'red'", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryCountMetadataDoesNotIgnoreConjunctions(t *testing.T) {
	resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
		Columns: map[string][]interface{}{"score": {int64(1), int64(5), int64(2), int64(6)}},
		Dictionaries: map[string]DictionaryColumn{
			"team": sqlTrustedDictionary([]string{"red", "blue"}, []uint32{0, 1, 0, 1}),
		},
		Rows: 4,
	}}

	result, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team != 'green' AND score >= 5", resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{{"total": int64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLColumnarDictionaryCountMetadataRequiresCountStar(t *testing.T) {
	query, err := parseSQLQueryWithCache("SELECT COUNT(team) FROM CACHE('items') WHERE team = 'green'", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	aggregates, _, _, ok := sqlColumnarNumericAggregates(query, nil)
	if !ok {
		t.Fatal("sqlColumnarNumericAggregates unexpectedly rejected dictionary count query")
	}
	if _, exact := sqlColumnarDictionaryCountMetadata(aggregates, query.where.kind, query.where.op, 0, true, true, "=", false, false, false, nil); exact {
		t.Fatal("sqlColumnarDictionaryCountMetadata accepted COUNT(field), want fallback")
	}
}

func TestSQLColumnarDictionaryCountMetadataPreservesInvalidCodeErrors(t *testing.T) {
	tests := []struct {
		name       string
		dictionary DictionaryColumn
	}{
		{
			name:       "legacy codes",
			dictionary: DictionaryColumn{Values: []string{"red"}, Codes: []uint32{1}},
		},
		{
			name:       "packed codes",
			dictionary: DictionaryColumn{Values: []string{"red"}, PackedCodes: []byte{1}, CodeWidth: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := sqlColumnarDictionaryCountMetadataResolver{batch: ColumnarBatch{
				Dictionaries: map[string]DictionaryColumn{"team": test.dictionary},
				Rows:         1,
			}}
			_, err := ExecuteSQLQueryContext(context.Background(), "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'green'", resolver, SQLQueryOptions{})
			if err == nil {
				t.Fatal("invalid dictionary code unexpectedly accepted")
			}
		})
	}
}

func TestRepeatedStringDictionaryTrustSurvivesPacking(t *testing.T) {
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{
			"team": {"red", "blue", "red", "blue"},
		},
		Rows: 4,
	}
	batch.EncodeRepeatedStrings()
	dictionary := batch.Dictionaries["team"]
	if !dictionary.codesTrusted {
		t.Fatal("repeated-string encoder did not mark codes trusted")
	}
	batch.PackDictionaryValues()
	batch.PackDictionaryCodes()
	if !batch.Dictionaries["team"].codesTrusted {
		t.Fatal("dictionary packing dropped trusted-code marker")
	}
}
