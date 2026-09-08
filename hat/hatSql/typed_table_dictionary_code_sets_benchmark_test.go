package hatSql

import (
	"context"
	"testing"
)

type typedTableDictionaryCodeSetBenchmarkResolver struct {
	batch    ColumnarBatch
	segments *ColumnarNumericSegments
}

func (resolver typedTableDictionaryCodeSetBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver typedTableDictionaryCodeSetBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver typedTableDictionaryCodeSetBenchmarkResolver) BorrowSQLColumnarSourceSegments(string, string, []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, resolver.segments, true, nil
}

func benchmarkTypedTableDictionaryBatch(rows int, trusted bool) ColumnarBatch {
	codes := make([]uint32, rows)
	for row := rows - 256; row < rows; row++ {
		codes[row] = 1
	}
	return ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {
				Values:       []string{"blue", "red"},
				Codes:        codes,
				codesTrusted: trusted,
			},
		},
		Rows: rows,
	}
}

func benchmarkTypedTableDictionarySegments(batch ColumnarBatch, rowsPerSegment int) *ColumnarNumericSegments {
	dictionary := batch.Dictionaries["team"]
	sets := make([]uint64, (batch.Rows+rowsPerSegment-1)/rowsPerSegment)
	for row := 0; row < batch.Rows; row++ {
		code, ok := dictionary.CodeAt(row)
		if !ok || code >= 64 {
			panic("invalid benchmark dictionary")
		}
		sets[row/rowsPerSegment] |= uint64(1) << code
	}
	return &ColumnarNumericSegments{
		RowsPerSegment: rowsPerSegment,
		DictionaryCodeSets: map[string][]uint64{
			"team": sets,
		},
	}
}

var typedTableDictionaryCodeSetBenchmarkResult SQLQueryResult
var typedTableDictionaryCodeSetBenchmarkSegments *ColumnarNumericSegments

func BenchmarkTypedTableColumnarDictionaryCodeSetsQuery(b *testing.B) {
	const rows = 99840
	const rowsPerSegment = 256
	batch := benchmarkTypedTableDictionaryBatch(rows, true)
	withSets := benchmarkTypedTableDictionarySegments(batch, rowsPerSegment)
	withoutSets := &ColumnarNumericSegments{RowsPerSegment: rowsPerSegment}
	query := "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'red'"

	for _, test := range []struct {
		name     string
		segments *ColumnarNumericSegments
	}{
		{name: "without_dictionary_code_sets", segments: withoutSets},
		{name: "with_dictionary_code_sets", segments: withSets},
	} {
		b.Run(test.name, func(b *testing.B) {
			resolver := typedTableDictionaryCodeSetBenchmarkResolver{batch: batch, segments: test.segments}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				result, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 1 || result.Rows[0]["total"] != int64(256) {
					b.Fatalf("result = %#v, want total 256", result.Rows)
				}
				typedTableDictionaryCodeSetBenchmarkResult = result
			}
		})
	}
}

func BenchmarkTypedTableColumnarDictionaryCodeSetsBuild(b *testing.B) {
	const rows = 99840
	const rowsPerSegment = 256
	table, err := NewTypedTable(TypedTableSchema{
		Name: "items",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
		},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:        true,
			RowsPerSegment: rowsPerSegment,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	trusted := benchmarkTypedTableDictionaryBatch(rows, true)
	untrusted := benchmarkTypedTableDictionaryBatch(rows, false)

	for _, test := range []struct {
		name  string
		batch ColumnarBatch
	}{
		{name: "without_dictionary_code_sets", batch: untrusted},
		{name: "with_dictionary_code_sets", batch: trusted},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				typedTableDictionaryCodeSetBenchmarkSegments = table.columnarNumericSegmentsLocked(test.batch)
			}
		})
	}
}
