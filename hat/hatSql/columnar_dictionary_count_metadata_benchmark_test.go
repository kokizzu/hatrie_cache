package hatSql

import (
	"context"
	"testing"
)

var sqlColumnarDictionaryCountBenchmarkResult SQLQueryResult

func benchmarkSQLColumnarDictionaryCountBatch(rows int) ColumnarBatch {
	codes := make([]uint32, rows)
	for index := range codes {
		codes[index] = uint32(index & 1)
	}
	return ColumnarBatch{
		Dictionaries: map[string]DictionaryColumn{
			"team": {Values: []string{"red", "blue"}, Codes: codes, codesTrusted: true},
		},
		Rows: rows,
	}
}

func BenchmarkSQLColumnarDictionaryCountMetadata(b *testing.B) {
	batch := benchmarkSQLColumnarDictionaryCountBatch(100_000)
	context := context.Background()

	b.Run("matching_value_row_scan", func(b *testing.B) {
		resolver := sqlColumnarDictionaryCountMetadataResolver{batch: batch}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryContext(context, "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'red'", resolver, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlColumnarDictionaryCountBenchmarkResult = result
		}
	})

	b.Run("missing_value_row_scan", func(b *testing.B) {
		untrusted := batch.Dictionaries["team"]
		untrusted.codesTrusted = false
		legacyBatch := batch
		legacyBatch.Dictionaries = map[string]DictionaryColumn{"team": untrusted}
		resolver := sqlColumnarDictionaryCountMetadataResolver{batch: legacyBatch}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryContext(context, "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'green'", resolver, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlColumnarDictionaryCountBenchmarkResult = result
		}
	})

	b.Run("missing_value_metadata", func(b *testing.B) {
		resolver := sqlColumnarDictionaryCountMetadataResolver{batch: batch}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryContext(context, "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team = 'green'", resolver, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlColumnarDictionaryCountBenchmarkResult = result
		}
	})

	b.Run("missing_in_row_scan", func(b *testing.B) {
		untrusted := batch.Dictionaries["team"]
		untrusted.codesTrusted = false
		legacyBatch := batch
		legacyBatch.Dictionaries = map[string]DictionaryColumn{"team": untrusted}
		resolver := sqlColumnarDictionaryCountMetadataResolver{batch: legacyBatch}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryContext(context, "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team IN ('green', 'yellow')", resolver, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlColumnarDictionaryCountBenchmarkResult = result
		}
	})

	b.Run("missing_in_metadata", func(b *testing.B) {
		resolver := sqlColumnarDictionaryCountMetadataResolver{batch: batch}
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			result, err := ExecuteSQLQueryContext(context, "SELECT COUNT(*) AS total FROM CACHE('items') WHERE team IN ('green', 'yellow')", resolver, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			sqlColumnarDictionaryCountBenchmarkResult = result
		}
	})
}
