package hatSql_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

const columnarNGramBenchmarkRows = 20_000

func BenchmarkColumnarNGramLikeFilter(b *testing.B) {
	b.Run("with_ngram_segments", func(b *testing.B) {
		resolver := newColumnarNGramBenchmarkResolver(true)
		benchmarkColumnarNGramQuery(b, resolver)
	})
	b.Run("without_ngram_segments", func(b *testing.B) {
		resolver := newColumnarNGramBenchmarkResolver(false)
		benchmarkColumnarNGramQuery(b, resolver)
	})
}

func benchmarkColumnarNGramQuery(b *testing.B, resolver ngramSegmentResolver) {
	b.Helper()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT name WHERE name LIKE '%needle%'", resolver, nil, hatSql.QueryOptions{})
		if err != nil || len(result.Rows) != 1 {
			b.Fatalf("query = %#v, error = %v", result, err)
		}
	}
}

func newColumnarNGramBenchmarkResolver(withNGrams bool) ngramSegmentResolver {
	values := make([]interface{}, columnarNGramBenchmarkRows)
	filters := make([]hatSql.ColumnarStringNGramBloomSegment, 0, (columnarNGramBenchmarkRows+255)/256)
	for start := 0; start < columnarNGramBenchmarkRows; start += 256 {
		end := start + 256
		if end > columnarNGramBenchmarkRows {
			end = columnarNGramBenchmarkRows
		}
		filter := hatSql.ColumnarStringNGramBloomSegment{}
		for index := start; index < end; index++ {
			value := "value-" + strconv.Itoa(index)
			if index == columnarNGramBenchmarkRows-1 {
				value = "needle-value"
			}
			values[index] = value
			filter.Add(value)
		}
		filters = append(filters, filter)
	}
	segments := &hatSql.ColumnarNumericSegments{RowsPerSegment: 256}
	if withNGrams {
		segments.StringNGramBloomFilters = map[string][]hatSql.ColumnarStringNGramBloomSegment{"name": filters}
	}
	return ngramSegmentResolver{batch: hatSql.ColumnarBatch{Columns: map[string][]interface{}{"name": values}, Rows: len(values)}, segments: segments}
}
