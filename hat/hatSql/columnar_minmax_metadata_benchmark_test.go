package hatSql

import (
	"context"
	"testing"
)

var sqlColumnarMinMaxBenchmarkResult SQLQueryResult

func newSQLColumnarMinMaxBenchmarkResolver(withMetadata bool) sqlColumnarMinMaxMetadataResolver {
	const rows = 100000
	values := make([]interface{}, rows)
	for row := range values {
		values[row] = int64(row)
	}
	resolver := sqlColumnarMinMaxMetadataResolver{
		batch: ColumnarBatch{
			Columns: map[string][]interface{}{"score": values},
			Rows:    rows,
		},
	}
	if withMetadata {
		const rowsPerSegment = 256
		segments := make([]ColumnarNumericSegment, (rows+rowsPerSegment-1)/rowsPerSegment)
		for index := range segments {
			start := index * rowsPerSegment
			end := start + rowsPerSegment - 1
			if end >= rows {
				end = rows - 1
			}
			segments[index] = ColumnarNumericSegment{Minimum: float64(start), Maximum: float64(end), Valid: true}
		}
		resolver.segments = &ColumnarNumericSegments{
			RowsPerSegment: rowsPerSegment,
			Columns:        map[string][]ColumnarNumericSegment{"score": segments},
		}
	}
	return resolver
}

func BenchmarkSQLColumnarMinMaxMetadata(b *testing.B) {
	query := "SELECT MIN(score) AS low, MAX(score) AS high FROM CACHE('items')"
	for _, test := range []struct {
		name     string
		resolver sqlColumnarMinMaxMetadataResolver
	}{
		{name: "legacy_row_scan", resolver: newSQLColumnarMinMaxBenchmarkResolver(false)},
		{name: "segment_metadata", resolver: newSQLColumnarMinMaxBenchmarkResolver(true)},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryContext(context.Background(), query, test.resolver, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				sqlColumnarMinMaxBenchmarkResult = result
			}
		})
	}
}
