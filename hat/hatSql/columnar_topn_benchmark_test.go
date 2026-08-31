package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type sqlColumnarTopNBenchmarkResolver struct {
	batch ColumnarBatch
	rows  []SQLRow
}

func (resolver sqlColumnarTopNBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

func (resolver sqlColumnarTopNBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

type sqlTopNRowsBenchmarkResolver struct{ rows []SQLRow }

func (resolver sqlTopNRowsBenchmarkResolver) ResolveSQLSource(string, string) ([]SQLRow, error) {
	return resolver.rows, nil
}

type sqlSegmentedTopNBenchmarkResolver struct {
	sqlColumnarTopNBenchmarkResolver
	segments *ColumnarNumericSegments
}

func (resolver sqlSegmentedTopNBenchmarkResolver) BorrowSQLColumnarSourceSegments(string, string, []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, resolver.segments, true, nil
}

func BenchmarkExecuteSQLQueryColumnarTopN(b *testing.B) {
	const count = 20_000
	ids := make([]interface{}, count)
	scores := make([]interface{}, count)
	rows := make([]SQLRow, count)
	codes := make([]uint32, count)
	teams := make([]string, 20)
	for index := range teams {
		teams[index] = "team-" + strconv.Itoa(index)
	}
	for index := range rows {
		id := int64(index)
		score := int64(index)
		codes[index] = uint32((index * 7) % len(teams))
		ids[index] = id
		scores[index] = score
		rows[index] = SQLRow{"id": id, "score": score, "team": teams[codes[index]]}
	}
	columnar := sqlColumnarTopNBenchmarkResolver{
		batch: ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Dictionaries: map[string]DictionaryColumn{"team": {Values: teams, Codes: codes}}, Rows: count},
		rows:  rows,
	}
	const query = "SELECT id FROM CACHE('items') WHERE team = 'team-2' AND score >= 10000 ORDER BY score DESC LIMIT 50"
	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "columnar_top_n", resolver: columnar},
		{name: "full_materialized", resolver: sqlTopNRowsBenchmarkResolver{rows: rows}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, benchmark.resolver, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 50 {
					b.Fatalf("rows = %d, want 50", len(result.Rows))
				}
			}
		})
	}
}

func BenchmarkExecuteSQLQueryColumnarTopNSegmentPruning(b *testing.B) {
	const count = 20_000
	const rowsPerSegment = 256
	ids := make([]interface{}, count)
	scores := make([]interface{}, count)
	segments := make([]ColumnarNumericSegment, 0, (count+rowsPerSegment-1)/rowsPerSegment)
	for start := 0; start < count; start += rowsPerSegment {
		end := start + rowsPerSegment
		if end > count {
			end = count
		}
		base := int64(start / rowsPerSegment * 1_000)
		for index := start; index < end; index++ {
			ids[index] = int64(index)
			scores[index] = base + int64(index-start)
		}
		segments = append(segments, ColumnarNumericSegment{Minimum: float64(base), Maximum: float64(base + int64(end-start-1)), Valid: true})
	}
	batch := ColumnarBatch{Columns: map[string][]interface{}{"id": ids, "score": scores}, Rows: count}
	base := sqlColumnarTopNBenchmarkResolver{batch: batch}
	segmented := sqlSegmentedTopNBenchmarkResolver{
		sqlColumnarTopNBenchmarkResolver: base,
		segments:                         &ColumnarNumericSegments{RowsPerSegment: rowsPerSegment, Columns: map[string][]ColumnarNumericSegment{"score": segments}},
	}
	const query = "SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 50"
	for _, benchmark := range []struct {
		name     string
		resolver SQLSourceResolver
	}{
		{name: "full_columnar_top_n", resolver: base},
		{name: "numeric_segment_pruning", resolver: segmented},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, benchmark.resolver, nil, SQLQueryOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(result.Rows) != 50 {
					b.Fatalf("rows = %d, want 50", len(result.Rows))
				}
			}
		})
	}
}
