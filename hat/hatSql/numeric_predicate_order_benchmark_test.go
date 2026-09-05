package hatSql

import (
	"context"
	"testing"
)

var sqlNumericPredicateOrderBenchmarkSink SQLQueryResult

type sqlNumericPredicateOrderBenchmarkResolver struct {
	batch    ColumnarBatch
	segments *ColumnarNumericSegments
}

func (resolver sqlNumericPredicateOrderBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func (resolver sqlNumericPredicateOrderBenchmarkResolver) ResolveSQLColumnarSource(string, string, []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver sqlNumericPredicateOrderBenchmarkResolver) BorrowSQLColumnarSourceSegments(string, string, []string) (ColumnarBatch, *ColumnarNumericSegments, bool, error) {
	return resolver.batch, resolver.segments, true, nil
}

func BenchmarkSQLColumnarNumericPredicateOrder(b *testing.B) {
	const (
		rows          = 262144
		rowsPerMark   = 256
		markCount     = rows / rowsPerMark
		query         = "FROM CACHE('events') AS event WHERE event.broad >= 0 AND event.narrow = 1 SELECT event.broad, event.narrow"
		matchingMarks = 1
	)
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{
			"broad":  make([]interface{}, rows),
			"narrow": make([]interface{}, rows),
		},
		Rows: rows,
	}
	marks := &ColumnarNumericSegments{
		RowsPerSegment: rowsPerMark,
		Columns: map[string][]ColumnarNumericSegment{
			"broad":  make([]ColumnarNumericSegment, markCount),
			"narrow": make([]ColumnarNumericSegment, markCount),
		},
	}
	for row := 0; row < rows; row++ {
		batch.Columns["broad"][row] = float64(1)
		if row < rowsPerMark {
			batch.Columns["narrow"][row] = float64(1)
		} else {
			batch.Columns["narrow"][row] = float64(0)
		}
	}
	for mark := 0; mark < markCount; mark++ {
		marks.Columns["broad"][mark] = ColumnarNumericSegment{Minimum: 1, Maximum: 1, Valid: true}
		value := float64(0)
		if mark < matchingMarks {
			value = 1
		}
		marks.Columns["narrow"][mark] = ColumnarNumericSegment{Minimum: value, Maximum: value, Valid: true}
	}
	legacy := sqlNumericPredicateOrderBenchmarkResolver{batch: batch}
	ordered := sqlNumericPredicateOrderBenchmarkResolver{batch: batch, segments: marks}
	b.Run("legacy_order", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, legacy, nil, SQLQueryOptions{MaxRows: rows + 1})
			if err != nil {
				b.Fatal(err)
			}
			sqlNumericPredicateOrderBenchmarkSink = result
		}
	})
	b.Run("mark_selectivity_order", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result, err := ExecuteSQLQueryParameters(context.Background(), query, ordered, nil, SQLQueryOptions{MaxRows: rows + 1})
			if err != nil {
				b.Fatal(err)
			}
			sqlNumericPredicateOrderBenchmarkSink = result
		}
	})
}
