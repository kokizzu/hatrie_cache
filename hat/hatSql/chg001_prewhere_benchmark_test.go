package hatSql

import (
	"context"
	"errors"
	"testing"
)

const (
	chg001BenchmarkRows       = 20_000
	chg001BenchmarkPayloadLen = 1_024
)

type chg001LegacyBenchmarkResolver struct {
	batch      ColumnarBatch
	sourceByte int64
}

func (resolver *chg001LegacyBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a columnar benchmark")
}

func (resolver *chg001LegacyBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.sourceByte += chg001BenchmarkBytesForFields(fields, chg001BenchmarkRows, chg001BenchmarkPayloadLen)
	return resolver.batch, true, nil
}

type chg001PrewhereBenchmarkResolver struct {
	batch      ColumnarBatch
	sourceByte int64
}

func (resolver *chg001PrewhereBenchmarkResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("row source must not be resolved for a prewhere benchmark")
}

func (resolver *chg001PrewhereBenchmarkResolver) ResolveSQLColumnarSource(_ string, _ string, _ []string) (ColumnarBatch, bool, error) {
	return resolver.batch, true, nil
}

func (resolver *chg001PrewhereBenchmarkResolver) ResolveSQLColumnarPrewhere(_ string, _ string, fields []string) (ColumnarBatch, bool, error) {
	resolver.sourceByte += chg001BenchmarkBytesForFields(fields, chg001BenchmarkRows, chg001BenchmarkPayloadLen)
	return ColumnarBatch{
		Columns: map[string][]interface{}{"score": resolver.batch.Columns["score"]},
		Rows:    resolver.batch.Rows,
	}, true, nil
}

func (resolver *chg001PrewhereBenchmarkResolver) ResolveSQLColumnarProjection(_ string, _ string, fields []string, rowIndexes []int) (ColumnarBatch, bool, error) {
	resolver.sourceByte += chg001BenchmarkBytesForFields(fields, len(rowIndexes), chg001BenchmarkPayloadLen)
	columns := make(map[string][]interface{}, len(fields))
	for _, field := range fields {
		values := make([]interface{}, len(rowIndexes))
		for index, rowIndex := range rowIndexes {
			values[index] = resolver.batch.Columns[field][rowIndex]
		}
		columns[field] = values
	}
	return ColumnarBatch{Columns: columns, Rows: len(rowIndexes)}, true, nil
}

func BenchmarkCHG001LegacyColumnarWideSelective(b *testing.B) {
	resolver := chg001BenchmarkResolver()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := ExecuteSQLQueryRows(ctx, "SELECT id, payload FROM CACHE('items') WHERE score = 0", resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(resolver.sourceByte)/float64(b.N), "source-bytes/op")
}

func BenchmarkCHG001AutomaticPrewhereWideSelective(b *testing.B) {
	resolver := chg001PrewhereBenchmarkResolver{batch: chg001BenchmarkBatch()}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := ExecuteSQLQueryRows(ctx, "SELECT id, payload FROM CACHE('items') WHERE score = 0", &resolver, nil, SQLQueryOptions{}, func([]string, SQLRow) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(resolver.sourceByte)/float64(b.N), "source-bytes/op")
}

func chg001BenchmarkResolver() *chg001LegacyBenchmarkResolver {
	return &chg001LegacyBenchmarkResolver{batch: chg001BenchmarkBatch()}
}

func chg001BenchmarkBatch() ColumnarBatch {
	ids := make([]interface{}, chg001BenchmarkRows)
	scores := make([]interface{}, chg001BenchmarkRows)
	payloads := make([]interface{}, chg001BenchmarkRows)
	payload := string(make([]byte, chg001BenchmarkPayloadLen))
	for index := 0; index < chg001BenchmarkRows; index++ {
		ids[index] = int64(index)
		scores[index] = int64(index % 100)
		payloads[index] = payload
	}
	return ColumnarBatch{Columns: map[string][]interface{}{
		"id":      ids,
		"score":   scores,
		"payload": payloads,
	}, Rows: chg001BenchmarkRows}
}

func chg001BenchmarkBytesForFields(fields []string, rows, payloadLen int) int64 {
	var bytesPerRow int64
	for _, field := range fields {
		switch field {
		case "id", "score":
			bytesPerRow += 8
		case "payload":
			bytesPerRow += int64(payloadLen)
		}
	}
	return int64(rows) * bytesPerRow
}
