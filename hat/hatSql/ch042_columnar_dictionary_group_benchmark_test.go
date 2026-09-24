package hatSql

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkCH042ColumnarDictionaryGroup(b *testing.B) {
	query := "SELECT region, COUNT(*) AS n, SUM(score) AS total, AVG(score) AS average FROM CACHE('items') GROUP BY region"
	for _, rowCount := range []int{1024, 20000} {
		batch, rows := newCH042ColumnarDictionaryGroupInput(rowCount)
		b.Run(fmt.Sprintf("rows_%d", rowCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result, err := ExecuteSQLQueryParameters(context.Background(), query, &sqlVectorColumnarResolver{batch: batch, rows: rows}, nil, SQLQueryOptions{})
				if err != nil || len(result.Rows) != 64 {
					b.Fatalf("query error = %v, rows = %d", err, len(result.Rows))
				}
			}
		})
	}
}

func newCH042ColumnarDictionaryGroupInput(rows int) (ColumnarBatch, []Row) {
	const groupCount = 64
	values := make([]string, groupCount)
	for index := range values {
		values[index] = fmt.Sprintf("region-%02d", index)
	}
	codes := make([]uint32, rows)
	scores := make([]interface{}, rows)
	rowValues := make([]Row, rows)
	for index := range codes {
		code := uint32(index % groupCount)
		codes[index] = code
		score := int64(index%100 + 1)
		scores[index] = score
		rowValues[index] = Row{"region": values[code], "score": score}
	}
	return ColumnarBatch{
		Columns: map[string][]interface{}{"score": scores},
		Dictionaries: map[string]DictionaryColumn{
			"region": {Values: values, Codes: codes, codesTrusted: true},
		},
		Rows: rows,
	}, rowValues
}
