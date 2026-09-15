package hatSql

import (
	"context"
	"strconv"
	"testing"
)

type ch031BaselineJSONResolver struct {
	rows []Row
}

func (resolver ch031BaselineJSONResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

func newCH031BaselineJSONResolver(rows int) ch031BaselineJSONResolver {
	values := make([]Row, rows)
	for row := range values {
		values[row] = Row{"doc": `{"id":` + strconv.Itoa(row) + `,"large":"` + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + `"}`}
	}
	return ch031BaselineJSONResolver{rows: values}
}

func BenchmarkCH031JSONValueBaseline(b *testing.B) {
	resolver := newCH031BaselineJSONResolver(4096)
	query := "SELECT JSON_VALUE(doc, '$.id') AS id FROM CACHE('items') WHERE JSON_VALUE(doc, '$.id') >= 2048"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQueryParameters(context.Background(), query, resolver, nil, SQLQueryOptions{})
		if err != nil || len(result.Rows) != 2048 {
			b.Fatalf("baseline result = %d rows, err=%v", len(result.Rows), err)
		}
	}
}
