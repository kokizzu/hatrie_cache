package hatSql

import (
	"context"
	"reflect"
	"testing"
)

type round16RowsOnlyResolver struct {
	rows []Row
}

func (resolver round16RowsOnlyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return resolver.rows, nil
}

var round16LimitWithTiesBenchmarkSink int

func TestRound16ColumnarLimitWithTies(t *testing.T) {
	tests := []struct {
		name  string
		query string
		batch ColumnarBatch
		want  []SQLRow
	}{
		{
			name:  "descending includes all boundary ties",
			query: "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 2 WITH TIES",
			batch: ColumnarBatch{Columns: map[string][]interface{}{
				"id":    {int64(1), int64(2), int64(3), int64(4)},
				"score": {int64(10), int64(9), int64(9), int64(8)},
			}, Rows: 4},
			want: []SQLRow{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}},
		},
		{
			name:  "offset applies before boundary ties",
			query: "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 1 WITH TIES OFFSET 1",
			batch: ColumnarBatch{Columns: map[string][]interface{}{
				"id":    {int64(1), int64(2), int64(3), int64(4), int64(5)},
				"score": {int64(10), int64(9), int64(9), int64(8), int64(8)},
			}, Rows: 5},
			want: []SQLRow{{"id": int64(2)}, {"id": int64(3)}},
		},
		{
			name:  "all order fields define the tie",
			query: "SELECT id FROM CACHE('items') ORDER BY score DESC, team ASC LIMIT 1 WITH TIES",
			batch: ColumnarBatch{Columns: map[string][]interface{}{
				"id":    {int64(1), int64(2), int64(3), int64(4)},
				"score": {int64(10), int64(10), int64(10), int64(9)},
				"team":  {"core", "core", "ops", "core"},
			}, Rows: 4},
			want: []SQLRow{{"id": int64(1)}, {"id": int64(2)}},
		},
		{
			name:  "ascending includes all boundary ties",
			query: "SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2 WITH TIES",
			batch: ColumnarBatch{Columns: map[string][]interface{}{
				"id":    {int64(1), int64(2), int64(3), int64(4)},
				"score": {int64(1), int64(2), int64(2), int64(3)},
			}, Rows: 4},
			want: []SQLRow{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(3)}},
		},
		{
			name:  "filter is applied during the tie pass",
			query: "SELECT id FROM CACHE('items') WHERE active = 1 ORDER BY score DESC LIMIT 1 WITH TIES",
			batch: ColumnarBatch{Columns: map[string][]interface{}{
				"id":     {int64(1), int64(2), int64(3), int64(4)},
				"score":  {int64(5), int64(5), int64(5), int64(4)},
				"active": {int64(1), int64(1), int64(0), int64(1)},
			}, Rows: 4},
			want: []SQLRow{{"id": int64(1)}, {"id": int64(2)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &sqlColumnarQueryRowsResolver{batch: test.batch}
			result, err := ExecuteSQLQueryParameters(context.Background(), test.query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func BenchmarkRound16ColumnarLimitWithTies(b *testing.B) {
	const rows = 20_000
	rowSource := make([]Row, rows)
	ids := make([]interface{}, rows)
	scores := make([]interface{}, rows)
	for index := 0; index < rows; index++ {
		id := int64(index)
		score := int64(index / 3)
		rowSource[index] = Row{"id": id, "score": score}
		ids[index], scores[index] = id, score
	}
	query := "SELECT id FROM CACHE('items') ORDER BY score DESC LIMIT 20 WITH TIES"
	ctx := context.Background()
	b.Run("generic-row-fallback", func(b *testing.B) {
		resolver := round16RowsOnlyResolver{rows: rowSource}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryParameters(ctx, query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			round16LimitWithTiesBenchmarkSink = len(result.Rows)
		}
	})
	b.Run("columnar-specialized", func(b *testing.B) {
		resolver := &sqlColumnarQueryRowsResolver{batch: ColumnarBatch{
			Columns: map[string][]interface{}{"id": ids, "score": scores},
			Rows:    rows,
		}}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			result, err := ExecuteSQLQueryParameters(ctx, query, resolver, nil, SQLQueryOptions{})
			if err != nil {
				b.Fatal(err)
			}
			round16LimitWithTiesBenchmarkSink = len(result.Rows)
		}
	})
}
