package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLLimitWithTies(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		options SQLQueryOptions
		want    []SQLRow
	}{
		{
			name:  "boundary ties",
			query: "FROM VALUES (1, 10), (2, 10), (3, 9), (4, 8) AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 1 WITH TIES",
			want: []SQLRow{
				{"id": int64(1), "score": int64(10)},
				{"id": int64(2), "score": int64(10)},
			},
		},
		{
			name:  "offset boundary ties",
			query: "FROM VALUES (1, 10), (2, 10), (3, 9), (4, 9), (5, 8) AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 1 WITH TIES OFFSET 2",
			want: []SQLRow{
				{"id": int64(3), "score": int64(9)},
				{"id": int64(4), "score": int64(9)},
			},
		},
		{
			name:  "zero limit",
			query: "FROM VALUES (1, 10), (2, 10) AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 0 WITH TIES",
			want:  []SQLRow{},
		},
		{
			name:  "external spill",
			query: "FROM VALUES (1, 10), (2, 10), (3, 9), (4, 8) AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 1 WITH TIES",
			options: SQLQueryOptions{
				MaxSortBytes:   1,
				SpillDirectory: t.TempDir(),
				MaxSpillBytes:  1 << 20,
			},
			want: []SQLRow{
				{"id": int64(1), "score": int64(10)},
				{"id": int64(2), "score": int64(10)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQueryContext(context.Background(), test.query, nil, test.options)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestSQLLimitWithTiesRequiresOrderedFiniteLimit(t *testing.T) {
	queries := []string{
		"FROM VALUES (1), (2) AS items(id) SELECT items.id LIMIT 1 WITH TIES",
		"FROM VALUES (1), (2) AS items(id) SELECT items.id ORDER BY items.id LIMIT -1 WITH TIES",
	}
	for _, query := range queries {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, nil, SQLQueryOptions{}); err == nil {
			t.Fatalf("query %q error = nil, want validation error", query)
		}
	}
}

func TestSQLLimitWithTiesRowsUsesMaterializedBoundary(t *testing.T) {
	query := "FROM VALUES (1, 10), (2, 10), (3, 9) AS items(id, score) SELECT items.id, items.score ORDER BY items.score DESC LIMIT 1 WITH TIES"
	rows := make([]SQLRow, 0, 2)
	err := ExecuteSQLQueryRows(context.Background(), query, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []SQLRow{
		{"id": int64(1), "score": int64(10)},
		{"id": int64(2), "score": int64(10)},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %#v, want %#v", rows, want)
	}
}
