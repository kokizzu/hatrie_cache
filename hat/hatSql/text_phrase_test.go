package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLContainsPhraseAndProximity(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "text": "Quick brown fox"},
		{"id": int64(2), "text": "Quick red brown fox"},
		{"id": int64(3), "text": "Fox brown quick"},
		{"id": int64(4), "text": "go go now"},
	}
	resolver := SQLSourceResolverFunc(func(name, key string) ([]SQLRow, error) {
		if name != "CACHE" || key != "docs" {
			return nil, nil
		}
		return rows, nil
	})

	for _, test := range []struct {
		name  string
		query string
		want  []SQLRow
	}{
		{name: "phrase", query: "FROM CACHE('docs') SELECT id WHERE CONTAINS_PHRASE(text, 'quick brown')", want: []SQLRow{{"id": int64(1)}}},
		{name: "proximity", query: "FROM CACHE('docs') SELECT id WHERE CONTAINS_PROXIMITY(text, 'quick fox', 2)", want: []SQLRow{{"id": int64(1)}, {"id": int64(2)}}},
		{name: "proximity bound", query: "FROM CACHE('docs') SELECT id WHERE CONTAINS_PROXIMITY(text, 'quick fox', 1)", want: []SQLRow{{"id": int64(1)}}},
		{name: "repeated phrase", query: "FROM CACHE('docs') SELECT id WHERE CONTAINS_PHRASE(text, 'go go')", want: []SQLRow{{"id": int64(4)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := ExecuteSQLQueryContext(context.Background(), test.query, resolver, SQLQueryOptions{})
			if err != nil {
				t.Fatalf("query error = %v", err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}
}

func TestSQLContainsPhraseRejectsInvalidProximityDistance(t *testing.T) {
	rows := []SQLRow{{"text": "quick brown fox"}}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) { return rows, nil })
	for _, query := range []string{
		"FROM CACHE('docs') SELECT text WHERE CONTAINS_PROXIMITY(text, 'quick fox', -1)",
		"FROM CACHE('docs') SELECT text WHERE CONTAINS_PROXIMITY(text, 'quick fox', 1.5)",
	} {
		if _, err := ExecuteSQLQueryContext(context.Background(), query, resolver, SQLQueryOptions{}); err == nil {
			t.Fatalf("query %q returned nil error, want invalid distance error", query)
		}
	}
}
