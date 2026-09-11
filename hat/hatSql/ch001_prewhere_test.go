package hatSql

import (
	"context"
	"reflect"
	"testing"
)

func TestSQLExplicitPrewhereMatchesCombinedPredicate(t *testing.T) {
	rows := []SQLRow{
		{"id": int64(1), "keep": true, "score": int64(10)},
		{"id": int64(2), "keep": false, "score": int64(100)},
		{"id": int64(3), "keep": true, "score": int64(90)},
		{"id": int64(4), "keep": nil, "score": int64(100)},
		{"id": int64(5), "keep": true, "score": int64(110)},
	}
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return rows, nil
	})

	prewhere, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('items') AS item PREWHERE item.keep = true WHERE item.score >= 90 SELECT item.id",
		resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("explicit PREWHERE error = %v", err)
	}
	combined, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('items') AS item WHERE item.keep = true AND item.score >= 90 SELECT item.id",
		resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("combined predicate error = %v", err)
	}
	if !reflect.DeepEqual(prewhere.Columns, combined.Columns) || !reflect.DeepEqual(prewhere.Rows, combined.Rows) {
		t.Fatalf("explicit PREWHERE = %#v/%#v, combined = %#v/%#v", prewhere.Columns, prewhere.Rows, combined.Columns, combined.Rows)
	}
	want := []SQLRow{{"id": int64(3)}, {"id": int64(5)}}
	if !reflect.DeepEqual(prewhere.Rows, want) {
		t.Fatalf("explicit PREWHERE rows = %#v, want %#v", prewhere.Rows, want)
	}
}

func TestSQLExplicitPrewherePreservesNullFiltering(t *testing.T) {
	resolver := SQLSourceResolverFunc(func(string, string) ([]SQLRow, error) {
		return []SQLRow{
			{"id": int64(1), "keep": nil, "score": int64(100)},
			{"id": int64(2), "keep": true, "score": nil},
			{"id": int64(3), "keep": true, "score": int64(100)},
		}, nil
	})
	result, err := ExecuteSQLQueryContext(context.Background(),
		"FROM CACHE('items') AS item PREWHERE item.keep = true WHERE item.score >= 100 SELECT item.id",
		resolver, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("explicit PREWHERE NULL error = %v", err)
	}
	want := []SQLRow{{"id": int64(3)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("explicit PREWHERE NULL rows = %#v, want %#v", result.Rows, want)
	}
}

func TestSQLExplicitPrewhereStreamsAndBindsParameters(t *testing.T) {
	resolver := &prewhereStreamResolver{rows: []SQLRow{
		{"id": int64(1), "keep": true, "score": int64(91)},
		{"id": int64(2), "keep": false, "score": int64(99)},
		{"id": int64(3), "keep": true, "score": int64(89)},
	}}
	result, err := ExecuteSQLQueryParameters(context.Background(),
		"FROM CACHE('items') AS item PREWHERE item.keep = $1 WHERE item.score >= $2 SELECT item.id",
		resolver, []interface{}{true, int64(90)}, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("parameterized explicit PREWHERE error = %v", err)
	}
	want := []SQLRow{{"id": int64(1)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("parameterized explicit PREWHERE rows = %#v, want %#v", result.Rows, want)
	}
	var streamed []SQLRow
	err = ExecuteSQLQueryRows(context.Background(),
		"FROM CACHE('items') AS item PREWHERE item.keep = $1 WHERE item.score >= $2 SELECT item.id",
		resolver, []interface{}{true, int64(90)}, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
			streamed = append(streamed, row)
			return nil
		})
	if err != nil {
		t.Fatalf("streamed parameterized explicit PREWHERE error = %v", err)
	}
	if !reflect.DeepEqual(streamed, want) {
		t.Fatalf("streamed parameterized explicit PREWHERE rows = %#v, want %#v", streamed, want)
	}
}
